package au.org.ala.images

import au.org.ala.images.metrics.MetricsTrackingOutputStream
import au.org.ala.images.storage.S3ApplicationStreamIdleTimeoutException
import au.org.ala.images.storage.S3ApplicationWatchdogOutputStream
import au.org.ala.images.storage.S3UploadCompletionTimeoutException
import au.org.ala.images.storage.S3UploadHealth
import au.org.ala.images.storage.S3UploadProgress
import au.org.ala.images.util.AbortableFilterOutputStream
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.MeterRegistry
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging

import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import grails.util.Holders

@Slf4j
class S3ByteSinkFactory implements ByteSinkFactory {

    static boolean streaming = Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.upload.streaming', 'true'))
    private static final Duration applicationStreamIdleTimeout = applicationStreamIdleTimeout()
    private static final Duration derivativeCompletionTimeout = derivativeCompletionTimeout()

    private final S3AsyncClient s3Client
    private final S3TransferManager s3TransferManager
    private final int connectionTimeout
    private final StoragePathStrategy storagePathStrategy
    private final String uuid
    private final String[] prefixes
    private final String bucket
    private final String region
    private final MeterRegistry meterRegistry
    private final Map<String, String> tags


    S3ByteSinkFactory(S3AsyncClient s3Client, S3TransferManager s3TransferManager, int connectionTimeout, StoragePathStrategy storagePathStrategy,
                      String bucket, String uuid, Map<String, String> tags, MeterRegistry meterRegistry, String region,
                      String... prefixes) {
        this.bucket = bucket
        this.region = region
        this.meterRegistry = meterRegistry
        this.s3Client = s3Client
        this.storagePathStrategy = storagePathStrategy
        this.uuid = uuid
        this.prefixes = prefixes
        this.tags = tags
        this.s3TransferManager = s3TransferManager
        this.connectionTimeout = connectionTimeout
    }

    @Override
    void prepare() throws IOException {

    }

    @Override
    ByteSink getByteSinkForNames(String... names) {
        def path = storagePathStrategy.createPathFromUUID(uuid, *(prefixes + names))
        return new ByteSink() {
            @Override
            OutputStream openStream() throws IOException {

                OutputStream baseStream
                String metricsOperationType

                String contentType = guessContentType(names.length > 0 ? names.last() : '')

                if (streaming && s3TransferManager) {
                    metricsOperationType = "streaming"
                    def reqBuilder = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(path)
                            .contentType(contentType)
                    if (tags) {
                        def tagSet = tags.collect { Tag.builder().key(it.key).value(it.value).build() }
                        reqBuilder = reqBuilder.tagging(Tagging.builder().tagSet(tagSet).build())
                    }

                    // Use S3TransferManager which supports streaming via BlockingOutputStreamAsyncRequestBody
                    def blockingBody = buildBlockingBody(connectionTimeout)

                    def uploadReq = UploadRequest.builder()
                            .putObjectRequest(reqBuilder.build())
                            .requestBody(blockingBody)
                            .build()
                    incrementUploadCounter('s3.upload.streaming.start')
                    def uploadFuture = s3TransferManager.upload(uploadReq).completionFuture()

                    def outputStream
                    try {
                        outputStream = blockingBody.outputStream()
                    } catch (IllegalStateException stuckState) {
                        // A stuck shared S3TransferManager / S3AsyncClient surfaces here when the request
                        // body is never subscribed within subscribeTimeout. Log a rich, single diagnostic
                        // line that lets us tell from logs whether the SAME manager/client is reused
                        // across failures. The exception is rethrown unchanged so behaviour is identical.
                        if (isStuckSubscription(stuckState)) {
                            recordStuckSubscription(path, uploadFuture)
                        }
                        // Otherwise the orphaned request stays queued in the client waiting for a body
                        // that will never arrive, holding capacity and keeping the client wedged.
                        uploadFuture.cancel(true)
                        throw stuckState
                    }
                    S3ApplicationWatchdogOutputStream watchdogStream = applicationStreamIdleTimeout == null ? null :
                            new S3ApplicationWatchdogOutputStream(outputStream, applicationStreamIdleTimeout, 'upload')
                    def progress = new S3UploadProgress('streaming', bucket, path)
                    baseStream = new AbortableFilterOutputStream(watchdogStream ?: outputStream) {
                        @Override
                        void write(int b) throws IOException {
                            try {
                                super.write(b)
                                progress.progressed(1)
                            } catch (IOException e) {
                                progress.reportFailure(e, health(uploadFuture))
                                throw e
                            }
                        }

                        @Override
                        void write(byte[] b, int off, int len) throws IOException {
                            try {
                                super.write(b, off, len)
                                progress.progressed(len)
                            } catch (IOException e) {
                                progress.reportFailure(e, health(uploadFuture))
                                throw e
                            }
                        }

                        @Override
                        protected void onAbort(Throwable cause) {
                            // If the SDK failed the upload first (e.g. a timeout), make sure that is logged.
                            if (uploadFuture.isCompletedExceptionally() && !uploadFuture.isCancelled()) {
                                progress.reportFailure(uploadFailure(uploadFuture), health(uploadFuture))
                            }
                            // Fail the request body so the partial object is never committed.
                            outputStream.cancel()
                            uploadFuture.cancel(true)
                            watchdogStream?.failed()
                            incrementUploadCounter('s3.upload.streaming.aborted')
                            log.debug("S3 streaming upload aborted by producer: bucket={} key={} cause={}", bucket, path, cause?.toString())
                        }

                        @Override
                        void close() throws IOException {
                            if (aborted) {
                                return
                            }
                            try {
                                super.close()
                                progress.producerFinished()
                                // Producer EOF is not terminal: retain the watchdog while the upload completes.
                                awaitCompletion(uploadFuture, path)
                                watchdogStream?.completed()
                                incrementUploadCounter('s3.upload.streaming.completion')
                            } catch (Exception e) {
                                incrementUploadCounter('s3.upload.streaming.failure')
                                progress.reportFailure(e, health(uploadFuture))
                                if (watchdogStream?.timedOut) {
                                    throw watchdogStream.timeoutException(e)
                                }
                                watchdogStream?.failed()
                                throw new IOException("S3 upload failed", e)
                            }
                        }
                    }
                } else {
                    // Buffer to a temp file and upload on close
                    metricsOperationType = "buffered"
                    def tempPath = Files.createTempFile("thumbnail-$uuid-${names.join('-')}", ".jpg")
                    def file = tempPath.toFile()
                    file.deleteOnExit()
                    
                    baseStream = new AbortableFilterOutputStream(new BufferedOutputStream(Files.newOutputStream(tempPath))) {
                        @Override
                        protected void onAbort(Throwable cause) {
                            try {
                                out.close()
                            } finally {
                                Files.deleteIfExists(tempPath)
                            }
                        }

                        @Override
                        void close() throws IOException {
                            if (aborted) {
                                return
                            }
                            super.close()
                            // once the file output is closed we can send it to S3 and then delete the temp file
                            def reqBuilder = PutObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(path)
                                    .contentType(contentType)
                            if (tags) {
                                def tagSet = tags.collect { Tag.builder().key(it.key).value(it.value).build() }
                                reqBuilder = reqBuilder.tagging(Tagging.builder().tagSet(tagSet).build())
                            }
                            try {
                                def response = s3Client.putObject(reqBuilder.build(), file.toPath()).join()
                            } catch (Exception e) {
                                throw new IOException("S3 upload failed", e)
                            } finally {
                                Files.deleteIfExists(tempPath)
                            }
                        }
                    }
                }
                // Wrap with metrics tracking if available
                if (meterRegistry != null) {
                    return new MetricsTrackingOutputStream(baseStream, meterRegistry, bucket, region, metricsOperationType)
                } else {
                    return baseStream
                }
            }

            private String guessContentType(String name) {
                // guess content type from names - default to image/jpeg
                String contentType
                String lastName = name.toLowerCase()
                if (lastName.endsWith('.png') || lastName.endsWith('_png')) {
                    contentType = "image/png"
                } else if (lastName.endsWith('.gif') || lastName.endsWith('_gif')) {
                    contentType = "image/gif"
                } else if (lastName.endsWith('.tiff') || lastName.endsWith('_tiff') || lastName.endsWith('.tif') || lastName.endsWith('_tif')) {
                    contentType = "image/tiff"
                } else if (lastName.endsWith('.webp') || lastName.endsWith('_webp')) {
                    contentType = "image/webp"
                } else if (lastName.endsWith('.jpeg') || lastName.endsWith('_jpeg') || lastName.endsWith('.jpg') || lastName.endsWith('_jpg')) {
                    contentType = "image/jpeg"
                } else if (lastName.endsWith('.bmp') || lastName.endsWith('_bmp')) {
                    contentType = "image/bmp"
                } else if (lastName.endsWith('.svg') || lastName.endsWith('_svg')) {
                    contentType = "image/svg+xml"
                } else if (lastName.endsWith('.pdf') || lastName.endsWith('_pdf')) {
                    contentType = "application/pdf"
                } else if (lastName.endsWith('.heic') || lastName.endsWith('_heic')) {
                    contentType = "image/heic"
                } else if (lastName.equals('thumbnail_square')) {
                    contentType = "image/png"
                } else if (lastName.equals('thumbnail') || lastName.startsWith('thumbnail_')) {
                    contentType = "image/jpeg"
                } else {
                    contentType = "application/octet-stream"
                }
                log.debug("Guessed content type [$contentType] for name [$name]")
                return contentType
            }
        }
    }
    /**
     * Builds the streaming request body. Extracted as a protected seam so tests can substitute a
     * body whose outputStream() never gets subscribed; production behaviour is identical.
     */
    protected BlockingOutputStreamAsyncRequestBody buildBlockingBody(int timeoutSeconds) {
        return BlockingOutputStreamAsyncRequestBody.builder()
                .subscribeTimeout(Duration.ofSeconds(timeoutSeconds)) // this should wait for any other s3 calls to timeout
                .build()
    }

    /**
     * Upper bound on waiting for an upload to finish after the body has been fully written.
     * Settable so tests can shorten it.
     */
    Duration completionTimeout = derivativeCompletionTimeout

    /**
     * Waits for the upload with a bound; on timeout the upload is cancelled so the request does not
     * keep holding client capacity, and the caller gets an exception instead of blocking forever.
     */
    private void awaitCompletion(CompletableFuture<?> uploadFuture, String path) {
        Duration timeout = completionTimeout
        try {
            uploadFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (TimeoutException timedOut) {
            uploadFuture.cancel(true)
            incrementUploadCounter('s3.upload.completion.timeout')
            log.debug("S3 streaming upload did not complete within {}s, cancelled: bucket={} key={} {}",
                    timeout.seconds, bucket, path, S3UploadHealth.describe(s3TransferManager, s3Client, uploadFuture))
            throw new S3UploadCompletionTimeoutException("S3 upload of ${path} did not complete within ${timeout.seconds}s", timedOut)
        } catch (ExecutionException failed) {
            throw new CompletionException(failed.cause ?: failed)
        } catch (InterruptedException interrupted) {
            uploadFuture.cancel(true)
            Thread.currentThread().interrupt()
            throw interrupted
        }
    }

    private String health(CompletableFuture<?> uploadFuture) {
        return S3UploadHealth.describe(s3TransferManager, s3Client, uploadFuture)
    }

    private static Throwable uploadFailure(CompletableFuture<?> uploadFuture) {
        try {
            uploadFuture.getNow(null)
            return null
        } catch (Throwable thrown) {
            return thrown
        }
    }

    /**
     * True when the IllegalStateException is the SDK's "request not made within N seconds"
     * stuck-subscription failure from BlockingOutputStreamAsyncRequestBody.
     * Package-private for testing.
     */
    static boolean isStuckSubscription(IllegalStateException exception) {
        String message = exception?.getMessage()
        return message != null && message.contains('was not made within')
    }

    /**
     * Logs a single rich WARN diagnostic for a stuck subscription and bumps the
     * {@code s3.upload.subscribe.timeout} counter. Never throws; all accessors are null-safe.
     */
    private void recordStuckSubscription(String path, def uploadFuture) {
        String health = S3UploadHealth.describe(s3TransferManager, s3Client, uploadFuture)
        log.warn("S3 streaming upload stuck: subscribeTimeout={}s bucket={} key={} {}",
                connectionTimeout, bucket, path, health)
        incrementUploadCounter('s3.upload.subscribe.timeout')
    }

    /**
     * Increments a streaming-upload counter tagged by bucket/region. No-op when no registry
     * is present so behaviour is unchanged in that configuration.
     */
    private void incrementUploadCounter(String metricName) {
        if (meterRegistry == null) {
            return
        }
        try {
            meterRegistry.counter(metricName,
                    'bucket', bucket ?: 'unknown',
                    'region', region ?: 'unknown')
                    .increment()
        } catch (Exception e) {
            log.warn("Failed to record S3 upload metric ${metricName}", e)
        }
    }

    private static Duration derivativeCompletionTimeout() {
        def defaultValue = Integer.getInteger('au.org.ala.images.s3.upload.derivative.completion.timeout', 60)
        def config = Holders.getConfig()
        int timeoutSeconds = config == null ? defaultValue : config.getProperty(
                'aws.s3.upload.derivative.completion.timeout.seconds', Integer, defaultValue)
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException('aws.s3.upload.derivative.completion.timeout.seconds must be a positive number of seconds')
        }
        return Duration.ofSeconds(timeoutSeconds)
    }

    private static Duration applicationStreamIdleTimeout() {
        def defaultValue = Integer.getInteger('au.org.ala.images.s3.application.stream.idle.timeout', 0)
        def config = Holders.getConfig()
        // Holders config is absent in plain unit tests; fall back to the system-property default.
        int timeoutSeconds = config == null ? defaultValue : config.getProperty(
                'aws.s3.application.stream.idle.timeout', Integer, defaultValue)
        if (timeoutSeconds < 0) {
            throw new IllegalArgumentException('aws.s3.application.stream.idle.timeout must be zero or a positive number of seconds')
        }
        return timeoutSeconds == 0 ? null : Duration.ofSeconds(timeoutSeconds)
    }
}
