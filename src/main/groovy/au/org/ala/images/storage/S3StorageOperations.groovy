package au.org.ala.images.storage

import au.org.ala.images.AuditService
import au.org.ala.images.DefaultStoragePathStrategy
import au.org.ala.images.ImageInfo
import au.org.ala.images.Range
import au.org.ala.images.S3ByteSinkFactory
import au.org.ala.images.StoragePathStrategy
import au.org.ala.images.util.ByteSinkFactory
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.github.benmanes.caffeine.cache.RemovalCause
import grails.util.Holders
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import org.slf4j.event.Level
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.core.async.BlockingInputStreamAsyncRequestBody
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.metrics.LoggingMetricPublisher
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.DefaultRetryStrategy
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.crt.S3CrtConnectionHealthConfiguration
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration
import software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest as V2GetObjectRequest
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import software.amazon.awssdk.services.s3.model.CopyObjectRequest as V2CopyObjectRequest
import software.amazon.awssdk.services.s3.S3Utilities
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.model.*

import java.time.Duration
import java.util.concurrent.CompletionException
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.concurrent.CountDownLatch

import com.google.common.annotations.VisibleForTesting
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j
import net.lingala.zip4j.io.inputstream.ZipInputStream
import org.apache.commons.io.FilenameUtils

@CompileStatic
@Slf4j
@EqualsAndHashCode(includes = ['region', 'bucket', 'prefix'])
class S3StorageOperations implements StorageOperations {

    String region
    String bucket
    String prefix
    String accessKey
    String secretKey
    boolean containerCredentials
    boolean publicRead
    // When true, explicitly attach the S3 canned Private ACL on upload metadata.
    // When false (and publicRead is also false), no ACL header will be attached, allowing
    // the bucket policy or default object ACLs to apply implicitly.
    boolean privateAcl
    boolean redirect
    String cloudfrontDomain

    boolean pathStyleAccess
    String hostname = ''

    private static final int maxConnections = Holders.getConfig().getProperty('aws.s3.max.connections', Integer, Integer.getInteger('au.org.ala.images.s3.max.connections', 500))
    private static final int maxErrorRetry = Holders.getConfig().getProperty('aws.s3.max.retry', Integer, Integer.getInteger('au.org.ala.images.s3.max.retry', 3))
    private static final int apiCallAttemptTimeout = Holders.getConfig().getProperty('aws.s3.timeouts.attempt', Integer, Integer.getInteger('au.org.ala.images.s3.timeouts.attempt', 60))
    private static final int apiCallTimeout = Holders.getConfig().getProperty('aws.s3.timeouts.call', Integer, Integer.getInteger('au.org.ala.images.s3.timeouts.call', 300))
    private static final int apacheConnectionTimeout = Holders.getConfig().getProperty('aws.s3.timeouts.connection', Integer, Integer.getInteger('au.org.ala.images.s3.timeouts.connection', 2))
    private static final int socketTimeout = Holders.getConfig().getProperty('aws.s3.timeouts.socket', Integer, Integer.getInteger('au.org.ala.images.s3.timeouts.socket', 50))
    private static final int apacheIdleTimeout = Holders.getConfig().getProperty('aws.s3.timeouts.idle', Integer, Integer.getInteger('au.org.ala.images.s3.timeouts.idle', 5))
    private static final int acquisitionTimeout = Holders.getConfig().getProperty('aws.s3.timeouts.acquisition', Integer, Integer.getInteger('au.org.ala.images.s3.timeouts.acquisition', 10))
    private static final int inflightTimeout = Holders.getConfig().getProperty('aws.s3.timeouts.eviction', Integer, Integer.getInteger('au.org.ala.images.s3.timeouts.eviction', apiCallTimeout + 10))
    private static final boolean tcpKeepAlive = Holders.getConfig().getProperty('aws.s3.sync.keepalive', Boolean, Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.sync.keepalive', 'true')))
    private static final boolean idleReaperEnabled = Holders.getConfig().getProperty('aws.s3.sync.idlereaper', Boolean, Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.sync.idlereaper', 'true')))
    private static final boolean useCrtAsyncClient = Holders.getConfig().getProperty('aws.s3.crt.enabled', Boolean, Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.crt.enabled', 'true')))
    private static final int crtThroughputSeconds = Holders.getConfig().getProperty('aws.s3.crt.throughput.seconds', Integer, Integer.getInteger('au.org.ala.images.s3.crt.throughput.seconds', 30))
    private static final int crtThroughputBps = Holders.getConfig().getProperty('aws.s3.crt.throughput.bps', Integer, Integer.getInteger('au.org.ala.images.s3.crt.throughput.bps', 2))
    private static final int crtConnectionTimeout = Holders.getConfig().getProperty('aws.s3.crt.connection.timeout', Integer, Integer.getInteger('au.org.ala.images.s3.crt.connection.timeout', 2))
    private static final boolean publishCloudwatchMetrics = Holders.getConfig().getProperty('aws.s3.cloudwatch.metrics.enabled', Boolean, Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.cloudwatch.metrics.enabled', 'false')))
    private static final String cloudWatchNamespace = Holders.getConfig().getProperty('aws.s3.cloudwatch.metrics.namespace', String, System.getProperty('au.org.ala.images.s3.cloudwatch.metrics.namespace', 'au.org.ala.image-service/S3'))
    private static final boolean forceAsyncCalls = Holders.getConfig().getProperty('aws.s3.force.async', Boolean, Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.force.async', 'false')))

    private static ScheduledExecutorService evictionScheduler = Executors.newSingleThreadScheduledExecutor({ Runnable r ->
        new Thread(r, "s3-eviction-scheduler").tap {
            daemon = true
        }
    } as ThreadFactory)

    private static final MetricPublisher sharedMetricPublisher
    static {
            if (publishCloudwatchMetrics) {
                log.info("Using CloudWatchMetricPublisher for S3 SDK metrics, namespace: ${cloudWatchNamespace}")
                sharedMetricPublisher = CloudWatchMetricPublisher.builder().namespace(cloudWatchNamespace).build()
            } else {
                log.info("Using LoggingMetricPublisher for S3 SDK metrics")
                sharedMetricPublisher = LoggingMetricPublisher.create(Level.INFO, LoggingMetricPublisher.Format.PLAIN)
            }

            // Register a shutdown hook to close the publisher and flush metrics when the JVM exits
            Runtime.getRuntime().addShutdownHook(new Thread({
                try {
                    sharedMetricPublisher.close()
                } catch (Exception e) {
                    System.err.println("Failed to close sharedMetricPublisher: " + e.message)
                }
            }, "s3-metrics-publisher-shutdown-hook"))
    }

    // TODO configure cache parameters via config
    private static final LoadingCache<CacheKey, S3Client> s3ClientCache = Caffeine<String, S3Client>.from("maximumSize=10,refreshAfterWrite=1h").removalListener {
        CacheKey key, S3Client client, RemovalCause cause ->
            log.info("S3Client evicted from cache: ${key.bucket}")
            evictionScheduler.schedule( {
                try {
                    client?.close()
                } catch (Exception e) {
                    log.warn("Failed to close evicted S3Client", e)
                }
            }, inflightTimeout, TimeUnit.SECONDS)
    }.build { CacheKey key ->
        return s3ClientCacheLoader(key)
    }

    private static final LoadingCache<CacheKey, S3AsyncClient> s3AsyncClientCache = Caffeine<String, S3AsyncClient>.from("maximumSize=10,refreshAfterWrite=1h").removalListener {
        CacheKey key, S3AsyncClient client, RemovalCause cause ->
            log.info("S3AsyncClient evicted from cache: ${key.bucket}")
            s3TransferManagerCache.invalidate(key) // also remove any associated transfer manager
            evictionScheduler.schedule( {
                try {
                    client?.close()
                } catch (Exception e) {
                    log.warn("Failed to close evicted S3AsyncClient", e)
                }
            }, inflightTimeout, TimeUnit.SECONDS)
    }.build { CacheKey key ->
        return s3AsyncClientCacheLoader(key)
    }

    private static final LoadingCache<CacheKey, S3TransferManager> s3TransferManagerCache = Caffeine<String, S3TransferManager>.from("maximumSize=10,refreshAfterWrite=1h").removalListener {
        CacheKey key, S3TransferManager manager, RemovalCause cause ->
            log.info("S3TransferManager evicted from cache: ${key.bucket}")
            evictionScheduler.schedule( {
                try {
                    manager?.close()
                } catch (Exception e) {
                    log.warn("Failed to close evicted S3TransferManager", e)
                }
            }, inflightTimeout, TimeUnit.SECONDS)
    }.build { CacheKey key ->
        return s3TransferManagerCacheLoader(key)
    }

    static final void clearS3ClientCache() {
        s3TransferManagerCache.invalidateAll()
        s3AsyncClientCache.invalidateAll()
        s3ClientCache.invalidateAll()
    }

    @TupleConstructor
    @EqualsAndHashCode
    @ToString
    private static final class CacheKey {
        String region
        boolean containerCredentials
        String accessKey
        String secretKey
        String bucket
        String hostname
        boolean pathStyleAccess
    }

    private CacheKey getCacheKeyObject() {
        return new CacheKey(region, containerCredentials, accessKey, secretKey, bucket, hostname, pathStyleAccess)
    }

    @VisibleForTesting
    protected S3Client getS3Client() {
        final cacheKey = getCacheKeyObject()
        return s3ClientCache.get(cacheKey)
    }

    private static S3Client s3ClientCacheLoader(CacheKey key) {
        def containerCredentials = key.containerCredentials
        def accessKey = key.accessKey
        def secretKey = key.secretKey
        def region = key.region
        def bucket = key.bucket
        def hostname = key.hostname
        def pathStyleAccess = key.pathStyleAccess

        log.info("Creating S3Client for bucket: ${bucket} in region: ${region ?: 'default'}")

        def credProvider = containerCredentials ? DefaultCredentialsProvider.create() : StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

        // Configure HTTP Client with max connections
        def httpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(maxConnections)
                .connectionTimeout(Duration.ofSeconds(apacheConnectionTimeout))
                .socketTimeout(Duration.ofSeconds(socketTimeout))
                .tcpKeepAlive(tcpKeepAlive)
                .connectionMaxIdleTime(Duration.ofSeconds(apacheIdleTimeout))
                .connectionAcquisitionTimeout(Duration.ofSeconds(acquisitionTimeout))
                .useIdleConnectionReaper(idleReaperEnabled)

        // Configure Retry Policy
        def overrideConfig = ClientOverrideConfiguration.builder()
                .retryStrategy(DefaultRetryStrategy.standardStrategyBuilder().maxAttempts(maxErrorRetry).build())
                .addMetricPublisher(sharedMetricPublisher)
                .apiCallAttemptTimeout(Duration.ofSeconds(apiCallAttemptTimeout))
                .apiCallTimeout(Duration.ofSeconds(apiCallTimeout))
                .build()

        def builder = S3Client.builder()
                .defaultsMode(DefaultsMode.AUTO)
                .credentialsProvider(credProvider)
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig)

        if (region) {
            builder = builder.region(Region.of(region))
        }
        if (hostname) {
            builder = builder.endpointOverride(URI.create(hostname))
        }
        if (pathStyleAccess) {
            builder = builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        }
        return builder.build()
    }

    @VisibleForTesting
    protected S3AsyncClient getS3AsyncClient() {
        final cacheKey = getCacheKeyObject()
        return s3AsyncClientCache.get(cacheKey)
    }

    private static S3AsyncClient s3AsyncClientCacheLoader(CacheKey key) {
        def containerCredentials = key.containerCredentials
        def accessKey = key.accessKey
        def secretKey = key.secretKey
        def region = key.region
        def bucket = key.bucket
        def hostname = key.hostname
        def pathStyleAccess = key.pathStyleAccess

        log.info("Creating S3AsyncClient for bucket: ${bucket} in region: ${region ?: 'default'}")

        def credProvider = containerCredentials ? DefaultCredentialsProvider.create() : StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

        def client

        if (!useCrtAsyncClient) {
            log.info("Using standard S3AsyncClient builder for S3 access")

            // Configure Retry Policy
            def overrideConfig = ClientOverrideConfiguration.builder()
                    .retryStrategy(DefaultRetryStrategy.standardStrategyBuilder().maxAttempts(maxErrorRetry).build())
                    .addMetricPublisher(sharedMetricPublisher)
                    .apiCallAttemptTimeout(Duration.ofSeconds(apiCallAttemptTimeout))
                    .apiCallTimeout(Duration.ofSeconds(apiCallTimeout))
                    .build()

            def builder = S3AsyncClient.builder()
                    .credentialsProvider(credProvider)
                    .overrideConfiguration(overrideConfig)
                    .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                            .connectionTimeout(Duration.ofSeconds(apacheConnectionTimeout))
                            .connectionAcquisitionTimeout(Duration.ofSeconds(acquisitionTimeout))
                            .readTimeout(Duration.ofSeconds(socketTimeout))
                            .writeTimeout(Duration.ofSeconds(socketTimeout))
                            .maxConcurrency(maxConnections)
                            .tcpKeepAlive(tcpKeepAlive)
                            .useIdleConnectionReaper(idleReaperEnabled)
                    )

            if (region) {
                builder = builder.region(Region.of(region))
            }
            if (hostname) {
                builder = builder.endpointOverride(URI.create(hostname))
            }
            if (pathStyleAccess) {
                builder = builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            }
            client = builder.build()
        } else {
            log.info("Using CRT S3AsyncClient builder for S3 access")

            def builder = S3AsyncClient.crtBuilder()
                    .httpConfiguration(
                            S3CrtHttpConfiguration.builder()
                                    .connectionTimeout(Duration.ofSeconds(crtConnectionTimeout))
                                    .connectionHealthConfiguration(S3CrtConnectionHealthConfiguration.builder()
                                            .minimumThroughputTimeout(Duration.ofSeconds(crtThroughputSeconds))
                                            .minimumThroughputInBps(crtThroughputBps)
                                            .build()
                                    )
                                    .build()
                    )
                    .credentialsProvider(credProvider)
                    .retryConfiguration(S3CrtRetryConfiguration.builder().numRetries(maxErrorRetry).build())
//                        .maxConcurrency(maxConnections)

            if (region) {
                builder = builder.region(Region.of(region))
            }
            if (hostname) {
                builder = builder.endpointOverride(URI.create(hostname))
            }
            if (pathStyleAccess) {
                builder = builder.forcePathStyle(pathStyleAccess)
            }
            client = builder.build()
        }
        return client
    }

    @VisibleForTesting
    protected S3TransferManager getS3TransferManager() {
        final cacheKey = getCacheKeyObject()
        // Ensure the underlying AsyncClient is kept alive in the cache whenever the TransferManager is requested
//        getS3AsyncClient(cacheKey)
        return s3TransferManagerCache.get(cacheKey)
    }

    private static S3TransferManager s3TransferManagerCacheLoader(CacheKey key) {
        log.info("Creating S3TransferManager for bucket: ${key.bucket} in region: ${key.region ?: 'default'}")

        def s3AsyncClient = s3AsyncClientCache.get(key)
        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build()
    }

    @VisibleForTesting
    protected S3Presigner getS3Presigner() {
            def credProvider = containerCredentials ? DefaultCredentialsProvider.create() : StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
            def builder = S3Presigner.builder().credentialsProvider(credProvider)
            if (region) {
                builder = builder.region(Region.of(region))
            }
            if (hostname) {
                builder = builder.endpointOverride(URI.create(hostname))
            }
            return builder.build()
    }

    // --- Helper methods to route via async client when forceAsyncCalls is enabled ---

    private S3Utilities getUtilities() {
        // Use utilities from the sync client; utilities do not execute network calls
        return s3Client.utilities()
    }

    private void putObjectAclAsyncAware(String bkt, String key, ObjectCannedACL acl) {
        if (forceAsyncCalls) {
            s3AsyncClient.putObjectAcl({ PutObjectAclRequest.Builder b -> b.bucket(bkt).key(key).acl(acl) } as Consumer<PutObjectAclRequest.Builder>).join()
        } else {
            s3Client.putObjectAcl({ PutObjectAclRequest.Builder b -> b.bucket(bkt).key(key).acl(acl) } as Consumer<PutObjectAclRequest.Builder>)
        }
    }

    private HeadObjectResponse headObjectAsyncAware(String bkt, String key) {
        if (forceAsyncCalls) {
            return s3AsyncClient.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(key) } as Consumer<HeadObjectRequest.Builder>).join()
        }
        return s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(key) } as Consumer<HeadObjectRequest.Builder>)
    }

    private void copyObjectAsyncAware(Consumer<V2CopyObjectRequest.Builder> consumer) {
        if (forceAsyncCalls) {
            s3AsyncClient.copyObject(consumer).join()
        } else {
            s3Client.copyObject(consumer)
        }
    }

    private void headBucketAsyncAware(String bkt) {
        if (forceAsyncCalls) {
            s3AsyncClient.headBucket({ HeadBucketRequest.Builder b -> b.bucket(bkt) } as Consumer<HeadBucketRequest.Builder>).join()
        } else {
            s3Client.headBucket({ HeadBucketRequest.Builder b -> b.bucket(bkt) } as Consumer<HeadBucketRequest.Builder>)
        }
    }

    private void deleteObjectAsyncAware(String bkt, String key) {
        if (forceAsyncCalls) {
            s3AsyncClient.deleteObject({ DeleteObjectRequest.Builder b -> b.bucket(bkt).key(key) } as Consumer<DeleteObjectRequest.Builder>).join()
        } else {
            s3Client.deleteObject({ DeleteObjectRequest.Builder b -> b.bucket(bkt).key(key) } as Consumer<DeleteObjectRequest.Builder>)
        }
    }

    private void putObjectSmallAsyncAware(String bkt, String key, String contentType, byte[] bytes) {
        def consumer = { PutObjectRequest.Builder b -> b.bucket(bkt).key(key).contentType(contentType) } as Consumer<PutObjectRequest.Builder>
        if (forceAsyncCalls) {
            s3AsyncClient.putObject(consumer, AsyncRequestBody.fromBytes(bytes)).join()
        } else {
            s3Client.putObject(consumer, RequestBody.fromBytes(bytes))
        }
    }

    private void putObjectStreamAsyncAware(String bkt, String key, String contentType, InputStream stream, long length) {
        def consumer = { PutObjectRequest.Builder b -> b.bucket(bkt).key(key).contentType(contentType) } as Consumer<PutObjectRequest.Builder>
        if (forceAsyncCalls) {
            // Use TransferManager with a blocking async request body to stream data
            def transferManager = getS3TransferManager()
            def blockingBody = BlockingOutputStreamAsyncRequestBody.builder().build()
            def uploadReq = UploadRequest.builder()
                    .putObjectRequest(PutObjectRequest.builder().bucket(bkt).key(key).contentType(contentType).build())
                    .requestBody(blockingBody)
                    .build()
            def future = transferManager.upload(uploadReq).completionFuture()
            blockingBody.outputStream().withStream { os ->
                stream.transferTo(os)
            }
            future.join()
        } else {
            s3Client.putObject(consumer, RequestBody.fromInputStream(stream, length))
        }
    }

    private byte[] getObjectBytesAsyncAware(String bkt, String key, Range range) {
        def consumer = { V2GetObjectRequest.Builder b ->
            b.bucket(bkt).key(key)
            if (range != null && !range.empty) {
                b.range("bytes=${range.start()}-${range.end()}")
            }
        } as Consumer<V2GetObjectRequest.Builder>
        if (forceAsyncCalls) {
            def resp = s3AsyncClient.getObject(consumer, AsyncResponseTransformer.toBytes()).join()
            return resp.asByteArray()
        } else {
            def s3Object = s3Client.getObject(consumer)
            return s3Object.withStream { it.bytes }
        }
    }

    private static class ListResult {
        List<S3Object> contents = []
        List<String> commonPrefixes = []
    }

    private ListResult listObjectsV2AsyncAware(String bkt, String prefix, String delimiter) {
        if (!forceAsyncCalls) {
            ListObjectsV2Iterable pages = s3Client.listObjectsV2Paginator({ ListObjectsV2Request.Builder b -> b.bucket(bkt).prefix(prefix).delimiter(delimiter) } as Consumer<ListObjectsV2Request.Builder>)
            ListResult lr = new ListResult()
            pages.each { page ->
                lr.contents.addAll(page.contents())
                lr.commonPrefixes.addAll(page.commonPrefixes().collect { it.prefix() })
            }
            return lr
        }
        ListResult result = new ListResult()
        CountDownLatch latch = new CountDownLatch(1)
        def publisher = s3AsyncClient.listObjectsV2Paginator({ ListObjectsV2Request.Builder b -> b.bucket(bkt).prefix(prefix).delimiter(delimiter) } as Consumer<ListObjectsV2Request.Builder>)
        publisher.subscribe(new org.reactivestreams.Subscriber<ListObjectsV2Response>() {
            org.reactivestreams.Subscription s
            @Override void onSubscribe(org.reactivestreams.Subscription s) { this.s = s; s.request(Long.MAX_VALUE) }
            @Override void onNext(ListObjectsV2Response page) {
                result.contents.addAll(page.contents())
                result.commonPrefixes.addAll(page.commonPrefixes().collect { it.prefix() })
            }
            @Override void onError(Throwable t) { latch.countDown(); }
            @Override void onComplete() { latch.countDown(); }
        })
        latch.await()
        return result
    }

    @Override
    boolean isSupportsRedirect() {
        redirect
    }

    @Override
    URI redirectLocation(String path) {
        if (cloudfrontDomain) {
            // If a CloudFront domain is set, use that for redirects
            new URI("https://${cloudfrontDomain}/${path}")
        } else if (publicRead) {
            S3Utilities utils = getUtilities()
            def url = utils.getUrl(GetUrlRequest.builder().bucket(bucket).key(path).build())
            url.toURI()
        } else if (privateAcl) {
            // Presign with AWS SDK v2 for 1 hour
            def presignReq = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofHours(1))
                    .getObjectRequest { b -> b.bucket(bucket).key(path) }
                    .build()
            def presigned = s3Presigner.withCloseable {
                it.presignGetObject(presignReq)
            }
            presigned.url().toURI()
        } else {
            // TODO read bucket policy to determine appropriate redirect URL?
            S3Utilities utils = getUtilities()
            def url = utils.getUrl(GetUrlRequest.builder().bucket(bucket).key(path).build())
            url.toURI()
        }
    }

    def updateACL() {
        walkPrefix(storagePathStrategy().basePath()) { Map objSummary ->
            final String path = (objSummary['key'] as String)
            try {
                // Update ACL if requested
                if (publicRead) {
                    final String bkt = this.bucket
                    final String pth = path
                    putObjectAclAsyncAware(bkt, pth, ObjectCannedACL.PUBLIC_READ)
                } else if (privateAcl) {
                    final String bkt = this.bucket
                    final String pth = path
                    putObjectAclAsyncAware(bkt, pth, ObjectCannedACL.PRIVATE)
                }

                // Update Cache-Control by copying object to itself with REPLACE
                def head = headObjectAsyncAware(bucket, path)
                def cacheControl = null
                if (publicRead) cacheControl = 'public,s-maxage=31536000,max-age=31536000'
                else if (privateAcl) cacheControl = 'private,max-age=31536000'

                final String bkt2 = this.bucket
                final String pth2 = path
                copyObjectAsyncAware({ V2CopyObjectRequest.Builder b ->
                    b.copySource("${bkt2}/${pth2}")
                     .destinationBucket(bkt2)
                     .destinationKey(pth2)
                     .metadataDirective(MetadataDirective.REPLACE)
                     .contentType(head.contentType())
                     .cacheControl(cacheControl)
                } as Consumer<V2CopyObjectRequest.Builder>)
            } catch (CompletionException e) {
                def cause = e.cause
                if (cause instanceof S3Exception) {
                    log.error('Error updating ACL for {}, public: {}, privateAcl: {}, error: {}', path, publicRead, privateAcl, e.message)
                } else {
                    throw e.cause
                }
            } catch (S3Exception e) {
                log.error('Error updating ACL for {}, public: {}, privateAcl: {}, error: {}', path, publicRead, privateAcl, e.message)
            }
        }
    }

    // Removed v1 ObjectMetadata helper; headers are set directly via v2 PutObjectRequest

    @Override
    boolean verifySettings() {
        try {
            // Check bucket existence via headBucket
            final String bkt = this.bucket
            headBucketAsyncAware(bkt)
            // Put a tiny object then delete
            final String key = storagePathStrategy().basePath() + '/' + UUID.randomUUID().toString()
            final String ct = 'application/octet-stream'
            putObjectSmallAsyncAware(bkt, key, ct, new byte[1])
            deleteObjectAsyncAware(bkt, key)
            return true
        } catch (CompletionException e) {
            def cause = e.cause
            if (cause instanceof S3Exception) {
                log.error("Exception while verifying S3 bucket {}", this, e)
                return false
            }
            throw e.cause
        } catch (S3Exception e) {
            log.error("Exception while verifying S3 bucket {}", this, e)
            return false
        }
    }

    @Override
    void store(String uuid, InputStream stream, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        String path = createOriginalPathFromUUID(uuid)
        storeInternal(stream, path, contentType, contentDisposition, length, [imageType: 'original'])
    }

    @Override
    byte[] retrieve(String uuid) {
        if (uuid) {
            def imagePath = createOriginalPathFromUUID(uuid)
            def bytes
            try {
                final String bkt = this.bucket
                final String imgKey = imagePath
                bytes = getObjectBytesAsyncAware(bkt, imgKey, null)
            } catch (CompletionException e) {
                def cause = e.cause
                if (cause instanceof NoSuchKeyException) {
                    throw new FileNotFoundException("S3 path $imagePath")
                } else if (cause instanceof S3Exception && (e.cause as S3Exception).statusCode() == 404) {
                    throw new FileNotFoundException("S3 path $imagePath")
                }
                throw e.cause
            } catch (NoSuchKeyException e) {
                throw new FileNotFoundException("S3 path $imagePath")
            } catch (S3Exception e) {
                if (e.statusCode() == 404) {
                    throw new FileNotFoundException("S3 path $imagePath")
                } else {
                    throw e
                }
            }
            return bytes
        }
        return null
    }

    @Override
    InputStream inputStream(String path, Range range) throws FileNotFoundException {
        try {
            def consumer = { V2GetObjectRequest.Builder b ->
                b.bucket(bucket).key(path)
                if (range != null && !range.empty) {
                    b.range("bytes=${range.start()}-${range.end()}")
                }
            } as Consumer<V2GetObjectRequest.Builder>
            if (forceAsyncCalls) {
                return s3AsyncClient
                        .getObject(consumer, AsyncResponseTransformer.toBlockingInputStream())
                        .join()
            } else {
                return s3Client.getObject(consumer)
            }
        } catch (CompletionException e) {
            def cause = e.cause
            if (cause instanceof NoSuchKeyException) {
                throw new FileNotFoundException("S3 path $path")
            } else if (cause instanceof S3Exception && (e.cause as S3Exception).statusCode() == 404) {
                throw new FileNotFoundException("S3 path $path")
            }
            throw e.cause
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("S3 path $path")
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw new FileNotFoundException("S3 path $path")
            } else {
                throw e
            }
        }
    }

    // v1 AbortingS3ObjectInputStream removed; v2 getObject returns a standard InputStream that can be closed directly.

    @Override
    boolean stored(String uuid) {
        def key = createOriginalPathFromUUID(uuid)
        try {
            final String bkt = this.bucket
            final String kk = key
            headObjectAsyncAware(bkt, kk)
            return true
        } catch (CompletionException e) {
            def cause = e.cause
            if (cause instanceof NoSuchKeyException) {
                return false
            } else if (cause instanceof S3Exception && (e.cause as S3Exception).statusCode() == 404) {
                return false
            }
            throw e.cause
        } catch (NoSuchKeyException e) {
            return false
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false
            throw e
        }
    }

    @Override
    boolean thumbnailExists(String uuid, String type) {
        def key = createThumbLargePathFromUUID(uuid, type)
        try {
            final String bkt = this.bucket
            final String kk = key
            headObjectAsyncAware(bkt, kk)
            return true
        } catch (CompletionException e) {
            def cause = e.cause
            if (cause instanceof NoSuchKeyException) {
                return false
            } else if (cause instanceof S3Exception && (e.cause as S3Exception).statusCode() == 404) {
                return false
            }
            throw e.cause
        } catch (NoSuchKeyException e) {
            return false
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false
            throw e
        }
    }

    @Override
    boolean tileExists(String uuid, int x, int y, int z) {
        def key = createTilesPathFromUUID(uuid, x, y, z)
        try {
            final String bkt = this.bucket
            final String kk = key
            headObjectAsyncAware(bkt, kk)
            return true
        } catch (CompletionException e) {
            def cause = e.cause
            if (cause instanceof NoSuchKeyException) {
                return false
            } else if (cause instanceof S3Exception && (e.cause as S3Exception).statusCode() == 404) {
                return false
            }
            throw e.cause
        } catch (NoSuchKeyException e) {
            return false
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false
            throw e
        }
    }

    @Override
    void storeTileZipInputStream(String uuid, String zipInputFileName, String contentType, long length = 0, ZipInputStream zipInputStream) {
        def path = FilenameUtils.normalize(createTilesPathFromUUID(uuid) + '/' + zipInputFileName)
        zipInputStream.withStream { stream ->
            final String bkt = this.bucket
            final String pth = path
            final String ct = contentType
            putObjectStreamAsyncAware(bkt, pth, ct, stream, length)
        }
    }

    long consumedSpace(String uuid) {
        return getConsumedSpaceInternal(storagePathStrategy().createPathFromUUID(uuid, ''))
    }

    @Override
    boolean deleteStored(String uuid) {
        final String bkt = this.bucket
        walkPrefix(storagePathStrategy().createPathFromUUID(uuid, '')) { Map obj ->
            final String key = obj['key'] as String
            deleteObjectAsyncAware(bkt, key)
        }
        AuditService.submitLog(uuid, "Image deleted from store", "N/A")
        return true
    }

    private long getConsumedSpaceInternal(String prefix) {
        long size = 0
        List<String> extraPrefixes = []
        final String bkt = this.bucket
        def lr = listObjectsV2AsyncAware(bkt, prefix, '/')
        size += (Long) (lr.contents.collect { it.size() }.sum() ?: 0L)
        extraPrefixes.addAll(lr.commonPrefixes)
        return size + (Long)((extraPrefixes.sum { getConsumedSpaceInternal(it) }) ?: 0L)
    }

    private <T> List<T> walkPrefix(String prefix, Closure<T> f) {
        List<T> results = []
        List<String> extraPrefixes = []
        final String bkt = this.bucket
        def lr = listObjectsV2AsyncAware(bkt, prefix, '/')
        lr.contents.each { obj ->
            Map summary = [key: obj.key(), size: obj.size()]
            results += f.call(summary)
        }
        extraPrefixes.addAll(lr.commonPrefixes)
        results.addAll((List<T>) extraPrefixes.collectMany { String extraPrefix -> walkPrefix(extraPrefix, f) })
        return results
    }

    StoragePathStrategy storagePathStrategy() {
        new DefaultStoragePathStrategy(prefix ?: '', true, false)
    }

    @Override
    ByteSinkFactory thumbnailByteSinkFactory(String uuid) {
        byteSinkFactory(uuid, [imageType: 'thumbnail'])
    }

    @Override
    ByteSinkFactory tilerByteSinkFactory(String uuid) {
        byteSinkFactory(uuid, [imageType: 'tile'], 'tms')
    }

    ByteSinkFactory byteSinkFactory(String uuid, Map<String, String> tags, String... prefixes) {
        return new S3ByteSinkFactory(s3AsyncClient, s3TransferManager, apiCallTimeout, storagePathStrategy(), bucket, uuid, tags, prefixes)
    }

    @Override
    void storeAnywhere(String uuid, InputStream stream, String relativePath, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        def path = storagePathStrategy().createPathFromUUID(uuid, relativePath)
        storeInternal(stream, path, contentType, contentDisposition, length, [:])
    }

    private storeInternal(InputStream stream, String absolutePath, String contentType, String contentDisposition, Long length, Map<String, String> tags) {
        try {
            stream.withStream { InputStream input ->
                def transferManager = getS3TransferManager()

                PutObjectRequest.Builder b = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(absolutePath)

                if (contentType) b.contentType(contentType)
                if (contentDisposition) b.contentDisposition(contentDisposition)

                // Cache-Control headers based on ACL flags (preserved behavior)
                if (publicRead) {
                    b.cacheControl('public,s-maxage=31536000,max-age=31536000')
                    b.acl(ObjectCannedACL.PUBLIC_READ)
                } else if (privateAcl) {
                    b.cacheControl('private,max-age=31536000')
                    b.acl(ObjectCannedACL.PRIVATE)
                }
                if (tags) {
                    def tagSet = tags.collect { k, v ->
                        Tag.builder().key(k).value(v).build()
                    }
                    if (tagSet) b.tagging(Tagging.builder().tagSet(tagSet).build())
                }

                if (length != null && length < 0) {
                    log.info("storeInternal: length for $absolutePath was negative, resetting to null")
                    length = null
                }
                AsyncRequestBody blockingBody = BlockingInputStreamAsyncRequestBody.builder()
                        .contentType(contentType)
                        .contentLength(length) // TODO remove this because it is optional and throws if wrong?
                        .build()

                def uploadReq = UploadRequest.builder()
                        .putObjectRequest(b.build())
                        .requestBody(blockingBody)
                        .build()

                def uploadFuture = transferManager.upload(uploadReq).completionFuture()

                blockingBody.writeInputStream(input)

                def result = uploadFuture.join()
                log.debug("Uploaded {} to S3 {}:{} with result etag {}", absolutePath, region, bucket, result.response().eTag())
            }
        } catch (Exception exception) {
            log.warn 'An exception was caught while storing input stream', exception
            throw new RuntimeException(exception)
        }
    }

    @Override
    void migrateTo(String uuid, String contentType, StorageOperations destination) {
        def basePath = createBasePathFromUUID(uuid)
        final String bkt = this.bucket
        walkPrefix(basePath) { Map obj ->
            final String k = obj['key'] as String
            def head = headObjectAsyncAware(bkt, k)
            InputStream objectStream
            if (forceAsyncCalls) {
                def consumer = { V2GetObjectRequest.Builder b -> b.bucket(bkt).key(k) } as Consumer<V2GetObjectRequest.Builder>
                objectStream = s3AsyncClient.getObject(consumer, AsyncResponseTransformer.toBlockingInputStream()).join()
            } else {
                objectStream = s3Client.getObject({ V2GetObjectRequest.Builder b -> b.bucket(bkt).key(k) } as Consumer<V2GetObjectRequest.Builder>)
            }
            destination.storeAnywhere(uuid, objectStream, k - basePath, head.contentType(), head.contentDisposition(), (obj['size'] as long))
        }
    }

    @Override
    long storedLength(String path) throws FileNotFoundException {
        try {
            final String bkt = this.bucket
            final String pth = path
            def head = headObjectAsyncAware(bkt, pth)
            return head.contentLength()
        } catch (CompletionException e) {
            def cause = e.cause
            if (cause instanceof NoSuchKeyException) {
                throw new FileNotFoundException("S3 path $path")
            } else if (cause instanceof S3Exception && (e.cause as S3Exception).statusCode() == 404) {
                throw new FileNotFoundException("S3 path $path")
            }
            throw e.cause
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("S3 path $path")
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw new FileNotFoundException("S3 path $path")
            } else {
                throw e
            }
        }
    }

    @Override
    ImageInfo originalImageInfo(String uuid) {
        def path = createOriginalPathFromUUID(uuid)
        return imageInfoInternal(path).tap { it.imageIdentifier = uuid }
    }

    @Override
    ImageInfo thumbnailImageInfo(String uuid, String type) {
        def path = createThumbLargePathFromUUID(uuid, type)
        return imageInfoInternal(path).tap { it.imageIdentifier = uuid }
    }

    @Override
    ImageInfo tileImageInfo(String uuid, int x, int y, int z) {
        def path = createTilesPathFromUUID(uuid, x, y, z)
        return imageInfoInternal(path).tap { it.imageIdentifier = uuid }
    }

    private ImageInfo imageInfoInternal(String path) {
        try {
            final String bkt = this.bucket
            final String pth = path
            def head = headObjectAsyncAware(bkt, pth)
            def contentLength = head.contentLength()
            Date lastMod = head.lastModified() != null ? Date.from(head.lastModified()) : null
            return new ImageInfo(
                    exists: true,
                    length: contentLength,
                    etag: head.eTag(),
                    lastModified: lastMod,
                    contentType: head.contentType(),
                    extension: FilenameUtils.getExtension(path),
                    redirectUri: supportsRedirect ? redirectLocation(path) : null,
                    inputStreamSupplier: { Range range -> inputStream(path, range ?: Range.emptyRange(contentLength)) }
            )
        } catch (CompletionException e) {
            def cause = e.cause
            if (cause instanceof NoSuchKeyException) {
                return new ImageInfo(exists: false)
            } else if (cause instanceof S3Exception && (e.cause as S3Exception).statusCode() == 404) {
                return new ImageInfo(exists: false)
            }
            throw e.cause
        } catch (NoSuchKeyException e) {
            return new ImageInfo(exists: false)
        }catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return new ImageInfo(exists: false)
            } else {
                throw e
            }
        }
    }

    void clearTilesForImage(String uuid) {
        def basePath = createTilesPathFromUUID(uuid)
        final String bkt = this.bucket
        walkPrefix(basePath) { Map summary ->
            final String key = summary['key'] as String
            deleteObjectAsyncAware(bkt, key)
        }
    }

    @Override
    String toString() {
        "S3StorageOperations: $region:$bucket:${prefix ?: ''}"
    }
}
