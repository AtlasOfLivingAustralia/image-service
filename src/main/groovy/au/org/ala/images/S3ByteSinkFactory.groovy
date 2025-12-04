package au.org.ala.images

import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import groovy.util.logging.Slf4j
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging

import java.nio.file.Files
import java.time.Duration

@Slf4j
class S3ByteSinkFactory implements ByteSinkFactory {

    static boolean streaming = Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.upload.streaming', 'true'))

    private final S3AsyncClient s3Client
    private final S3TransferManager s3TransferManager
    private final int connectionTimeout
    private final StoragePathStrategy storagePathStrategy
    private final String uuid
    private final String[] prefixes
    private final String bucket
    private final Map<String, String> tags


    S3ByteSinkFactory(S3AsyncClient s3Client, S3TransferManager s3TransferManager, int connectionTimeout, StoragePathStrategy storagePathStrategy, String bucket, String uuid, Map<String, String> tags, String... prefixes) {
        this.bucket = bucket
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

                String contentType = guessContentType(names.length > 0 ? names.last() : '')

                if (streaming && s3TransferManager) {
                    def reqBuilder = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(path)
                            .contentType(contentType)
                    if (tags) {
                        def tagSet = tags.collect { Tag.builder().key(it.key).value(it.value).build() }
                        reqBuilder = reqBuilder.tagging(Tagging.builder().tagSet(tagSet).build())
                    }

                    // Use S3TransferManager which supports streaming via BlockingOutputStreamAsyncRequestBody
                    def blockingBody = BlockingOutputStreamAsyncRequestBody.builder()
                            .subscribeTimeout(Duration.ofSeconds(connectionTimeout)) // this should wait for any other s3 calls to timeout
                            .build()

                    def uploadReq = UploadRequest.builder()
                            .putObjectRequest(reqBuilder.build())
                            .requestBody(blockingBody)
                            .build()
                    def uploadFuture = s3TransferManager.upload(uploadReq).completionFuture()

                    return new FilterOutputStream(blockingBody.outputStream()) {
                        @Override
                        void close() throws IOException {
                            super.close()
                            // wait for the upload thread to finish
                            try {
                                uploadFuture.join()
                            } catch (Exception e) {
                                throw new IOException("S3 upload failed", e)
                            }
                        }
                    }
                } else {
                    // Buffer to a temp file and upload on close
                    def tempPath = Files.createTempFile("thumbnail-$uuid-${names.join('-')}", ".jpg")
                    def file = tempPath.toFile()
                    file.deleteOnExit()
                    return new FilterOutputStream(new BufferedOutputStream(Files.newOutputStream(tempPath))) {
                        @Override
                        void close() throws IOException {
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
}
