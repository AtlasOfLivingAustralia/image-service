package au.org.ala.images

import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging

import java.nio.file.Files

class S3ByteSinkFactory implements ByteSinkFactory {

    static boolean streaming = Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.upload.streaming', 'true'))

    private final S3AsyncClient s3Client
    private final S3TransferManager s3TransferManager
    private final StoragePathStrategy storagePathStrategy
    private final String uuid
    private final String[] prefixes
    private final String bucket
    private final Map<String, String> tags


    S3ByteSinkFactory(S3AsyncClient s3Client, S3TransferManager s3TransferManager, StoragePathStrategy storagePathStrategy, String bucket, String uuid, Map<String, String> tags, String... prefixes) {
        this.bucket = bucket
        this.s3Client = s3Client
        this.storagePathStrategy = storagePathStrategy
        this.uuid = uuid
        this.prefixes = prefixes
        this.tags = tags
        this.s3TransferManager = s3TransferManager
    }
    S3ByteSinkFactory(S3AsyncClient s3Client, StoragePathStrategy storagePathStrategy, String bucket, String uuid, Map<String, String> tags, String... prefixes) {
        this(s3Client, null, storagePathStrategy, bucket, uuid, tags, prefixes)
    }

    S3ByteSinkFactory(S3AsyncClient s3Client, StoragePathStrategy storagePathStrategy, String bucket, String uuid, String... prefixes) {
        this(s3Client, null, storagePathStrategy, bucket, uuid, [:], prefixes)
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
                    def blockingBody = BlockingOutputStreamAsyncRequestBody.builder().build()

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
                } else {
                    contentType = "application/octet-stream"
                }
                log.debug("Guessed content type [$contentType] for name [$name]")
                return contentType
            }
        }
    }
}
