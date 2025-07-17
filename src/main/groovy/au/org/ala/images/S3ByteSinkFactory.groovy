package au.org.ala.images

import au.org.ala.images.util.ByteSinkFactory
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectTagging
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.Tag
import com.google.common.io.ByteSink

import java.nio.file.Files

class S3ByteSinkFactory implements ByteSinkFactory {

    private final AmazonS3 s3Client
    private final StoragePathStrategy storagePathStrategy
    private final String uuid
    private final String[] prefixes
    private final String bucket
    private final Map<String, String> tags

    S3ByteSinkFactory(AmazonS3 s3Client, StoragePathStrategy storagePathStrategy, String bucket, String uuid, Map<String, String> tags, String... prefixes) {
        this(s3Client, storagePathStrategy, bucket, uuid, prefixes)
        this.tags = tags
    }

    S3ByteSinkFactory(AmazonS3 s3Client, StoragePathStrategy storagePathStrategy, String bucket, String uuid, String... prefixes) {
        this.bucket = bucket
        this.s3Client = s3Client
        this.storagePathStrategy = storagePathStrategy
        this.uuid = uuid
        this.prefixes = prefixes
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
                // Amazon S3 client doesn't support streaming in v1, buffer to temp file and then
                // send when the outputstream is closed.
                // TODO convert to v2 when streaming support lands
                def tempPath = Files.createTempFile("thumbnail-$uuid-${names.join('-')}", ".jpg")
                def file = tempPath.toFile()
                file.deleteOnExit()
                return new FilterOutputStream(new BufferedOutputStream(Files.newOutputStream(tempPath))) {
                    @Override
                    void close() throws IOException {
                        super.close()
                        // once the file output is closed we can send it to S3 and then delete the temp file
                        def putObjectRequest = new PutObjectRequest(bucket, path, file)
                        if (tags) {
                            def tagSet = tags.collect { new Tag(it.key, it.value) }
                            putObjectRequest.setTagging(new ObjectTagging(tagSet))
                        }
                        s3Client.putObject(putObjectRequest)
                        Files.deleteIfExists(tempPath)
                    }
                }
            }
        }
    }
}
