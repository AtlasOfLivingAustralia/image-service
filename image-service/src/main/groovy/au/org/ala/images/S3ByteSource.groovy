package au.org.ala.images

import com.google.common.io.ByteSource
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

/**
 * Accepts a URL in the form s3://bucketname/key and returns a ByteSource for the object in the bucket with the given key.
 *
 * Authentication is supported by providing the access key and secret key in the userInfo part of the URL in the form accessKey:secretKey
 * or by the default AWS credentials provider chain if no userinfo is provided.
 *
 * This is not intended to be used with image service managed objects but for ingesting images from S3.
 */
class S3ByteSource extends ByteSource {

    private URI url

    S3ByteSource(URI url) {
        if (url.scheme != 's3') {
            throw new IllegalArgumentException("URL scheme must be 's3' for S3ByteSource")
        }
        if (!url.path) {
            throw new IllegalArgumentException("URL path is required for S3ByteSource")
        }
        if (!url.host) {
            throw new IllegalArgumentException("URL host is required for S3ByteSource")
        }
        if (url.userInfo && !url.userInfo.contains(':')) {
            throw new IllegalArgumentException("URL userInfo must be in the form 'accessKey:secretKey'")
        }
        this.url = url
    }

    @Override
    InputStream openStream() throws IOException {
        def builder = S3Client.builder()
        if (url.userInfo) {
            def parts = url.userInfo.split(':')
            builder = builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(parts[0], parts[1])))
        }
        // Region is optional in the URL; S3 client will use default provider chain if not set
        def client = builder.build()

        def bucketname = url.host
        def key = url.path

//        if (url.host.matches('s3\\.(.*\\.)?amazonaws\\.com')) {
//            bucketname = url.path.substring(1, url.path.indexOf('/', 1))
//            key = url.path.substring(url.path.indexOf('/', 1) + 1)
//        } else {
//            bucketname = url.host.substring(0, url.host.indexOf('.'))
//            key = url.path.substring(1)
//        }
//        def path = url.path
//        if (path.startsWith('/')) {
//            path = path.substring(1)
//        }
        if (key.startsWith('/')) {
            key = key.substring(1)
        }
        def resp = client.getObject(GetObjectRequest.builder().bucket(bucketname).key(key).build())
        return resp
    }
}
