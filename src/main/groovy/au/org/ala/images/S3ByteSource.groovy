package au.org.ala.images

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.google.common.io.ByteSource

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
        def client
        if (url.userInfo) {
            def parts = url.userInfo.split(':')
            client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(parts[0], parts[1]))).build()
        } else {
            client = AmazonS3ClientBuilder.standard().build()
        }

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
        return client.getObject(bucketname, key).getObjectContent()
    }
}
