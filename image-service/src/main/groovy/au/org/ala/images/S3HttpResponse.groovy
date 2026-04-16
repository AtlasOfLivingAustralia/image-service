package au.org.ala.images

import groovy.util.logging.Slf4j
import org.apache.commons.io.IOUtils
import org.apache.logging.log4j.util.Strings
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3ClientBuilder
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception

import java.util.regex.Pattern

/**
 * HttpResponse implementation that wraps an AWS S3 SDK response.
 * Supports s3:// URLs with optional credentials in the userInfo part.
 */
@Slf4j
class S3HttpResponse implements HttpResponse {

    private static final Pattern S3_BUCKET_PATTERN = Pattern.compile("(.*)\\.s3\\.(.*\\.)?amazonaws\\.com")

    private final URI uri
    private final String bucketName
    private final String region
    private final String key
    private final S3ClientBuilder builder
    private S3Client _client // not final to allow lazy initialization

    // For testing with localstack or custom endpoints
    private String endpoint
    private boolean pathStyleAccess = false

    private ResponseInputStream<GetObjectResponse> responseStream
    private GetObjectResponse objectResponse
    private HeadObjectResponse headResponse

    S3HttpResponse(URI uri) {
        if (uri.scheme != 's3') {
            throw new IllegalArgumentException("URL scheme must be 's3' for S3HttpResponse")
        }
        if (!uri.path) {
            throw new IllegalArgumentException("URL path is required for S3HttpResponse")
        }
        if (!uri.host) {
            throw new IllegalArgumentException("URL host is required for S3HttpResponse")
        }
        if (uri.userInfo && !uri.userInfo.contains(':')) {
            throw new IllegalArgumentException("URL userInfo must be in the form 'accessKey:secretKey'")
        }

        this.uri = uri
        var host = uri.host
        if (host.matches(S3_BUCKET_PATTERN)) {
            var matcher = S3_BUCKET_PATTERN.matcher(host)
            this.bucketName = Strings.EMPTY
            this.region = Strings.EMPTY
            if (matcher.find()) {
                var groupCount = matcher.groupCount()
                this.bucketName = matcher.group(1)
                this.region = matcher.group(2)
            } else {
                throw new IllegalArgumentException("Could not parse bucket name and region from host: ${host}")
            }
        } else {
            this.bucketName = host
        }

        def path = uri.path
        if (path.startsWith('/')) {
            path = path.substring(1)
        }
        this.key = path

        // Build S3 client with optional credentials
        def builder = S3Client.builder()
        if (uri.userInfo) {
            def parts = uri.userInfo.split(':')
            builder = builder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(parts[0], parts[1])
                )
            )
        }
        if (this.region && !this.region.isEmpty()) {
            builder = builder.region(Region.of(this.region))
        }
        this.builder = builder
    }

    void setEndpoint(String endpoint) {
        this.endpoint = endpoint
    }

    void setPathStyleAccessEnabled(boolean enabled) {
        this.pathStyleAccess = enabled
    }

    S3Client getClient() {
        if (_client == null) {
            def builder = this.builder
            if (endpoint) {
                builder = builder.endpointOverride(URI.create(endpoint))
            }
            if (pathStyleAccess) {
                builder = builder.forcePathStyle(true)
            }
            _client = builder.build()
        }
        return _client
    }

    @Override
    URI getUri() {
        return uri
    }

    @Override
    int getStatusCode() throws IOException {
        // For S3, we'll use HEAD to check if object exists
        // If it exists, return 200, otherwise throw exception
        try {
            if (headResponse == null) {
                def request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build()
                headResponse = client.headObject(request)
            }
            return 200
        } catch (NoSuchKeyException ignored) {
            return 404
        } catch (S3Exception e) {
            // Map S3 status codes to HTTP status codes
            return e.statusCode()
        }
    }

    @Override
    String getHeader(String name) {
        try {
            ensureHeadResponse()

            // Map common HTTP headers to S3 response headers
            switch (name?.toLowerCase()) {
                case 'content-type':
                    return headResponse ? headResponse.contentType() : objectResponse.contentType()
                case 'content-length':
                    return headResponse ? headResponse.contentLength()?.toString() : objectResponse.contentLength()?.toString()
                case 'etag':
                    return headResponse ? headResponse.eTag() : objectResponse.eTag()
                case 'last-modified':
                    return headResponse ? headResponse.lastModified()?.toString() : objectResponse.lastModified()?.toString()
                default:
                    // Check metadata for custom headers
                    return headResponse ? headResponse.metadata()?.get(name) : objectResponse.metadata()?.get(name)
            }
        } catch (Exception e) {
            log.debug("Error getting header ${name} from S3 object: ${e.message}")
            return null
        }
    }

    @Override
    String getContentType() {
        try {
            ensureHeadResponse()
            return headResponse ? headResponse.contentType() : objectResponse.contentType()
        } catch (Exception e) {
            log.debug("Error getting content type from S3 object: ${e.message}")
            return null
        }
    }

    @Override
    long getContentLength() {
        try {
            ensureHeadResponse()
            return (headResponse ? headResponse.contentLength() : objectResponse.contentLength()) ?: -1
        } catch (Exception e) {
            log.debug("Error getting content length from S3 object: ${e.message}")
            return -1
        }
    }

    @Override
    InputStream getInputStream() throws IOException {
        try {
            if (responseStream == null) {
                def request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build()
                responseStream = client.getObject(request)
                objectResponse = responseStream.response()
            }
            return responseStream
        } catch (NoSuchKeyException e) {
            throw new IOException("S3 object not found: s3://${bucketName}/${key}", e)
        } catch (S3Exception e) {
            throw new IOException("S3 error accessing s3://${bucketName}/${key}: ${e.message}", e)
        }
    }

    @Override
    InputStream getErrorStream() {
        // S3 doesn't have separate error streams
        return null
    }

    @Override
    void close() {
        try {
            if (responseStream != null) {
                try {
                    IOUtils.consume(responseStream)
                } finally {
                    responseStream.close()
                }
            }
        } catch (Exception e) {
            log.debug("Error closing S3 response stream: ${e.message}")
        }

        try {
            if (client != null) {
                client.close()
            }
        } catch (Exception e) {
            log.debug("Error closing S3 client: ${e.message}")
        }
    }

    /**
     * Ensure we have a HEAD response for getting metadata
     */
    private void ensureHeadResponse() {
        if (headResponse == null && objectResponse == null) {
            def request = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()
            headResponse = client.headObject(request)
        }
    }
}

