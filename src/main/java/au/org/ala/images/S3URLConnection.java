package au.org.ala.images;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Pattern;

/**
 * Support for non standard s3:// URLs, the URL should be in the form
 * s3://accesskey:secretKey@bucketname/path
 *
 * The accesskey and secretKey are optional, if not provided the default credentials will be used.
 */
public class S3URLConnection extends URLConnection {

    private static final Logger log = LoggerFactory.getLogger(S3URLConnection.class);

    private S3Client client;
    private ResponseInputStream<GetObjectResponse> object;

    private String endpoint;

    private boolean pathStyleAccessEnabled = false;

    /**
     * Constructs a URL connection to the specified URL. A connection to
     * the object referenced by the URL is not created.
     *
     * @param url the specified URL.
     */
    protected S3URLConnection(URL url) {
        super(url);

        if (!url.getProtocol().equals("s3")) {
            throw new IllegalArgumentException("URL must use s3 protocol");
        }

        if (Strings.isBlank(url.getHost())) {
            throw new IllegalArgumentException("URL must have a bucket name");
        }

        // TODO support path based.
        //https://bucket-name.s3.region-code.amazonaws.com/key-name
        var host = url.getHost();
        if (!host.matches("(.*)\\.s3\\.(.*\\.)?amazonaws\\.com")) {
            throw new IllegalArgumentException("URL host must be in the form bucketname.s3.region-code.amazonaws.com");
        }

        if (Strings.isBlank(url.getPath())) {
            throw new IllegalArgumentException("URL must have a key");
        }

        if (url.getUserInfo() != null && url.getUserInfo().split(":").length != 2) {
            throw new IllegalArgumentException("URL user info must be in the form accesskey:secretKey");
        }
    }

    @Override
    public void connect() throws IOException {
        URI uri;

        try {
            uri = url.toURI();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }

        var host = uri.getHost();
        var pattern = Pattern.compile("(.*)\\.s3\\.(.*\\.)?amazonaws\\.com");

        var matcher = pattern.matcher(host);
        String bucket = Strings.EMPTY;
        String region = Strings.EMPTY;
        if (matcher.find()) {
            var groupCount = matcher.groupCount();
            bucket = matcher.group(1);
            region = matcher.group(2);
        }
        if (!host.matches("(.*)\\.s3\\.(.*\\.)?amazonaws\\.com")) {
            throw new IllegalArgumentException("URL host must be in the form bucketname.s3.region-code.amazonaws.com");
        }

        var builder = S3Client.builder();
        if (Strings.isNotBlank(uri.getUserInfo())) {
            var parts = uri.getUserInfo().split(":");
            builder = builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(parts[0], parts[1])));
        }
        if (Strings.isNotBlank(region)) {
            builder = builder.region(Region.of(region));
            if (Strings.isNotBlank(endpoint)) {
                builder = builder.endpointOverride(URI.create(endpoint));
            }
        }
        if (pathStyleAccessEnabled) {
            builder = builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }
        this.client = builder.build();


        var key = uri.getPath();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        log.trace("Connecting to s3 bucket: {} key: {}", bucket, key);
        object = this.client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());

//        if (url.host.matches('s3\\.(.*\\.)?amazonaws\\.com')) {
//            bucketname = url.path.substring(1, url.path.indexOf('/', 1))
//            key = url.path.substring(url.path.indexOf('/', 1) + 1)
//        } else {
//            bucketname = url.host.substring(0, url.host.indexOf('.'))
//            key = url.path.substring(1)
//        }

    }

    public void disconnect() {
        if (object != null) {
            try {
                object.close();
            } catch (IOException e) { /* ignored */ }
            object = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private void ensureConnected() {
        if (object == null) {
            try {
                connect();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String getContentEncoding() {
        ensureConnected();
        return object.response().contentEncoding();
    }

    @Override
    public long getContentLengthLong() {
        ensureConnected();
        return object.response().contentLength();
    }

    @Override
    public String getContentType() {
        ensureConnected();
        return object.response().contentType();
    }

    @Override
    public long getExpiration() {
        ensureConnected();
        return object.response().expires() != null ? object.response().expires().toEpochMilli() : 0L;
    }

    @Override
    public long getLastModified() {
        ensureConnected();
        return object.response().lastModified() != null ? object.response().lastModified().toEpochMilli() : 0L;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        ensureConnected();
        return object;
    }

    @Override
    public String getHeaderField(String name) {
        ensureConnected();
        return object.response().sdkHttpResponse().firstMatchingHeader(name).orElse(null);
    }

    /**
     * For testing, allows overriding the default amazon s3 endpoint
     *
     * @return the endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * For testing, allows overriding the default amazon s3 endpoint
     *
     * @param endpoint the new endpoint to use, eg localstack.local
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public boolean isPathStyleAccessEnabled() {
        return pathStyleAccessEnabled;
    }

    public void setPathStyleAccessEnabled(boolean pathStyleAccessEnabled) {
        this.pathStyleAccessEnabled = pathStyleAccessEnabled;
    }
}
