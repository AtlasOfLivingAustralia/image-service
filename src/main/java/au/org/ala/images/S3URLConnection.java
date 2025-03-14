package au.org.ala.images;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

public class S3URLConnection extends URLConnection {

    private S3Object object;

    /**
     * Constructs a URL connection to the specified URL. A connection to
     * the object referenced by the URL is not created.
     *
     * @param url the specified URL.
     */
    protected S3URLConnection(URL url) {
        super(url);
    }

    @Override
    public void connect() throws IOException {
        AmazonS3 client;
        URI uri;

        try {
            uri = url.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        if (Strings.isNotBlank(uri.getUserInfo())) {
            var parts = uri.getUserInfo().split(":");
            client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(parts[0], parts[1]))).build();
        } else {
            client = AmazonS3ClientBuilder.standard().build();
        }

        var bucketname = uri.getHost();
        var key = uri.getPath();

        object = client.getObject(bucketname, key);

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
//        return client.getObject(bucketname, key).getObjectContent()
    }

    @Override
    public String getContentEncoding() {
        return object.getObjectMetadata().getContentEncoding();
    }

    @Override
    public long getContentLengthLong() {
        return object.getObjectMetadata().getContentLength();
    }

    @Override
    public String getContentType() {
        return object.getObjectMetadata().getContentType();
    }

    @Override
    public long getExpiration() {
        return object.getObjectMetadata().getExpirationTime().getTime();
    }

    @Override
    public long getLastModified() {
        return object.getObjectMetadata().getLastModified().getTime();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return object.getObjectContent();
    }

    @Override
    public String getHeaderField(String name) {
        return object.getObjectMetadata().getRawMetadataValue(name).toString();
    }
}
