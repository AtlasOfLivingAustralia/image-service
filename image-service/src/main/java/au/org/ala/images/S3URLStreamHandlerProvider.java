package au.org.ala.images;

import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;

/**
 * Support s3:// URLs
 */
public class S3URLStreamHandlerProvider extends URLStreamHandlerProvider {

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        if ("s3".equals(protocol)) {
            return new S3URLStreamHandler();
        }
        return null;
    }
}
