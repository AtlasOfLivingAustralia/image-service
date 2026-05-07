package au.org.ala.images

import groovy.transform.CompileStatic

/**
 * HttpResponse implementation that wraps a URLConnection
 */
@CompileStatic
class UrlConnectionHttpResponse implements HttpResponse {
    
    private final URLConnection connection
    
    UrlConnectionHttpResponse(URLConnection connection) {
        this.connection = connection
    }

    @Override
    URI getUri() {
        return connection.getURL().toURI()
    }
    
    @Override
    int getStatusCode() throws IOException {
        if (connection instanceof HttpURLConnection) {
            return ((HttpURLConnection) connection).responseCode
        }
        // For non-HTTP connections (like s3://), assume success
        return 200
    }
    
    @Override
    String getHeader(String name) {
        return connection.getHeaderField(name)
    }
    
    @Override
    String getContentType() {
        return connection.contentType
    }
    
    @Override
    long getContentLength() {
        return connection.contentLengthLong
    }
    
    @Override
    InputStream getInputStream() throws IOException {
        return connection.inputStream
    }
    
    @Override
    InputStream getErrorStream() {
        if (connection instanceof HttpURLConnection) {
            return ((HttpURLConnection) connection).errorStream
        }
        return null
    }
    
    @Override
    void close() {
        if (connection instanceof HttpURLConnection) {
            ((HttpURLConnection) connection).disconnect()
        }
    }
}

