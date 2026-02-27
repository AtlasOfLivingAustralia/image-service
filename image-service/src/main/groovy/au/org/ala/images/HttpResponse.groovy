package au.org.ala.images

/**
 * Abstraction over different HTTP client implementations (URLConnection and HttpClient)
 * to provide a consistent interface for getting response headers and body.
 */
interface HttpResponse extends AutoCloseable {

    /**
     * Get the request URI
     */
    URI getUri()

    /**
     * Get the HTTP status code
     */
    int getStatusCode() throws IOException
    
    /**
     * Get a response header value
     */
    String getHeader(String name)
    
    /**
     * Get the Content-Type header
     */
    String getContentType()
    
    /**
     * Get the Content-Length header
     */
    long getContentLength()
    
    /**
     * Get the response body as an InputStream
     */
    InputStream getInputStream() throws IOException
    
    /**
     * Get the error stream for failed responses (4xx, 5xx)
     */
    InputStream getErrorStream()
    
    /**
     * Close any resources associated with this response
     */
    void close()
}

