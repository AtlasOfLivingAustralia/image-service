package au.org.ala.images

import groovy.transform.CompileStatic

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse as JavaHttpResponse

/**
 * HttpResponse implementation that wraps a java.net.http.HttpResponse
 */
@CompileStatic
class HttpClientHttpResponse implements HttpResponse {
    
    private final JavaHttpResponse<InputStream> response
    
    HttpClientHttpResponse(JavaHttpResponse<InputStream> response) {
        this.response = response
    }

    @Override
    URI getUri() {
        return response.uri()
    }
    
    @Override
    int getStatusCode() throws IOException {
        return response.statusCode()
    }
    
    @Override
    String getHeader(String name) {
        return response.headers().firstValue(name).orElse(null)
    }
    
    @Override
    String getContentType() {
        return getHeader("Content-Type")
    }
    
    @Override
    long getContentLength() {
        def lengthHeader = getHeader("Content-Length")
        if (lengthHeader) {
            try {
                return Long.parseLong(lengthHeader)
            } catch (NumberFormatException e) {
                return -1
            }
        }
        return -1
    }
    
    @Override
    InputStream getInputStream() throws IOException {
        return response.body()
    }
    
    @Override
    InputStream getErrorStream() {
        // For HttpClient, error responses are in the same body stream
        return response.body()
    }
    
    @Override
    void close() {
        // HttpClient responses don't need explicit closing
        // The InputStream will be closed by the caller
    }
}

