package au.org.ala.images

import groovy.transform.InheritConstructors

/**
 * Base exception for HTTP-related image upload failures
 */
@InheritConstructors
class HttpImageUploadException extends RuntimeException {
    int statusCode
    String url
    
    HttpImageUploadException(String message, String url, int statusCode) {
        super(message)
        this.url = url
        this.statusCode = statusCode
    }
    
    HttpImageUploadException(String message, String url, int statusCode, Throwable cause) {
        super(message, cause)
        this.url = url
        this.statusCode = statusCode
    }
}

