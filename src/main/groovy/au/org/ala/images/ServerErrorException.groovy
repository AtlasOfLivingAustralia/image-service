package au.org.ala.images

import groovy.transform.InheritConstructors

/**
 * Exception for server errors (5xx HTTP status codes)
 */
@InheritConstructors
class ServerErrorException extends HttpImageUploadException {
    ServerErrorException(String message, String url, int statusCode) {
        super(message, url, statusCode)
    }
}

