package au.org.ala.images

import groovy.transform.InheritConstructors

/**
 * Exception for client errors (4xx HTTP status codes)
 */
@InheritConstructors
class ClientErrorException extends HttpImageUploadException {
    ClientErrorException(String message, String url, int statusCode) {
        super(message, url, statusCode)
    }
}

