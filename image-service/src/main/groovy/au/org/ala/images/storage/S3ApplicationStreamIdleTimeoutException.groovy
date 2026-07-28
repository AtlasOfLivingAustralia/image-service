package au.org.ala.images.storage

import groovy.transform.CompileStatic

/**
 * Raised when an application-owned S3 stream makes no forward progress within
 * the configured idle interval.
 */
@CompileStatic
class S3ApplicationStreamIdleTimeoutException extends IOException {

    S3ApplicationStreamIdleTimeoutException(String message) {
        super(message)
    }

    S3ApplicationStreamIdleTimeoutException(String message, Throwable cause) {
        super(message, cause)
    }
}
