package au.org.ala.images.storage

import groovy.transform.CompileStatic

/**
 * Raised when an S3 upload does not complete within the configured bound after its body has
 * been fully supplied. The upload has been cancelled when this is thrown.
 */
@CompileStatic
class S3UploadCompletionTimeoutException extends IOException {

    S3UploadCompletionTimeoutException(String message, Throwable cause) {
        super(message, cause)
    }
}
