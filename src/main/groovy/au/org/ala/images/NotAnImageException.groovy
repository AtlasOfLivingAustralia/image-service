package au.org.ala.images

import groovy.transform.CompileStatic

/**
 * Exception thrown when attempting to generate thumbnails or tiles for non-image content
 * (e.g., audio files, documents, videos).
 * 
 * This allows fast-fail behavior to avoid expensive processing operations on inappropriate content.
 */
@CompileStatic
class NotAnImageException extends RuntimeException {
    
    final String imageIdentifier
    final String actualMimeType
    final String operation
    
    NotAnImageException(String imageIdentifier, String actualMimeType, String operation) {
        super("Cannot ${operation} for ${imageIdentifier}: content type is ${actualMimeType}, not an image")
        this.imageIdentifier = imageIdentifier
        this.actualMimeType = actualMimeType
        this.operation = operation
    }
    
    NotAnImageException(String imageIdentifier, String actualMimeType) {
        this(imageIdentifier, actualMimeType, "generate derivatives")
    }
    
    @Override
    synchronized Throwable fillInStackTrace() {
        // Don't fill in stack trace for performance (this is a fast-fail exception)
        return this
    }
}

