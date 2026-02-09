package au.org.ala.images.config

import groovy.transform.CompileStatic

@CompileStatic
class Filter {
    long minBytes = 0
    Map<String, Long> minBytesByFormat = [:]
    long minPixels = 0
    long minWidth = 0
    long minHeight = 0
    List<String> contentTypes = []
    Boolean animated // null means not checked
    Boolean alpha    // null means not checked

    static Filter minBytes(long bytes) {
        Filter filter = new Filter()
        filter.minBytes = bytes
        return filter
    }

    static Filter minPixels(long pixels) {
        Filter filter = new Filter()
        filter.minPixels = pixels
        return filter
    }

    Filter contentTypes(List<String> types) {
        this.contentTypes = types
        return this
    }

    Filter minWidth(long width) {
        this.minWidth = width
        return this
    }

    Filter minHeight(long height) {
        this.minHeight = height
        return this
    }

    Filter animated(boolean value) {
        this.animated = value
        return this
    }

    Filter alpha(boolean value) {
        this.alpha = value
        return this
    }

    Filter minBytesByFormat(Map<String, Long> map) {
        this.minBytesByFormat = map
        return this
    }
}
