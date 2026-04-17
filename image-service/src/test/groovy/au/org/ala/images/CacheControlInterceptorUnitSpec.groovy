package au.org.ala.images

import spock.lang.Specification

class CacheControlInterceptorUnitSpec extends Specification {

    void "should treat html, json and xml content types as non-cacheable"() {
        expect:
        CacheControlInterceptor.isNoCacheContentType(contentType)

        where:
        contentType << [
                'text/html',
                'application/xhtml+xml',
                'application/json',
                'application/problem+json',
                'application/xml',
                'text/xml',
                'application/atom+xml'
        ]
    }

    void "should not treat binary image content as non-cacheable content type"() {
        expect:
        !CacheControlInterceptor.isNoCacheContentType('image/png')
    }

    void "should detect gzipped csv attachment"() {
        expect:
        CacheControlInterceptor.isGzippedCsv('application/gzip', 'attachment;filename=images-export.csv.gz')
        !CacheControlInterceptor.isGzippedCsv('application/gzip', 'attachment;filename=images-export.avro.gz')
        !CacheControlInterceptor.isGzippedCsv('application/octet-stream', 'attachment;filename=images-export.csv.gz')
    }

    void "should detect no-cache opt-out annotation on controller and action"() {
        expect:
        CacheControlInterceptor.isNoCacheOptOut(ControllerOptOut, 'index')
        CacheControlInterceptor.isNoCacheOptOut(ActionOptOut, 'index')
        !CacheControlInterceptor.isNoCacheOptOut(ActionOptOut, 'other')
        !CacheControlInterceptor.isNoCacheOptOut(NoOptOut, 'index')
    }

    @NoCacheOptOut
    private static class ControllerOptOut {
        def index() {}
    }

    private static class ActionOptOut {
        @NoCacheOptOut
        def index() {}

        def other() {}
    }

    private static class NoOptOut {
        def index() {}
    }
}
