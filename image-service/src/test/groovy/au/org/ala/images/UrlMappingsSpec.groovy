package au.org.ala.images

import grails.testing.web.UrlMappingsUnitTest
import spock.lang.Specification

class UrlMappingsSpec extends Specification implements UrlMappingsUnitTest<UrlMappings> {

    void setup() {
        mockController(ImageController)
    }

    void "verify image urls match store urls (assuming no context path)"() {
        when:
        assertUrlMapping("/store/4/3/2/1/1234-1234-1234-1234/original", controller: 'image', action: 'getOriginalFile') {
            id = '1234-1234-1234-1234'
            a = 4
            b = 3
            c = 2
            d = 1
        }
        assertUrlMapping("/store/4/3/2/1/1234-1234-1234-1234/thumbnail", controller: 'image', action: 'proxyImageThumbnail') {
            id = '1234-1234-1234-1234'
            a = 4
            b = 3
            c = 2
            d = 1
        }
        assertUrlMapping("/store/4/3/2/1/1234-1234-1234-1234/thumbnail_large", controller: 'image', action: 'proxyImageThumbnailType') {
            id = '1234-1234-1234-1234'
            thumbnailType = 'large'
            a = 4
            b = 3
            c = 2
            d = 1
        }
        assertUrlMapping("/store/4/3/2/1/1234-1234-1234-1234/thumbnail_square", controller: 'image', action: 'proxyImageThumbnailType') {
            id = '1234-1234-1234-1234'
            thumbnailType = 'square'
            a = 4
            b = 3
            c = 2
            d = 1
        }
        assertUrlMapping("/store/4/3/2/1/1234-1234-1234-1234/thumbnail_square_black", controller: 'image', action: 'proxyImageThumbnailType') {
            id = '1234-1234-1234-1234'
            thumbnailType = 'square_black'
            a = 4
            b = 3
            c = 2
            d = 1
        }

        then:
        noExceptionThrown()
    }

    def "verify tile urls"() {
        when:
        assertUrlMapping("/store/4/3/2/1/1234-1234-1234-1234/tms/3/1/2.png", controller: 'image', action: 'proxyImageTile') {
            id = '1234-1234-1234-1234'
            x = 1
            y = 2
            z = 3
            a = 4
            b = 3
            c = 2
            d = 1
        }

        then:
        noExceptionThrown()
    }
}
