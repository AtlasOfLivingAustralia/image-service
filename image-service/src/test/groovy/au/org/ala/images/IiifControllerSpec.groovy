package au.org.ala.images

import grails.testing.web.controllers.ControllerUnitTest
import spock.lang.Specification

class IiifControllerSpec extends Specification implements ControllerUnitTest<IiifController> {

    @Override
    Closure doWithSpring() {{ ->
        redirectCacheHeadersService(RedirectCacheHeadersService)
    }}

    def "render redirect gets redirect cache headers, not the image's"() {
        given:
        controller.iiifImageService = Mock(IiifImageService)
        params.identifier = 'abc'
        params.region = 'full'
        params.size = 'max'
        params.rotation = '0'
        params.quality = 'default'
        params.format = 'jpg'
        Date redirectExpires = presigned ? new Date(System.currentTimeMillis() + 3600_000L) : null

        when:
        controller.renderImage()

        then:
        1 * controller.iiifImageService.render('abc', 'full', 'max', '0', 'default', 'jpg', false) >> new IiifImageService.IiifRenderResult(
                imageInfo: new ImageInfo(exists: true, redirectUri: new URI('https://example.org/x.jpg'), redirectExpires: redirectExpires),
                mime: 'image/jpeg', etag: 'etag', lastModified: new Date())
        response.status == 302
        response.getHeader('Location') == 'https://example.org/x.jpg'
        response.getHeader('Cache-Control') == cacheControl
        response.getHeader('ETag') == null
        response.getHeader('Last-Modified') == null

        where:
        presigned || cacheControl
        false     || 'public, max-age=3600'
        true      || 'private, max-age=300'
    }
}
