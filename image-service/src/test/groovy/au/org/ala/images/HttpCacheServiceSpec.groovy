package au.org.ala.images

import org.springframework.mock.web.MockHttpServletResponse
import spock.lang.Specification

class HttpCacheServiceSpec extends Specification {

    HttpCacheService service = new HttpCacheService()

    def "cache false applies the legacy no-cache headers"() {
        given:
        def response = new MockHttpServletResponse()
        long expected = System.currentTimeMillis() - 86_400_000L

        when:
        service.cache(response, false)

        then:
        response.getHeader('Cache-Control') == 'no-cache, no-store'
        response.getHeader('Pragma') == 'no-cache'
        Math.abs(response.getDateHeader('Expires') - expected) < 2_000L
    }

    def "cache true is rejected"() {
        when:
        service.cache(new MockHttpServletResponse(), true)

        then:
        thrown(IllegalArgumentException)
    }

    def "shared never-expiring cache applies the legacy one-year headers"() {
        given:
        def response = new MockHttpServletResponse()
        long now = System.currentTimeMillis()

        when:
        service.cache(response, [shared: true, neverExpires: true])

        then:
        response.getHeader('Cache-Control') == 'public, s-maxage=31536000, max-age=31536000'
        response.getDateHeader('Expires') > now + 364L * 24 * 60 * 60 * 1000
        response.getDateHeader('Expires') <= now + 366L * 24 * 60 * 60 * 1000
        response.getDateHeader('Last-Modified') > 0
    }
}
