package au.org.ala.images

import grails.testing.web.taglib.TagLibUnitTest
import spock.lang.Specification

class ImagesTagLibSpec extends Specification implements TagLibUnitTest<ImagesTagLib> {

    def setup() {
        tagLib.sanitiserService = new SanitiserService()
    }

    void 'test sanitised markup is created'() {
        when:
        def expected = '''<a href="http://example.org" rel="nofollow">Link</a>'''
        def output = applyTemplate('''<img:sanitise value="${'<a href="http://example.org" onclick=alert(1)>Link</a>'}"/>''')

        then:
        output == expected
    }

    void 'sanitiseString length, image and key parameters are optional'() {
        given:
        def expected = '''<a href="http://example.org" rel="nofollow">Link</a>'''

        expect:
        tagLib.sanitise(value: '<a href="http://example.org" onclick=alert(1)>Link</a>') == expected
    }

    void 'sanitiseString length parameter is applied'() {
        given:
        def expected = '''<a href="http://example.org" rel="nofollow">Link</a>'''

        expect:
        tagLib.sanitiseString(value: '<a href="http://example.org" onclick=alert(1)>Link</a>') == expected
    }

    void 'sanitise length, image and key parameters are optional'() {
        given:
        def expected = '''<a href="http://example.org" rel="nofollow">Link</a>'''

        expect:
        tagLib.sanitise(value: '<a href="http://example.org" onclick=alert(1)>Link</a>') == expected
    }

    void 'sanitise length parameter is applied'() {
        given:
        def expected = '''<a href="http://example.org" rel="nofollow">Some...</a>'''

        expect:
        tagLib.sanitise(value: '<a href="http://example.org" onclick=alert(1)>Some Text</a>', length: 7) == expected
    }

    def "masks credentials for non-admin users with username and password"() {
        given:
        def url = 'https://user:secret@example.com/path?x=1'

        when:
        def masked = tagLib.maskUrlCredentials(value: url, isAdmin: false)

        then:
        masked == 'https://****:****@example.com/path?x=1'
    }

    def "masks credentials for non-admin users with username only"() {
        given:
        def url = 'http://user@example.org'

        when:
        def masked = tagLib.maskUrlCredentials(value: url, isAdmin: false)

        then:
        masked == 'http://****@example.org'
    }

    def "leaves URL unchanged for admin users"() {
        given:
        def url = 'https://user:secret@example.com/foo'

        expect:
        tagLib.maskUrlCredentials(value: url, isAdmin: true) == url
    }

    def "leaves non-URL strings unchanged"() {
        given:
        def value = 'not a url.txt'

        expect:
        tagLib.maskUrlCredentials(value: value, isAdmin: false) == value
    }
}