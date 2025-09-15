package au.org.ala.images

import spock.lang.Specification
import spock.lang.Ignore

@Ignore
class ImagesTagLibSpec extends Specification {

    def taglib

    def setup() {
        taglib = new ImagesTagLib()
        // Mock sanitiserService to return input unchanged
        taglib.sanitiserService = [ sanitise: { String s -> s } ] as Object
    }

    def "masks credentials for non-admin users with username and password"() {
        given:
        def url = 'https://user:secret@example.com/path?x=1'

        when:
        def masked = taglib.maskUrlCredentials(value: url, isAdmin: false)

        then:
        masked == 'https://****:****@example.com/path?x=1'
    }

    def "masks credentials for non-admin users with username only"() {
        given:
        def url = 'http://user@example.org'

        when:
        def masked = taglib.maskUrlCredentials(value: url, isAdmin: false)

        then:
        masked == 'http://****@example.org'
    }

    def "leaves URL unchanged for admin users"() {
        given:
        def url = 'https://user:secret@example.com/foo'

        expect:
        taglib.maskUrlCredentials(value: url, isAdmin: true) == url
    }

    def "leaves non-URL strings unchanged"() {
        given:
        def value = 'not a url.txt'

        expect:
        taglib.maskUrlCredentials(value: value, isAdmin: false) == value
    }
}
