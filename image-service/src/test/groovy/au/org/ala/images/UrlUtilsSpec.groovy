package au.org.ala.images

import spock.lang.Specification

class UrlUtilsSpec extends Specification {

    def "masks credentials for non-admin users with username and password"() {
        expect:
        UrlUtils.maskCredentials('https://user:secret@example.com/path?x=1', false) == 'https://****:****@example.com/path?x=1'
    }

    def "masks credentials for non-admin users with username only"() {
        expect:
        UrlUtils.maskCredentials('http://user@example.org', false) == 'http://****@example.org'
    }

    def "leaves URL unchanged for admin users"() {
        expect:
        UrlUtils.maskCredentials('https://user:secret@example.com/foo', true) == 'https://user:secret@example.com/foo'
    }

    def "leaves non-URL strings unchanged"() {
        expect:
        UrlUtils.maskCredentials('not a url.txt', false) == 'not a url.txt'
    }
}
