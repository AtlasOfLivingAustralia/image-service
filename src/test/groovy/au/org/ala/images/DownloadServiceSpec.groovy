package au.org.ala.images

import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit tests for HTTP download improvements including redirect security,
 * sensitive parameter detection, and HttpClient support.
 */
class DownloadServiceSpec extends Specification implements ServiceUnitTest<DownloadService>, DataTest {

    def setup() {
        // Initialize the service's sensitive params
        service.sensitiveParams = ["token","key","secret","apikey","api_key","password","pwd","auth"].toSet()
//        service.disallowHttpToHttpDifferentHost = true
    }

    // =================================================================================
    // Sensitive Parameter Detection Tests
    // =================================================================================

    @Unroll
    def "hasSensitiveParams detects '#param' in query string"() {
        when:
        def result = service.hasSensitiveParams(queryString)

        then:
        result == expected

        where:
        queryString                     | expected | param
        "token=abc123"                  | true     | "token"
        "auth=bearer123"                | true     | "auth"
        "apikey=xyz789"                 | true     | "apikey"
        "api_key=xyz789"                | true     | "api_key"
        "password=secret"               | true     | "password"
        "pwd=secret"                    | true     | "pwd"
        "key=value123"                  | true     | "key"
        "secret=hidden"                 | true     | "secret"
        "size=large&format=jpg"         | false    | "none"
        "width=100&height=200"          | false    | "none"
        "mytoken=abc"                   | true     | "token (substring)"
        "access_key=xyz"                | true     | "key (substring)"
        ""                              | false    | "empty"
        null                            | false    | "null"
    }

    def "hasSensitiveParams is case insensitive"() {
        expect:
        service.hasSensitiveParams("TOKEN=abc") == true
        service.hasSensitiveParams("Auth=bearer") == true
        service.hasSensitiveParams("APIKey=xyz") == true
    }

    // =================================================================================
    // Redirect Security Tests
    // =================================================================================

    @Unroll
    def "shouldFollowRedirect: HTTP to HTTPS upgrade is always allowed"() {
        when:
        def result = service.shouldFollowRedirect(
            new URI("http://example.com/image.jpg"),
            new URI("https://example.com/image.jpg")
        )

        then:
        result == true
    }

    @Unroll
    def "shouldFollowRedirect: HTTPS to HTTPS is always allowed"() {
        when:
        def result = service.shouldFollowRedirect(
            new URI("https://example.com/old"),
            new URI("https://example.com/new")
        )

        then:
        result == true
    }

    @Unroll
    def "shouldFollowRedirect: HTTP to HTTP same host - #scenario"() {
        given:
        service.disallowHttpToHttpDifferentHost = disallowDifferentHost

        when:
        def result = service.shouldFollowRedirect(from, to)

        then:
        result == expected

        where:
        scenario                    | disallowDifferentHost | from                                      | to                                    | expected
        "same host, restriction on" | true                  | new URI("http://example.com/old")         | new URI("http://example.com/new")     | true
        "diff host, restriction on" | true                  | new URI("http://example.com/image")       | new URI("http://other.com/image")     | false
        "diff host, restriction off"| false                 | new URI("http://example.com/image")       | new URI("http://other.com/image")     | true
        "same host, restriction off"| false                 | new URI("http://example.com/old")         | new URI("http://example.com/new")     | true
    }

    @Unroll
    def "shouldFollowRedirect: HTTP to HTTP rejects userinfo"() {
        given:
        service.disallowHttpToHttpDifferentHost = false
        service.disallowHttpToHttpUserInfo = disallowUserInfo

        when:
        def result = service.shouldFollowRedirect(from, to)

        then:
        result == expected

        where:
        from                                              | to                                    | disallowUserInfo | expected
        new URI("http://user:pass@example.com/image")     | new URI("http://example.com/new")     | true             | false
        new URI("http://example.com/image")               | new URI("http://user:pass@ex.com/new")| true             | false
        new URI("http://example.com/image")               | new URI("http://example.com/new")     | true             | true
        new URI("http://user:pass@example.com/image")     | new URI("http://example.com/new")     | false            | true
        new URI("http://example.com/image")               | new URI("http://user:pass@ex.com/new")| false            | true
        new URI("http://example.com/image")               | new URI("http://example.com/new")     | false            | true
    }

    @Unroll
    def "shouldFollowRedirect: HTTP to HTTP rejects sensitive params"() {
        given:
        service.disallowHttpToHttpDifferentHost = false
        service.disallowHttpToHttpSensitiveParams = disallowSensitive

        when:
        def result = service.shouldFollowRedirect(from, to)

        then:
        result == expected

        where:
        from                                                    | to                                | disallowSensitive | expected
        new URI("http://example.com/image?token=abc")           | new URI("http://ex.com/new")      | true              | false
        new URI("http://example.com/image?apikey=xyz")          | new URI("http://ex.com/new")      | true              | false
        new URI("http://example.com/image?size=large")          | new URI("http://ex.com/new")      | true              | true
        new URI("http://example.com/image")                     | new URI("http://ex.com/new")      | true              | true
        new URI("http://example.com/image?token=abc")           | new URI("http://ex.com/new")      | false             | true
        new URI("http://example.com/image?apikey=xyz")          | new URI("http://ex.com/new")      | false             | true
        new URI("http://example.com/image?size=large")          | new URI("http://ex.com/new")      | false             | true
        new URI("http://example.com/image")                     | new URI("http://ex.com/new")      | false             | true
    }

    @Unroll
    def "shouldFollowRedirect: HTTPS to HTTP downgrade - #scenario"() {
        when:
        def result = service.shouldFollowRedirect(from, to)

        then:
        result == expected

        where:
        scenario                        | from                                                | to                                        | expected
        "same host, no sensitive data"  | new URI("https://example.com/image")                | new URI("http://example.com/new")         | true
        "diff host"                     | new URI("https://example.com/image")                | new URI("http://other.com/image")         | false
        "same host with userinfo"       | new URI("https://user:pass@example.com/image")      | new URI("http://example.com/new")         | false
        "same host with token"          | new URI("https://example.com/image?token=abc")      | new URI("http://example.com/new")         | false
        "same host safe params"         | new URI("https://example.com/image?size=large")     | new URI("http://example.com/new")         | true
    }

    // =================================================================================
    // logBadUrl Tests
    // =================================================================================

    def "logBadUrl saves URL only"() {
        given:
        def testUrl = "http://example.com/broken.jpg"
        mockDomain(FailedUpload)

        when:
        service.logBadUrl(testUrl)

        then:
        def failed = FailedUpload.findByUrl(testUrl)
        failed != null
        failed.url == testUrl
        failed.httpStatusCode == null
        failed.errorMessage == null
    }

    def "logBadUrl saves URL with status code"() {
        given:
        def testUrl = "http://example.com/notfound.jpg"
        mockDomain(FailedUpload)

        when:
        service.logBadUrl(testUrl, 404)

        then:
        def failed = FailedUpload.findByUrl(testUrl)
        failed != null
        failed.url == testUrl
        failed.httpStatusCode == 404
        failed.errorMessage == null
    }

    def "logBadUrl saves URL with status code and error message"() {
        given:
        def testUrl = "http://example.com/error.jpg"
        def errorMsg = "Not Found"
        mockDomain(FailedUpload)

        when:
        service.logBadUrl(testUrl, 404, errorMsg)

        then:
        def failed = FailedUpload.findByUrl(testUrl)
        failed != null
        failed.url == testUrl
        failed.httpStatusCode == 404
        failed.errorMessage == errorMsg
    }

    // =================================================================================
    // isBadUrl Tests
    // =================================================================================

    def "isBadUrl returns true for failed URL"() {
        given:
        def testUrl = "http://example.com/failed.jpg"
        mockDomain(FailedUpload)
        new FailedUpload(url: testUrl).save(flush: true)

        when:
        def result = service.isBadUrl(testUrl)

        then:
        result == true
    }

    def "isBadUrl returns false for non-failed URL"() {
        given:
        mockDomain(FailedUpload)

        when:
        def result = service.isBadUrl("http://example.com/good.jpg")

        then:
        result == false
    }

    // =================================================================================
    // Configuration Tests
    // =================================================================================

    def "disallowHttpToHttpDifferentHost defaults to false"() {
        given:
        def newService = new DownloadService()

        expect:
        newService.disallowHttpToHttpDifferentHost == false
    }

    @Unroll
    def "sensitive params can be empty"() {
        given:
        service.sensitiveParams = params

        expect:
        service.hasSensitiveParams("token=abc") == expected
        service.hasSensitiveParams("anything=value") == expected

        where:
        params | expected
        []     | false
        null   | false
    }

    // =================================================================================
    // Helper Methods Tests
    // =================================================================================
}

