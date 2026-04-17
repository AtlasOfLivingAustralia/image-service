package au.org.ala.images

import au.org.ala.images.utils.ImagesIntegrationSpec
import grails.testing.mixin.integration.Integration
import grails.gorm.transactions.Rollback
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.BlockingHttpClient
import io.micronaut.http.client.HttpClient
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Shared

@Integration(applicationClass = Application.class)
@Rollback
class CacheControlInterceptorSpec extends ImagesIntegrationSpec {

    @Shared
    String imageId

    def grailsApplication

    private URL getBaseUrl() {
        def serverContextPath = grailsApplication.config.getProperty('server.servlet.context-path', String, '')
        def url = "http://localhost:${serverPort}${serverContextPath}"
        return url.toURL()
    }

    private BlockingHttpClient getClient() {
        HttpClient.create(baseUrl).toBlocking()
    }

    def setup() {
        if (!imageId) {
            MultiValueMap<String, String> form = new LinkedMultiValueMap<String, String>()
            form.add("imageUrl", "https://upload.wikimedia.org/wikipedia/commons/e/ed/Puma_concolor_camera_trap_Arizona_2.jpg")

            def request = HttpRequest.create(HttpMethod.POST, "${baseUrl}/ws/uploadImage")
                    .contentType("application/x-www-form-urlencoded")
                    .header('User-Agent', userAgent())
                    .body(form)
            HttpResponse<Map> uploadResponse = client.exchange(request, Map)
            imageId = uploadResponse.body().imageId
        }
    }

    void "Test /image/details/ID has no-cache headers (HTML)"() {
        when:
        def request = HttpRequest.GET("${baseUrl}/image/details/${imageId}")
                .header('User-Agent', userAgent())
        HttpResponse resp = client.exchange(request, String)

        then:
        resp.status == HttpStatus.OK
        resp.header("Cache-Control")?.contains("no-cache")
        resp.header("Cache-Control")?.contains("no-store")
        resp.header("Vary")?.contains("Accept")
        resp.header("X-Content-Type-Options") == "nosniff"
        resp.header("X-Frame-Options") == "SAMEORIGIN"
    }

    void "Test /ws/ JSON endpoints have no-cache headers"() {
        when:
        def request = HttpRequest.GET("${baseUrl}/ws/image/${imageId}")
                .header('User-Agent', userAgent())
        HttpResponse resp = client.exchange(request, Map)

        then:
        resp.status == HttpStatus.OK
        resp.header('Content-Type')?.contains('application/json')
        resp.header("Cache-Control")?.contains("no-cache")
        resp.header("Cache-Control")?.contains("no-store")
    }

    void "Test /ws/exportCSV has no-cache headers for gzipped csv"() {
        when:
        def request = HttpRequest.GET("${baseUrl}/ws/exportCSV")
                .header('User-Agent', userAgent())
        HttpResponse resp = client.exchange(request, byte[])

        then:
        resp.status == HttpStatus.OK
        resp.header('Content-Type')?.contains('application/gzip')
        resp.header('Content-disposition')?.contains('.csv.gz')
        resp.header("Cache-Control")?.contains("no-cache")
        resp.header("Cache-Control")?.contains("no-store")
    }

    void "Test /image/ID/thumbnail has public caching headers (excluded)"() {
        when:
        def request = HttpRequest.GET("${baseUrl}/image/${imageId}/thumbnail")
                .header('User-Agent', userAgent())
        HttpResponse resp = client.exchange(request, byte[])

        then:
        resp.status == HttpStatus.OK
        // Should NOT have no-cache
        !resp.header("Cache-Control").contains("no-cache")
    }
}
