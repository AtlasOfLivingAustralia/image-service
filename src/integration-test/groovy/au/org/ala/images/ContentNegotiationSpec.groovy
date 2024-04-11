package au.org.ala.images

import au.org.ala.images.utils.ImagesIntegrationSpec
import grails.testing.mixin.integration.Integration
import grails.gorm.transactions.Rollback
import groovy.json.JsonSlurper
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.client.BlockingHttpClient
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap

import spock.lang.Specification

import java.security.MessageDigest

/**
 * Content negotiation tests for /image/<UUID> URLs
 */
@Integration(applicationClass = Application.class)
@Rollback
class ContentNegotiationSpec extends ImagesIntegrationSpec {

    def imageId
    def grailsApplication

    private URL getBaseUrl() {
        def serverContextPath = grailsApplication.config.getProperty('server.servlet.context-path', String, '')
        def url = "http://localhost:${serverPort}${serverContextPath}"
        return url.toURL()
    }

    def setup() {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<String, String>()
        form.add("imageUrl", "https://upload.wikimedia.org/wikipedia/commons/e/ed/Puma_concolor_camera_trap_Arizona_2.jpg")

        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImage")
                .contentType("application/x-www-form-urlencoded")
                .body(form)
        HttpResponse uploadResponse = rest.exchange(request, String)

        def jsonResponse = new JsonSlurper().parseText(uploadResponse.body())
        imageId = jsonResponse.imageId

        assert imageId != null
    }

    def cleanup() {
    }

    private BlockingHttpClient getRest() {
        HttpClient.create(baseUrl).toBlocking()
    }

    /**
     * Testing equivalent of
     * curl -X GET "https://images.ala.org.au/image/1a6dc180-96b1-45df-87da-7d0912dddd4f" -H "Accept: application/json"
     */
    void "Test accept: application/json"() {
        when:
        def request = HttpRequest.create(HttpMethod.GET,"${baseUrl}/image/${imageId}")
            .accept(MediaType.APPLICATION_JSON_TYPE)
        HttpResponse uploadResponse = rest.exchange(request, String)

        println("response received")
        def jsonResponse = new JsonSlurper().parseText(uploadResponse.body())
        println("response parsed")

        then:
        uploadResponse.status == HttpStatus.OK
        println("checking")
        jsonResponse.originalFileName != null
        println("checked")
    }

    /**
     * Testing equivalent of
     * curl -X GET "https://images.ala.org.au/image/ABC" -H "Accept: application/json"
     */
    void "Test accept: application/json with expected 404"() {
        when:
        def request = HttpRequest.create(HttpMethod.GET,"${baseUrl}/image/ABC")
            .accept(MediaType.APPLICATION_JSON_TYPE)
        HttpResponse resp = rest.exchange(request, String)

        then:
        HttpClientResponseException ex = thrown()
        def jsonResponse = new JsonSlurper().parseText(ex.response.body())
        and:
        ex.status == HttpStatus.NOT_FOUND
        jsonResponse.success == false
    }

    /**
     * Testing equivalent of
     * curl -X GET "https://images.ala.org.au/ws/image/ABC" -H "Accept: application/json"
     */
    void "Test WS accept: application/json with expected 404"() {
        when:
        def request = HttpRequest.create(HttpMethod.GET, "${baseUrl}/image/ABC")
            .accept(MediaType.APPLICATION_JSON_TYPE)
        HttpResponse resp = rest.exchange(request, String)

        then:
        HttpClientResponseException ex = thrown()
        def jsonResponse = new JsonSlurper().parseText(ex.response.body())
        and:
        ex.status == HttpStatus.NOT_FOUND
        jsonResponse.success == false
    }

    /**
     * Testing equivalent of
     * curl -X GET "https://images.ala.org.au/image/1a6dc180-96b1-45df-87da-7d0912dddd4f" -H "Accept: image/jpeg"
     */
    void "Test accept: image/jpeg"() {
        when:

        def request = HttpRequest.create(HttpMethod.GET, "${baseUrl}/image/${imageId}")
                .accept("image/jpeg")

        def resp = rest.exchange(request, byte[])
        def imageInBytes = resp.body()

        MessageDigest md = MessageDigest.getInstance("MD5")
        def md5Hash = md.digest(imageInBytes)

        //compare image with source
        def imageAsBytes = new URL("https://upload.wikimedia.org/wikipedia/commons/e/ed/Puma_concolor_camera_trap_Arizona_2.jpg").getBytes()

        def md5Hash2 =  md.digest(imageAsBytes)

        then:
        md5Hash == md5Hash2
    }

    /**
     * Testing equivalent of
     * curl -X GET "https://images.ala.org.au/image/1a6dc180-96b1-45df-87da-7d0912dddd4f" -H "Accept: image/jpeg"
     */
    void "Test accept: image/jpeg - 404"() {
        when:

        def request = HttpRequest.create(HttpMethod.GET, "${baseUrl}/image/ABC")
                .accept("image/jpeg")

        def resp = rest.exchange(request, byte[])

        then:
        def e = thrown(HttpClientResponseException)
        assert e.status.code == 404
    }
}