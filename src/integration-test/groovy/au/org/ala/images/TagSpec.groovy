package au.org.ala.images

import au.org.ala.images.utils.ImagesIntegrationSpec
import grails.testing.mixin.integration.Integration
import grails.gorm.transactions.Rollback
import groovy.json.JsonSlurper
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.BlockingHttpClient
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.HttpClientConfiguration
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Ignore
import spock.lang.Specification

import java.time.Duration

@Integration(applicationClass = Application.class)
@Rollback
class TagSpec extends ImagesIntegrationSpec {

    def grailsApplication

    private URL getBaseUrl() {
        def serverContextPath = grailsApplication.config.getProperty('server.servlet.context-path', String, '')
        def url = "http://localhost:${serverPort}${serverContextPath}"
        return url.toURL()
    }

    def setup() {}

    def cleanup() {}

    private BlockingHttpClient getRest() {
        HttpClientConfiguration configuration = new DefaultHttpClientConfiguration()
        configuration.readTimeout = Duration.ofSeconds(30)
        HttpClient.create(baseUrl, configuration).toBlocking()
    }

    @Ignore
    //Fail in the jenkins
    void "test home page"() {
        when:
        def request = HttpRequest.create(HttpMethod.GET, "${baseUrl}")
        HttpResponse resp = rest.exchange(request, String)
        then:
        resp.status == HttpStatus.OK
    }

    void "test create tag"() {
        when:
        def request = HttpRequest.create(HttpMethod.PUT,"${baseUrl}/ws/tagWS?tagPath=Birds/Colour/Red")
        HttpResponse resp = rest.exchange(request, String)
        def jsonResponse = new JsonSlurper().parseText(resp.body())
        then:
        resp.status == HttpStatus.OK
        jsonResponse.tagId != null
    }

    void "test get tag model"() {
        when:
        def request = HttpRequest.create(HttpMethod.GET,"${baseUrl}/ws/tagsWS")
        HttpResponse resp = rest.exchange(request, String)
        def jsonResponse = new JsonSlurper().parseText(resp.body())
        then:
        resp.status == HttpStatus.OK
        jsonResponse.size() > 0
    }

    void "test tag an image"(){
        when:

        //upload an image
        MultiValueMap<String, String> form = new LinkedMultiValueMap<String, String>()
        form.add("imageUrl", "https://www.ala.org.au/app/uploads/2019/05/palm-cockatoo-by-Alan-Pettigrew-1920-1200-CCBY-28072018-640x480.jpg")
        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImage")
            .contentType("application/x-www-form-urlencoded")
            .body(form)
        HttpResponse resp = rest.exchange(request, String)

        def jsonResponse = new JsonSlurper().parseText(resp.body())
        def imageId = jsonResponse.imageId

        println("Created image: " + imageId)

        //create a tag
        def request2 = HttpRequest.create(HttpMethod.PUT, "${baseUrl}/ws/tagWS?tagPath=Birds/Colour/Blue")
        HttpResponse createTagResp = rest.exchange(request2, String)
        def tagId = new JsonSlurper().parseText(createTagResp.body()).tagId

        //remove existing tags if present
        def request3 = HttpRequest.create(HttpMethod.DELETE, "${baseUrl}/ws/tag/${tagId}/imageWS/${imageId}")
        HttpResponse tagRemoveResp = rest.exchange(request3, String)
        println("Delete response status: " + tagRemoveResp.body())

        //tag the image
        def request4 = HttpRequest.create(HttpMethod.PUT, "${baseUrl}/ws/tag/${tagId}/imageWS/${imageId}")
        HttpResponse tagResp = rest.exchange(request4, String)
        def taggedJson = new JsonSlurper().parseText(tagResp.body())

        println("Create Response status: " + resp.status)
        println(resp.body)


        then:
        tagResp.status == HttpStatus.OK
        taggedJson.success == true
    }

}
