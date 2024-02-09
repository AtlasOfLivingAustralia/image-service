package au.org.ala.images

import au.org.ala.images.utils.ImagesIntegrationSpec
import grails.core.GrailsApplication
import grails.testing.mixin.integration.Integration
import grails.gorm.transactions.*
import groovy.json.JsonSlurper
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.BlockingHttpClient
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.HttpClientConfiguration
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.http.client.multipart.MultipartBody
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Ignore
import spock.lang.Specification
import java.time.Duration

@Integration(applicationClass = Application.class)
@Rollback
class ImageUploadSpec extends ImagesIntegrationSpec {

    GrailsApplication grailsApplication

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
        def request = HttpRequest.create(HttpMethod.GET,"${baseUrl}")
        HttpResponse uploadResponse = rest.exchange(request, String)
        println(uploadResponse.status)
        then:
        uploadResponse.status == HttpStatus.OK
    }

    void "test upload image - empty request - should result in 400"() {
        when:
        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImagesFromUrls")
                .contentType("application/json")
                .body([:])
        HttpResponse resp = rest.exchange(request, String)
        then:
        HttpClientResponseException ex = thrown()
        and:
        ex.status == HttpStatus.BAD_REQUEST
    }

    void "test upload image"() {
        when:
        MultiValueMap<String, String> form = new LinkedMultiValueMap<String, String>()
        form.add("imageUrl", "https://www.ala.org.au/app/uploads/2019/05/palm-cockatoo-by-Alan-Pettigrew-1920-1200-CCBY-28072018-640x480.jpg")

        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImage")
            .contentType("application/x-www-form-urlencoded")
            .body(form)
        HttpResponse resp = rest.exchange(request, String)
        then:
        resp.status == HttpStatus.OK
    }

    void "test multi upload image - bad submission"() {
        when:

        def url1 = "https://www.ala.org.au/app/uploads/2019/05/mycena-epipterygia-by-Reiner-Richter-CCBYNCInt-26052018-1920-1200--640x480.jpg"
        def url2 = "https://www.ala.org.au/app/uploads/2019/06/Rufous-Betting-by-Graham-Armstrong-CCBY-25-Apr-2019-1920-x-1200-640x480.jpg"

        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImagesFromUrls")
                .contentType("application/json")
                .body([images:[[sourceURL:url1], [sourceURL: url2]]])
        HttpResponse resp = rest.exchange(request, String)

        then:
        HttpClientResponseException ex = thrown()
        and:
        ex.status == HttpStatus.BAD_REQUEST
    }


    void "test multi upload image - good submission"() {
        when:

        def url1 = "https://www.ala.org.au/app/uploads/2019/05/mycena-epipterygia-by-Reiner-Richter-CCBYNCInt-26052018-1920-1200--640x480.jpg"
        def url2 = "https://www.ala.org.au/app/uploads/2019/06/Rufous-Betting-by-Graham-Armstrong-CCBY-25-Apr-2019-1920-x-1200-640x480.jpg"

        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImagesFromUrls")
                .contentType("application/json")
                .body([images:[[sourceUrl:url1], [sourceUrl: url2]]])
        HttpResponse resp = rest.exchange(request, String)

        def jsonResponse = new JsonSlurper().parseText(resp.body())

        then:
        resp.status == HttpStatus.OK
        jsonResponse.success == true
        jsonResponse.results.get(url1).success == true
        jsonResponse.results.get(url2).success == true
        jsonResponse.results.get(url1).imageId != null
        jsonResponse.results.get(url2).imageId != null
    }

    @Ignore
    /* Set to ignore as it is problematic in Travis - working here */
    void 'test iNaturalist bug'(){
        when:

        MultiValueMap<String, String> form = new LinkedMultiValueMap<String, String>()
        form.add("imageUrl", "https://static.inaturalist.org/photos/35335345/original.jpeg?1555821308")
        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImage")
            .contentType("application/x-www-form-urlencoded")
            .body(form)

        HttpResponse resp = rest.exchange(request, String)
        def jsonResponse1 = new JsonSlurper().parseText(resp.body())

        MultiValueMap<String, String> form2 = new LinkedMultiValueMap<String, String>()
        form2.add("imageUrl", "https://static.inaturalist.org/photos/35335341/original.jpeg?1555821307")
        def request2 = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImage")
            .contentType("application/x-www-form-urlencoded")
            .body(form2)
        HttpResponse resp2 = rest.exchange(request2, String)
        def jsonResponse2 = new JsonSlurper().parseText(resp2.body())

        then:
        resp.status == HttpStatus.OK
        resp2.status == HttpStatus.OK
        jsonResponse1.imageId != null
        jsonResponse2.imageId != null
        jsonResponse1.imageId != jsonResponse2.imageId
    }

    void 'test multi-part upload submission'(){
        when:

        File imageFile = new File("src/integration-test/resources/test.jpg")

        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImage")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("image", imageFile)
                        .build())
        HttpResponse resp = rest.exchange(request, String)
        def jsonResponse = new JsonSlurper().parseText(resp.body())

        println("Response status: " + resp.status)
        println(resp.body)

        then:
        resp.status == HttpStatus.OK
        jsonResponse.imageId != null
    }
}
