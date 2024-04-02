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
import io.micronaut.http.client.HttpClient

@Integration(applicationClass = Application.class)
@Rollback
class SearchSpec extends ImagesIntegrationSpec {

    def grailsApplication

    private URL getBaseUrl() {
        def serverContextPath = grailsApplication.config.getProperty('server.servlet.context-path', String, '')
        def url = "http://localhost:${serverPort}${serverContextPath}"
        return url.toURL()
    }

    private BlockingHttpClient getRest() {
        HttpClient.create(baseUrl).toBlocking()
    }

    void 'test upload'() {

        when:
        def occurrenceID = "f4c13adc-2926-44c8-b2cd-fb2d62378a1a"

        //first upload an image
        def testUrl = "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f1/Red_kangaroo_-_melbourne_zoo.jpg/800px-Red_kangaroo_-_melbourne_zoo.jpg"

        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/uploadImagesFromUrls")
        .contentType("application/json")
                .body([images: [[
                                  sourceUrl   : testUrl,
                                  occurrenceID: occurrenceID
                          ]]])
        HttpResponse uploadResponse = rest.exchange(request, String)
        def jsonUploadResponse = new JsonSlurper().parseText(uploadResponse.body())

        then:
        uploadResponse.status == HttpStatus.OK
        jsonUploadResponse.success == true
    }


//    void 'test search for previous upload'() {
//
//        when:
//
//        Thread.sleep(5000)
//
//        boolean hasBacklog = true
//        int counter = 0
//        int MAX_CHECKS = 10
//
//        while (hasBacklog && counter < MAX_CHECKS) {
//            def request = HttpRequest.create(HttpMethod.GET,"${baseUrl}/ws/backgroundQueueStats")
//            HttpResponse response = rest.exchange(request, String)
//            def json = new JsonSlurper().parseText(response.body())
//            if (json.queueLength > 0) {
//                println("Queue length: " + json.queueLength)
//                Thread.sleep(5000)
//            } else {
//                hasBacklog = false
//            }
//            counter += 1
//        }
//
//        def countRequest = HttpRequest.create(HttpMethod.GET,"${baseUrl}/ws/search")
//        HttpResponse countResponse = rest.exchange(countRequest, String)
//        def jsonCount = new JsonSlurper().parseText(countResponse.body())
//        jsonCount
//
//        //search by occurrence ID
//        def request = HttpRequest.create(HttpMethod.POST,"${baseUrl}/ws/findImagesByMetadata")
//                .contentType("application/json")
//                .body([
//                    key : "occurrenceid", values : ["f4c13adc-2926-44c8-b2cd-fb2d62378a1a"]
//                ])
//        HttpResponse searchResponse = rest.exchange(request, String)
//
//        def jsonResponse = new JsonSlurper().parseText(searchResponse.body())
//
//        then:
//        searchResponse.status == HttpStatus.OK
//        jsonResponse.count > 0
//        //check for legacy fields
//        jsonResponse.images.size() > 0
//        jsonResponse.images.get("f4c13adc-2926-44c8-b2cd-fb2d62378a1a")[0].imageId != null
//        jsonResponse.images.get("f4c13adc-2926-44c8-b2cd-fb2d62378a1a")[0].tileZoomLevels != null
//        jsonResponse.images.get("f4c13adc-2926-44c8-b2cd-fb2d62378a1a")[0].filesize != null
//        jsonResponse.images.get("f4c13adc-2926-44c8-b2cd-fb2d62378a1a")[0].imageUrl != null
//        jsonResponse.images.get("f4c13adc-2926-44c8-b2cd-fb2d62378a1a")[0].largeThumbUrl != null
//        jsonResponse.images.get("f4c13adc-2926-44c8-b2cd-fb2d62378a1a")[0].squareThumbUrl != null
//        jsonResponse.images.get("f4c13adc-2926-44c8-b2cd-fb2d62378a1a")[0].thumbUrl != null
//    }
}
