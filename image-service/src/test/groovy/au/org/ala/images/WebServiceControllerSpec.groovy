package au.org.ala.images

import grails.testing.gorm.DataTest
import grails.testing.web.controllers.ControllerUnitTest
import org.grails.web.util.GrailsApplicationAttributes
import spock.lang.Specification

class WebServiceControllerSpec extends Specification implements ControllerUnitTest<WebServiceController>, DataTest {

    def setupSpec() {
        mockDomains Image
    }

    def "deleteImageService falls back to json when callback is invalid"() {
        setup:
        request.addHeader('Accept', 'text/json')
        request.setAttribute(GrailsApplicationAttributes.RESPONSE_FORMAT, 'json')
        request.addHeader('apiKey', 'api-key-user')
        params.imageID = 'missing-image'
        params.callback = 'alert(1)'

        when:
        controller.deleteImageService()

        then:
        response.contentType == 'application/json;charset=UTF-8'
        !response.contentAsString.startsWith('alert(1)(')
        response.json.success == false
        response.json.message == 'Invalid image identifier.'
    }
}
