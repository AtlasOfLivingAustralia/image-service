package au.org.ala.images

import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import grails.web.servlet.mvc.GrailsParameterMap
import org.springframework.mock.web.MockHttpServletRequest
import spock.lang.Specification

class SearchServiceSpec extends Specification implements ServiceUnitTest<SearchService>, DataTest {

    def setup() {
        mockDomains Image, ImageKeyword, Tag, ImageTag, FileSystemStorageLocation
    }

    def "findImagesByKeyword returns keyword-matched images from mocked domains"() {
        setup:
        def storageLocation = new FileSystemStorageLocation(basePath: '/tmp').save(failOnError: true)
        def imageOne = new Image(imageIdentifier: 'image-1', storageLocation: storageLocation).save(failOnError: true)
        def imageTwo = new Image(imageIdentifier: 'image-2', storageLocation: storageLocation).save(failOnError: true)
        new ImageKeyword(image: imageTwo, keyword: 'bird').save(failOnError: true)
        new ImageKeyword(image: imageOne, keyword: 'bird').save(failOnError: true)

        when:
        QueryResults<Image> results = service.findImagesByKeyword('bird', params())

        then:
        results.list*.id == [imageTwo.id, imageOne.id]
        results.totalCount == 2
    }

    def "findImagesByTagID returns tag-matched images from mocked domains"() {
        setup:
        def storageLocation = new FileSystemStorageLocation(basePath: '/tmp').save(failOnError: true)
        def imageOne = new Image(imageIdentifier: 'image-1', storageLocation: storageLocation).save(failOnError: true)
        def imageTwo = new Image(imageIdentifier: 'image-2', storageLocation: storageLocation).save(failOnError: true)
        def tag = new Tag(path: '/taxonomy/birds').save(failOnError: true)
        new ImageTag(image: imageTwo, tag: tag).save(failOnError: true)
        new ImageTag(image: imageOne, tag: tag).save(failOnError: true)

        when:
        QueryResults<Image> results = service.findImagesByTagID(tag.id.toString(), params())

        then:
        results.list*.id == [imageTwo.id, imageOne.id]
        results.totalCount == 2
    }

    private static GrailsParameterMap params() {
        new GrailsParameterMap([:], new MockHttpServletRequest())
    }
}
