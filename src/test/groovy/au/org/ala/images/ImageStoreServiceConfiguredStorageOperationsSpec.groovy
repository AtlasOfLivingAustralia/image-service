package au.org.ala.images

import au.org.ala.images.storage.StorageOperations
import com.google.common.io.ByteSource
import grails.testing.services.ServiceUnitTest
import org.grails.orm.hibernate.cfg.GrailsHibernateUtil
import spock.lang.Specification

class ImageStoreServiceConfiguredStorageOperationsSpec extends Specification implements ServiceUnitTest<ImageStoreService> {

    ConfiguredStorageOperationsService configuredStorageOperationsService
    StorageOperations mockConfiguredStorageOperations
    StorageOperations mockImageStorageOperations
    Image mockImage
    StorageLocation mockStorageLocation

    def setup() {
        mockConfiguredStorageOperations = Mock(StorageOperations)
        mockImageStorageOperations = Mock(StorageOperations)
        mockStorageLocation = Mock(StorageLocation)
        mockImage = Mock(Image)

        configuredStorageOperationsService = Mock(ConfiguredStorageOperationsService)
        service.configuredStorageOperationsService = configuredStorageOperationsService

        // Set up the mock image and storage location
        mockImage.getStorageLocation() >> mockStorageLocation
        mockImage.getImageIdentifier() >> "test-image-id"
        mockStorageLocation.asStandaloneStorageOperations() >> mockImageStorageOperations
    }

    def cleanup() {
    }

    void "test getStorageOperationsToUse returns configured StorageOperations when available"() {
        given:
        configuredStorageOperationsService.getConfiguredStorageOperations() >> mockConfiguredStorageOperations

        when:
        def result = service.getStorageOperationsToUse(mockImage)

        then:
        result == mockConfiguredStorageOperations
    }

    void "test getStorageOperationsToUse returns image's StorageOperations when configured StorageOperations is not available"() {
        given:
        configuredStorageOperationsService.getConfiguredStorageOperations() >> null

        when:
        def result = service.getStorageOperationsToUse(mockImage)

        then:
        result == mockImageStorageOperations
    }

    void "test generateImageThumbnails uses configured StorageOperations when available"() {
        given:
        configuredStorageOperationsService.getConfiguredStorageOperations() >> mockConfiguredStorageOperations
        mockImage.originalInputStream() >> Mock(InputStream)

        when:
        service.generateImageThumbnails(mockImage)

        then:
        1 * service.generateThumbnailsImpl(_, "test-image-id", mockConfiguredStorageOperations)
        0 * service.generateThumbnailsImpl(_, "test-image-id", mockImageStorageOperations)
    }

    void "test generateImageThumbnails uses image's StorageOperations when configured StorageOperations is not available"() {
        given:
        configuredStorageOperationsService.getConfiguredStorageOperations() >> null
        mockImage.originalInputStream() >> Mock(InputStream)

        when:
        service.generateImageThumbnails(mockImage)

        then:
        1 * service.generateThumbnailsImpl(_, "test-image-id", mockImageStorageOperations)
        0 * service.generateThumbnailsImpl(_, "test-image-id", mockConfiguredStorageOperations)
    }

    void "test generateTMSTiles uses configured StorageOperations when available"() {
        given:
        configuredStorageOperationsService.getConfiguredStorageOperations() >> mockConfiguredStorageOperations
        mockImage.getZoomLevels() >> 3

        when:
        service.generateTMSTiles(mockImage)

        then:
        1 * service.generateTMSTiles("test-image-id", mockConfiguredStorageOperations, 3, null)
        0 * service.generateTMSTiles("test-image-id", mockImageStorageOperations, 3, null)
    }

    void "test generateTMSTiles uses image's StorageOperations when configured StorageOperations is not available"() {
        given:
        configuredStorageOperationsService.getConfiguredStorageOperations() >> null
        mockImage.getZoomLevels() >> 3

        when:
        service.generateTMSTiles(mockImage)

        then:
        1 * service.generateTMSTiles("test-image-id", mockImageStorageOperations, 3, null)
        0 * service.generateTMSTiles("test-image-id", mockConfiguredStorageOperations, 3, null)
    }
}