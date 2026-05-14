package au.org.ala.images

import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import spock.lang.Specification
import spock.lang.Unroll

class ImageServiceSpec extends Specification implements ServiceUnitTest<ImageService>, DataTest {

    private static final List<String> TEST_IMAGE_SERVICE_URLS = ['https://images.ala.org.au/']

    @Override
    Closure doWithConfig() {
        { config ->
            config.imageServiceUrls = TEST_IMAGE_SERVICE_URLS
        }
    }

    def setup() {
        mockDomains Image, FileSystemStorageLocation, License, LicenseMapping
        service.initImagePropertyMap()
        stubSessionFactory()
    }

    def "batchUpdate keeps recognisedLicense in sync for existing images"() {
        setup:
        def originalLicense = persistLicense('OLD', 'Original License')
        def updatedLicense = persistLicense('CC-BY', 'Creative Commons Attribution')
        persistImage('img-2', 'existing-image.jpg', 'dr-2', originalLicense.acronym, originalLicense)

        when:
        def result = service.batchUpdate([[identifier: 'existing-image.jpg', dataResourceUid: 'dr-2', license: updatedLicense.acronym]], 'tester')

        then:
        result['existing-image.jpg'].success
        result['existing-image.jpg'].alreadyStored
        result['existing-image.jpg'].metadataUpdated

        with(Image.findByImageIdentifier('img-2')) {
            license == updatedLicense.acronym
            recognisedLicense == updatedLicense
        }
    }

    private void stubSessionFactory() {
        def session = Stub(org.hibernate.Session) {
            setFlushMode(_) >> null
            flush() >> null
        }
        service.sessionFactory = Stub(org.hibernate.SessionFactory) {
            getCurrentSession() >> session
        }
    }

    private License persistLicense(String acronym, String name) {
        new License(
                acronym: acronym,
                name: name,
                url: "https://example.org/licenses/${acronym.toLowerCase()}",
                imageUrl: "https://example.org/licenses/${acronym.toLowerCase()}.png"
        ).save(failOnError: true)
    }

    private Image persistImage(String imageIdentifier, String originalFilename, String dataResourceUid, String license, License recognisedLicense) {
        new Image(
                imageIdentifier: imageIdentifier,
                originalFilename: originalFilename,
                dataResourceUid: dataResourceUid,
                license: license,
                recognisedLicense: recognisedLicense,
                storageLocationName: 'test-storage'
        ).save(failOnError: true)
    }

    def "test migrate storage location happy path"() {
        setup:
        service.imageStoreService = Mock(ImageStoreService)
        service.auditService = Mock(AuditService)
        def src = new FileSystemStorageLocation(basePath: '/tmp/1').save()
        def image = new Image(imageIdentifier: '1234', mimeType: 'image/jpeg', dateDeleted: null, storageLocation: src).save()
        def dest = new FileSystemStorageLocation(basePath: '/tmp/2').save()

        when:
        service.migrateImage(image.id, dest.id, '1234', false)

        then:
        1 * service.imageStoreService.migrateImage(image, dest)
        1 * service.auditService.log(image.imageIdentifier, "Migrated to $dest", '1234')
        image.storageLocation == dest
    }

    def "test migrate storage location same storage location"() {
        setup:
        service.imageStoreService = Mock(ImageStoreService)
        service.auditService = Mock(AuditService)
        def src = new FileSystemStorageLocation(basePath: '/tmp').save()
        def image = new Image(imageIdentifier: '1234', mimeType: 'image/jpeg', dateDeleted: null, storageLocation: src).save()
        def dest = new FileSystemStorageLocation(basePath: '/tmp').save()

        when:
        service.migrateImage(image.id, dest.id, '1234', false)

        then:
        0 * service.imageStoreService.migrateImage(image, dest)
        0 * service.auditService.log(image.imageIdentifier, "Migrated to $dest", '1234')
        image.storageLocation == src
    }

    def "test migrate storage location migrate throws"() {
        setup:
        service.imageStoreService = Mock(ImageStoreService)
        service.auditService = Mock(AuditService)
        def src = new FileSystemStorageLocation(basePath: '/tmp/1').save()
        def image = new Image(imageIdentifier: '1234', mimeType: 'image/jpeg', dateDeleted: null, storageLocation: src).save()
        def dest = new FileSystemStorageLocation(basePath: '/tmp/2').save()

        when:
        service.migrateImage(image.id, dest.id, '1234', false)

        then:
        1 * service.imageStoreService.migrateImage(image, dest) >> { throw new IOException("Boo") }
        thrown IOException
        0 * service.auditService.log(image.imageIdentifier, "Migrated to $dest", '1234')
        image.storageLocation == src
    }

    @Unroll
    def "test findImageIdInImageServiceUrl(#imageUrl) == #result"() {
        expect:
        result == service.findImageIdInImageServiceUrl(imageUrl)

        where:
        imageUrl                                                                                || result
        'https://images.ala.org.au/store/4/3/2/1/1234-1234-1234-1234/original'                  || '1234-1234-1234-1234'
        'https://images.ala.org.au/store/4/3/2/1/1234-1234-1234-1234/original?param=value'      || '1234-1234-1234-1234'
        'https://images.ala.org.au/store/4/3/2/1/1234-1234-1234-1234/thumbnail'                 || '1234-1234-1234-1234'
        'https://images.ala.org.au/store/4/3/2/1/1234-1234-1234-1234/thumbnail_square'          || '1234-1234-1234-1234'
        'https://images.ala.org.au/store/4/3/2/1/1234-1234-1234-1234/thumbnail_square_black'    || '1234-1234-1234-1234'
        'https://images.ala.org.au/store/4/3/2/1/1234-1234-1234-1234/thumbnail_square_white'    || '1234-1234-1234-1234'
        'https://images.ala.org.au/store/4/3/2/1/1234-1234-1234-1234/thumbnail_square_darkGrey' || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/1234-1234-1234-1234'                                   || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/1234-1234-1234-1234/original'                          || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/1234-1234-1234-1234/thumbnail'                         || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/1234-1234-1234-1234/thumbnail_square'                  || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/1234-1234-1234-1234/thumbnail_square_black'            || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/1234-1234-1234-1234/thumbnail_square_white'            || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/1234-1234-1234-1234/thumbnail_square_darkGrey'         || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/proxyImageThumbnailLarge?id=1234-1234-1234-1234'       || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/proxyImageThumbnailLarge?imageId=1234-1234-1234-1234'  || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/proxyImageThumbnail?id=1234-1234-1234-1234'            || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/proxyImageThumbnail?imageId=1234-1234-1234-1234'       || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/proxyImage?id=1234-1234-1234-1234'                     || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/proxyImage?imageId=1234-1234-1234-1234'                || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/proxyImage?imageId=1234-1234-1234-1234&param=value'    || '1234-1234-1234-1234'
        'https://images.ala.org.au/image/viewer/1234-1234-1234-1234'                            || '1234-1234-1234-1234'
        'https://images.ala.org.au/'                                                            || ''
        'https://example.org/image/proxyImage?imageId=1234-1234-1234-1234'                      || ''
        'https://example.org/store/4/3/2/1/1234-1234-1234-1234/original'                        || ''
        'https://example.org/image/1234-1234-1234-1234'                                         || ''
        'https://example.org/image/1234-1234-1234-1234/thumbnail'                               || ''
        'https://example.org/some/garbage/original?id=test&target=other#fragment'               || ''

    }
}
