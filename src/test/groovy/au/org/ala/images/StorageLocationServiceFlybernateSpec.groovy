package au.org.ala.images

import au.org.ala.images.helper.FlybernateSpec
import grails.testing.services.ServiceUnitTest

class StorageLocationServiceFlybernateSpec extends FlybernateSpec implements ServiceUnitTest<StorageLocationService> {

    // GORM Unit testing returns an ArrayList for Criteria.scroll {}, so we run
    // this test against a real database
    def 'test migrate'() {
        setup:
        def src = new FileSystemStorageLocation(basePath: '/tmp/1').save(flush: true)
        def dst = new S3StorageLocation(region: 'region', bucket: 'bucket', prefix: 'prefix', accessKey: 'a', secretKey: 's', publicRead: false).save(flush: true)
        def img1 = new Image(imageIdentifier: UUID.randomUUID().toString(), storageLocation: src).save(flush: true)
        def img2 = new Image(imageIdentifier: UUID.randomUUID().toString(), storageLocation: src).save(flush: true)
        def img3 = new Image(imageIdentifier: UUID.randomUUID().toString(), storageLocation: src).save(flush: true)

        service.imageService = Mock(ImageService)

        when:
        service.migrate(src.id, dst.id, '1234', false)

        then:
        1 * service.imageService.scheduleBackgroundTask(new MigrateStorageLocationTask(imageId: img1.id, destinationStorageLocationId: dst.id, userId: '1234', imageService: service.imageService, deleteSource: false))
        1 * service.imageService.scheduleBackgroundTask(new MigrateStorageLocationTask(imageId: img2.id, destinationStorageLocationId: dst.id, userId: '1234', imageService: service.imageService, deleteSource: false))
        1 * service.imageService.scheduleBackgroundTask(new MigrateStorageLocationTask(imageId: img3.id, destinationStorageLocationId: dst.id, userId: '1234', imageService: service.imageService, deleteSource: false))
    }

}
