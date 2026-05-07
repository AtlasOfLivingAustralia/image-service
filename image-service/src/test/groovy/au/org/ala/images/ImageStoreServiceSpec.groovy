package au.org.ala.images

import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import com.google.common.io.Resources
import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import org.grails.plugins.testing.GrailsMockMultipartFile
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ImageStoreServiceSpec extends Specification implements ServiceUnitTest<ImageStoreService>, DataTest {

    IImageThumbnailer imageThumbnailer = Mock(IImageThumbnailer)
    IImageTiler imageTiler = Mock(IImageTiler)
    IOnDemandImageTiler onDemandImageTiler = Mock(IOnDemandImageTiler)

    def setup() {
        applicationContext.registerBean("imageThumbnailer", IImageThumbnailer, { -> imageThumbnailer })
        applicationContext.registerBean("imageTiler", IImageTiler, { -> imageTiler })
        applicationContext.registerBean("onDemandImageTiler", IOnDemandImageTiler, { -> onDemandImageTiler })

        service.auditService = Mock(AuditService)
        service.storageLocationService = Mock(StorageLocationService)
    }

    def "test store tiles zip"() {
        setup:
        def image = Mock(Image)
        def storageLocation = Mock(StorageLocation)
        service.storageLocationService.getStorageOperationsForImage(image) >> storageLocation
        def uuid = UUID.randomUUID().toString()
        image.getImageIdentifier() >> uuid
        image.getStorageLocation() >> storageLocation
        def mpf = new GrailsMockMultipartFile('upload.zip', 'upload.zip', 'application/zip', Resources.getResource('test.zip').newInputStream())

        when:
        service.storeTilesArchiveForImage(image, mpf)

        then:

        1 * storageLocation.stored(uuid) >> true
        // no directories entries call storeTileZipInputStream
        0 * storageLocation.storeTileZipInputStream(uuid, '0/0/', _, 0, _)
        // file entries do call storeTileZipInputStream
        1 * storageLocation.storeTileZipInputStream(uuid, '0/0/0.png', 'image/jpeg', 1994, _)
        1 *  service.auditService.log(uuid, 'Image tiles stored from zip file (outsourced job?)', 'N/A')
    }

    def "test storeTileZipInputStream prevents Zip Slip"() {
        given:
        def image = Mock(Image)
        image.imageIdentifier >> "123"

        when:
        service.storeTileZipInputStream(image, "../../etc/passwd", "text/plain", 10, null)

        then:
        thrown(IllegalArgumentException)
    }

    def "thumbnailImageInfo coalesces concurrent loads for same key when cache is enabled"() {
        given:
        service.initSemaphores()
        service.init()
        service.disableCache = false
        def callCount = new AtomicInteger(0)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-1', '') >> {
                callCount.incrementAndGet()
                Thread.sleep(200)
                new ImageInfo(exists: true, imageIdentifier: 'img-1', contentType: 'image/jpeg', extension: 'jpg', inputStreamSupplier: { r -> new ByteArrayInputStream(new byte[0]) })
            }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations

        def gate = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(2)

        when:
        def f1 = pool.submit({ gate.await(); service.thumbnailImageInfo('img-1', '', false) } as Callable<ImageInfo>)
        def f2 = pool.submit({ gate.await(); service.thumbnailImageInfo('img-1', '', false) } as Callable<ImageInfo>)
        gate.countDown()
        def r1 = f1.get(5, TimeUnit.SECONDS)
        def r2 = f2.get(5, TimeUnit.SECONDS)

        then:
        r1.exists
        r2.exists
        callCount.get() == 1

        cleanup:
        pool.shutdownNow()
    }

    def "thumbnailImageInfo performs separate loads when cache is disabled"() {
        given:
        service.initSemaphores()
        service.init()
        service.disableCache = true
        def callCount = new AtomicInteger(0)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-2', '') >> {
                callCount.incrementAndGet()
                Thread.sleep(100)
                new ImageInfo(exists: true, imageIdentifier: 'img-2', contentType: 'image/jpeg', extension: 'jpg', inputStreamSupplier: { r -> new ByteArrayInputStream(new byte[0]) })
            }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations

        def gate = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(2)

        when:
        def f1 = pool.submit({ gate.await(); service.thumbnailImageInfo('img-2', '', false) } as Callable<ImageInfo>)
        def f2 = pool.submit({ gate.await(); service.thumbnailImageInfo('img-2', '', false) } as Callable<ImageInfo>)
        gate.countDown()
        f1.get(5, TimeUnit.SECONDS)
        f2.get(5, TimeUnit.SECONDS)

        then:
        callCount.get() == 2

        cleanup:
        pool.shutdownNow()
    }

}
