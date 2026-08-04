package au.org.ala.images

import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import com.google.common.io.ByteSource
import com.google.common.io.Resources
import org.apache.commons.lang3.tuple.Pair
import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import org.grails.plugins.testing.GrailsMockMultipartFile
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BiFunction
import javax.imageio.ImageIO
import java.awt.Color
import java.awt.image.BufferedImage

class ImageStoreServiceSpec extends Specification implements ServiceUnitTest<ImageStoreService>, DataTest {

    IImageThumbnailer imageThumbnailer = Mock(IImageThumbnailer)
    IImageTiler imageTiler = Mock(IImageTiler)
    IOnDemandImageTiler onDemandImageTiler = Mock(IOnDemandImageTiler)

    def setup() {
        applicationContext.registerBean("imageThumbnailer", IImageThumbnailer, { -> imageThumbnailer })
        applicationContext.registerBean("imageTiler", IImageTiler, { -> imageTiler })
        applicationContext.registerBean("onDemandImageTiler", IOnDemandImageTiler, { -> onDemandImageTiler })
        applicationContext.registerBean("derivativeLoaderExecutor", Executor, { -> { Runnable task -> task.run() } as Executor })

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

    def "storeImage supplies the optimised stream and length to storage"() {
        given:
        def originalBytes = pngBytes(1, 1)
        def transformedBytes = pngBytes(2, 2)
        assert transformedBytes.length != originalBytes.length
        def optimiserOutputDirectory = java.nio.file.Files.createTempDirectory('image-store-optimiser-test').toFile()
        def optimiserOutput = new File(optimiserOutputDirectory, 'optimised.png')
        optimiserOutput.bytes = transformedBytes
        def optimisationService = Mock(ImageOptimisationService)
        def operations = Mock(StorageOperations)
        byte[] storedBytes
        Long storedLength
        service.grailsApplication.config.images.optimisation.enabled = true
        service.imageOptimisationService = optimisationService

        when:
        service.storeImage(ByteSource.wrap(originalBytes), operations, 'image/png', 'original.png')

        then:
        1 * optimisationService.optimise(_ as File, 'image/png') >> new ImageOptimisationService.OptimisationResult(
                optimisedFile: optimiserOutput,
                outputContentType: 'image/png'
        )
        1 * operations.store(_, _, 'image/png', null, _) >> { String uuid, InputStream stream, String contentType, String contentDisposition, Long length ->
            storedBytes = stream.bytes
            storedLength = length
        }
        storedBytes == transformedBytes
        storedLength == transformedBytes.length

        cleanup:
        optimiserOutputDirectory.deleteDir()
    }

    private static byte[] pngBytes(int width, int height) {
        def image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        def graphics = image.graphics
        try {
            graphics.color = Color.BLUE
            graphics.fillRect(0, 0, width, height)
        } finally {
            graphics.dispose()
        }
        def output = new ByteArrayOutputStream()
        ImageIO.write(image, 'png', output)
        return output.toByteArray()
    }

    def "thumbnailImageInfo coalesces concurrent loads for same key when cache is enabled"() {
        given:
        ExecutorService derivativeExecutor = Executors.newSingleThreadExecutor({ Runnable runnable -> new Thread(runnable, 'derivative-loader-test-') })
        service.initSemaphores()
        service.derivativeLoaderExecutor = derivativeExecutor
        service.init()
        service.disableCache = false
        def callCount = new AtomicInteger(0)
        def loaderThreadName = new AtomicReference<String>()
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-1', '') >> {
                callCount.incrementAndGet()
                loaderThreadName.set(Thread.currentThread().name)
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
        loaderThreadName.get().startsWith('derivative-loader-test-')

        cleanup:
        pool.shutdownNow()
        derivativeExecutor.shutdownNow()
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

    def "thumbnailImageInfo evicts a timed-out cache future so a later request can retry"() {
        given:
        ExecutorService derivativeExecutor = Executors.newSingleThreadExecutor()
        CountDownLatch firstLoadStarted = new CountDownLatch(1)
        CountDownLatch allowFirstLoadToFinish = new CountDownLatch(1)
        AtomicInteger callCount = new AtomicInteger(0)
        service.initSemaphores()
        service.derivativeLoaderExecutor = derivativeExecutor
        service.derivativeLoadTimeoutSeconds = 1
        service.init()
        service.disableCache = false
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-timeout', '') >> {
                if (callCount.incrementAndGet() == 1) {
                    firstLoadStarted.countDown()
                    allowFirstLoadToFinish.await(5, TimeUnit.SECONDS)
                }
                new ImageInfo(exists: true, imageIdentifier: 'img-timeout', contentType: 'image/jpeg', extension: 'jpg', inputStreamSupplier: { r -> new ByteArrayInputStream(new byte[0]) })
            }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations

        when:
        service.thumbnailImageInfo('img-timeout', '', false)

        then:
        firstLoadStarted.await(1, TimeUnit.SECONDS)
        thrown(ImageStoreService.DerivativeLoadTimeout)

        when:
        allowFirstLoadToFinish.countDown()
        def retried = service.thumbnailImageInfo('img-timeout', '', false)

        then:
        retried.exists
        callCount.get() == 2

        cleanup:
        allowFirstLoadToFinish.countDown()
        derivativeExecutor.shutdownNow()
    }

    def "thumbnailImageInfo propagates a saturated thumbnail generator"() {
        given:
        service.thumbnailConcurrencyLevel = 1
        service.thumbnailConcurrencyTimeout = 0
        service.initSemaphores()
        service.init()
        service.disableCache = false
        service.@thumbnailSemaphore.acquire()
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-saturated', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-saturated')
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations

        when:
        service.thumbnailImageInfo('img-saturated', '', false)

        then:
        thrown(ImageStoreService.GenerateDerivativeTimeout)

        cleanup:
        service.@thumbnailSemaphore.release()
    }

    def "thumbnailImageInfo reports a rejected derivative loader"() {
        given:
        service.initSemaphores()
        service.init()

        when:
        service.awaitDerivativeLoad(service.thumbnailCache, Pair.of('img-rejected', ''),
                { key, executor -> CompletableFuture.failedFuture(new RejectedExecutionException('queue full')) } as BiFunction)

        then:
        thrown(ImageStoreService.DerivativeLoadRejected)
    }

}
