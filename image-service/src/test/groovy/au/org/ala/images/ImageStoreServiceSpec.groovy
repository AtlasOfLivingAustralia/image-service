package au.org.ala.images

import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.google.common.io.ByteSource
import com.google.common.io.Resources
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.commons.lang3.tuple.Pair
import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import org.grails.plugins.testing.GrailsMockMultipartFile
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
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

    def "derivative-path thumbnail generation no longer acquires the thumbnail semaphore"() {
        given: 'the only semaphore permit is held, which previously blocked the derivative path'
        service.thumbnailConcurrencyLevel = 1
        service.thumbnailConcurrencyTimeout = 0
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        service.grailsApplication.config.imageservice.thumbnail.size = 200
        service.@thumbnailSemaphore.acquire()
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-unguarded', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-unguarded')
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations
        service.storageLocationService.isActuallyAnImage('img-unguarded') >> true
        def thumbDefCount = new AtomicInteger(0)
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> { ByteSource src, sinkFactory, defs ->
                thumbDefCount.set(defs.size())
                [new au.org.ala.images.thumb.ThumbnailingResult(200, 200, false, 'thumbnail')]
            }
        }

        when: 'a derivative load runs while the semaphore is fully held'
        def info = service.thumbnailImageInfo('img-unguarded', '', false)

        then: 'generation still ran (no GenerateDerivativeTimeout) because the pool is the sole limiter'
        thumbDefCount.get() == 1
        noExceptionThrown()

        cleanup:
        service.@thumbnailSemaphore.release()
    }

    def "concurrent derivative generations are not semaphore-limited"() {
        given: 'semaphore of 1 but a 2-thread derivative pool generating two distinct thumbnails'
        service.thumbnailConcurrencyLevel = 1
        service.thumbnailConcurrencyTimeout = 0
        service.initSemaphores()
        ThreadPoolExecutor derivativeExecutor = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>())
        derivativeExecutor.prestartAllCoreThreads() // fixed pools lazily create workers; force both so the two loads truly overlap
        service.derivativeLoaderExecutor = derivativeExecutor
        service.init()
        service.disableCache = false // go through the derivative pool (cache on); distinct keys => no coalescing
        def inside = new AtomicInteger(0)
        def maxInside = new AtomicInteger(0)
        def entered = new CountDownLatch(2)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo(_, _) >> { String id, String t -> new ImageInfo(exists: false, imageIdentifier: id) }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations
        // Hand-written fake: Spock mocks/stubs serialize concurrent invocations on a single mock,
        // which would falsely serialize the two generations and mask the semaphore removal.
        service.imageThumbnailer = new IImageThumbnailer() {
            @Override
            List<au.org.ala.images.thumb.ThumbnailingResult> generateThumbnails(ByteSource src, au.org.ala.images.util.ByteSinkFactory sinkFactory, List<au.org.ala.images.thumb.ThumbDefinition> defs) {
                def now = inside.incrementAndGet()
                maxInside.updateAndGet({ int prev -> Math.max(prev, now) })
                entered.countDown()
                entered.await(10, TimeUnit.SECONDS) // hold both generations open until both have entered
                inside.decrementAndGet()
                []
            }
        }
        service.grailsApplication.config.imageservice.thumbnail.size = 200
        ThreadPoolExecutor httpPool = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>())
        httpPool.prestartAllCoreThreads() // fixed pools lazily spawn workers on an unbounded queue; force both so the calls overlap

        when: 'two distinct thumbnails generate concurrently while the semaphore has only one permit'
        def f1 = httpPool.submit({ service.generateImageThumbnail('img-c1', operations, 'thumbnail') } as Callable)
        def f2 = httpPool.submit({ service.generateImageThumbnail('img-c2', operations, 'thumbnail') } as Callable)
        f1.get(15, TimeUnit.SECONDS)
        f2.get(15, TimeUnit.SECONDS)

        then: 'both ran inside generateThumbnailsImpl at the same time (pool, not semaphore, is the limiter)'
        maxInside.get() == 2
        noExceptionThrown()

        cleanup:
        httpPool.shutdownNow()
        derivativeExecutor.shutdownNow()
    }

    def "ingest-path generateImageThumbnails is still bounded by the thumbnail semaphore"() {
        given: 'the only semaphore permit is held, so the eager ingest path must time out'
        service.thumbnailConcurrencyLevel = 1
        service.thumbnailConcurrencyTimeout = 0
        service.initSemaphores()
        service.@thumbnailSemaphore.acquire()
        def image = Mock(Image) {
            getImageIdentifier() >> 'img-ingest'
            getStorageLocation() >> Mock(StorageLocation)
        }

        when:
        service.generateImageThumbnails(image)

        then:
        thrown(ImageStoreService.GenerateDerivativeTimeout)

        cleanup:
        service.@thumbnailSemaphore.release()
    }

    def "ingest-path generateImageThumbnails acquires and releases the semaphore around generation"() {
        given: 'a free semaphore; generation runs and must release the permit afterwards'
        service.thumbnailConcurrencyLevel = 1
        service.thumbnailConcurrencyTimeout = 5
        service.initSemaphores()
        service.grailsApplication.config.imageservice.thumbnail.size = 200
        def operations = Mock(StorageOperations)
        def image = Mock(Image) {
            getImageIdentifier() >> 'img-ingest-ok'
            getStorageLocation() >> Mock(StorageLocation) { asStandaloneStorageOperations() >> operations }
        }
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> []
        }

        when:
        service.generateImageThumbnails(image)

        then: 'the permit was released, so a subsequent acquire succeeds immediately'
        service.@thumbnailSemaphore.tryAcquire(0, TimeUnit.SECONDS)

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

    def "rejected derivative submission logs live executor state"() {
        given: 'a real ThreadPoolTaskExecutor whose queue and single thread are saturated'
        def taskExecutor = new ThreadPoolTaskExecutor()
        taskExecutor.corePoolSize = 1
        taskExecutor.maxPoolSize = 1
        taskExecutor.queueCapacity = 0 // no queue: the single busy thread forces an immediate rejection
        taskExecutor.threadNamePrefix = 'derivative-loader-test-'
        taskExecutor.initialize()
        service.derivativeLoaderExecutor = taskExecutor
        service.initSemaphores()
        service.init()
        def appender = attachListAppender()
        def blocker = new CountDownLatch(1)
        def started = new CountDownLatch(1)
        taskExecutor.execute({ started.countDown(); blocker.await(5, TimeUnit.SECONDS) } as Runnable)
        assert started.await(5, TimeUnit.SECONDS)

        when: 'a submission is rejected because the pool cannot accept more work'
        def future = service.submitDerivativeLoad({ key -> new ImageInfo(exists: true) }, 'img-state', taskExecutor)

        then: 'the future carries the rejection and a WARN with pool state was logged'
        future.isCompletedExceptionally()
        def warns = appender.list.findAll { it.level == Level.WARN }
        warns.any { it.formattedMessage.contains('poolSize=') && it.formattedMessage.contains('activeCount=') && it.formattedMessage.contains('queueSize=') && it.formattedMessage.contains('img-state') }

        cleanup:
            blocker.countDown()
        taskExecutor.shutdown()
        detachListAppender(appender)
    }

    def "thumbnail negative cache hit short-circuits generation"() {
        given: 'a thumbnail whose original was previously found permanently missing'
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        def generationAttempts = new AtomicInteger(0)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-neg', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-neg')
            originalInputStream('img-neg', null) >> {
                generationAttempts.incrementAndGet()
                throw new FileNotFoundException('NoSuchKey: img-neg original missing')
            }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations
        service.storageLocationService.isActuallyAnImage('img-neg') >> true
        // The default IImageThumbnailer mock returns null without opening the source; replace it
        // with one that reads the original bytes so the storage FileNotFoundException surfaces.
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> { ByteSource src, sf, defs -> src.openStream().withStream { it.bytes }; [] }
        }

        when: 'the first request triggers generation which fails with FileNotFoundException'
        def first = service.thumbnailImageInfo('img-neg', '', false)

        then: 'generation ran once and the miss is marked as should-not-exist'
        !first.exists
        generationAttempts.get() == 1
        service.thumbnailNegativeCache.getIfPresent(Pair.of('img-neg', ''))

        when: 'a subsequent request arrives'
        def second = service.thumbnailImageInfo('img-neg', '', false)

        then: 'it is served from the negative cache without re-running generation'
        !second.exists
        second.shouldExist
        second.contentType == 'image/jpeg'
        generationAttempts.get() == 1
    }

    def "thumbnail negative cache is populated only on FileNotFoundException and not on transient errors"() {
        given:
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-transient', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-transient')
            originalInputStream('img-transient', null) >> { throw new RuntimeException('S3 500 InternalError') }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations
        service.storageLocationService.isActuallyAnImage('img-transient') >> true
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> { ByteSource src, sf, defs -> src.openStream().withStream { it.bytes }; [] }
        }

        when:
        service.thumbnailImageInfo('img-transient', '', false)

        then: 'a transient failure does not populate the negative cache'
        !service.thumbnailNegativeCache.getIfPresent(Pair.of('img-transient', ''))
    }

    def "thumbnail negative cache entry expires after the configured TTL"() {
        given:
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.thumbnailNegativeCacheTtlSeconds = 1
        service.init()
        service.disableCache = false
        service.thumbnailNegativeCache.put(Pair.of('img-ttl', ''), Boolean.TRUE)

        expect:
        service.thumbnailNegativeCache.getIfPresent(Pair.of('img-ttl', ''))

        when:
        Thread.sleep(1500)

        then:
        !service.thumbnailNegativeCache.getIfPresent(Pair.of('img-ttl', ''))
    }

    def "refresh invalidates the negative cache entry before consulting it"() {
        given: 'a thumbnail negatively cached as permanently missing'
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        def generationAttempts = new AtomicInteger(0)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-refresh', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-refresh')
            originalInputStream('img-refresh', null) >> {
                generationAttempts.incrementAndGet()
                throw new FileNotFoundException('NoSuchKey')
            }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations
        service.storageLocationService.isActuallyAnImage('img-refresh') >> true
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> { ByteSource src, sf, defs -> src.openStream().withStream { it.bytes }; [] }
        }

        when: 'the first request populates the negative cache'
        service.thumbnailImageInfo('img-refresh', '', false)

        then:
        generationAttempts.get() == 1
        service.thumbnailNegativeCache.getIfPresent(Pair.of('img-refresh', ''))

        when: 'a refresh request arrives'
        service.thumbnailImageInfo('img-refresh', '', true)

        then: 'the negative entry was invalidated before consult, so generation re-ran'
        generationAttempts.get() == 2
    }

    def "disableCache bypasses both negative-cache consult and populate"() {
        given:
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = true
        def generationAttempts = new AtomicInteger(0)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-nocache', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-nocache')
            originalInputStream('img-nocache', null) >> {
                generationAttempts.incrementAndGet()
                throw new FileNotFoundException('NoSuchKey')
            }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations
        service.storageLocationService.isActuallyAnImage('img-nocache') >> true
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> { ByteSource src, sf, defs -> src.openStream().withStream { it.bytes }; [] }
        }

        when: 'two requests arrive while caching is disabled'
        service.thumbnailImageInfo('img-nocache', '', false)
        service.thumbnailImageInfo('img-nocache', '', false)

        then: 'the FNFE is never written to the negative cache, so generation runs every time'
        generationAttempts.get() == 2
        !service.thumbnailNegativeCache.getIfPresent(Pair.of('img-nocache', ''))
    }

    def "successful generation invalidates the negative cache entry"() {
        given: 'a thumbnail negatively cached then fixed in storage so generation now succeeds'
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        service.grailsApplication.config.imageservice.thumbnail.size = 200
        def key = Pair.of('img-recover', '')
        service.thumbnailNegativeCache.put(key, Boolean.TRUE)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-recover', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-recover')
        }
        def thumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> [new au.org.ala.images.thumb.ThumbnailingResult(200, 200, false, 'thumbnail')]
        }
        service.imageThumbnailer = thumbnailer

        when: 'the cache loader runs and generation succeeds'
        def loader = service.&ensureThumbnailExistsCacheLoader.curry(null).curry(operations)
        loader.call(key)

        then: 'the loader cleared the negative-cache entry on success'
        !service.thumbnailNegativeCache.getIfPresent(key)
    }

    def "negative cache warns exactly once on the populating failure and is silent on hits"() {
        given: 'a thumbnail whose original is permanently missing from storage'
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        def generationAttempts = new AtomicInteger(0)
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-warn', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-warn')
            originalInputStream('img-warn', null) >> {
                generationAttempts.incrementAndGet()
                throw new FileNotFoundException('NoSuchKey: img-warn original missing')
            }
        }
        service.storageLocationService.getStorageOperationsWithoutDbLookup() >> operations
        service.storageLocationService.isActuallyAnImage('img-warn') >> true
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> { ByteSource src, sf, defs -> src.openStream().withStream { it.bytes }; [] }
        }
        def appender = attachListAppender()

        when: 'the first request runs generation, fails with FileNotFoundException and populates the negative cache'
        def first = service.thumbnailImageInfo('img-warn', '', false)

        then: 'exactly one WARN was emitted for the populating failure'
        !first.exists
        generationAttempts.get() == 1
        appender.list.count { it.level == Level.WARN } == 1

        when: 'two subsequent requests are served from the negative cache'
        appender.list.clear()
        service.thumbnailImageInfo('img-warn', '', false)
        service.thumbnailImageInfo('img-warn', '', false)

        then: 'no WARN is logged on the negative-cache hits and generation did not re-run'
        generationAttempts.get() == 1
        appender.list.count { it.level == Level.WARN } == 0

        cleanup:
        detachListAppender(appender)
    }

    def "zero thumbnails for non-image content logs at debug not warn"() {
        given: 'an audio/document record whose generation produces no thumbnails'
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-audio', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-audio')
        }
        // not an image -> expected, no thumbnails produced
        service.storageLocationService.isActuallyAnImage('img-audio') >> false
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> []
        }
        def appender = attachListAppender()
        def loader = service.&ensureThumbnailExistsCacheLoader.curry(null).curry(operations)

        when: 'the loader runs generation which returns zero thumbnails'
        loader.call(Pair.of('img-audio', ''))

        then: 'no WARN is emitted for the expected non-image case (a DEBUG/INFO line is fine)'
        appender.list.count { it.level == Level.WARN } == 0
        appender.list.count { it.level == Level.ERROR } == 0

        cleanup:
        detachListAppender(appender)
    }

    def "zero thumbnails for an image logs a warn without the misleading reported success phrasing"() {
        given: 'a genuine image whose generation produces zero thumbnails'
        service.initSemaphores()
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor
        service.init()
        service.disableCache = false
        def operations = Stub(StorageOperations) {
            thumbnailImageInfo('img-zero', '') >> new ImageInfo(exists: false, imageIdentifier: 'img-zero')
        }
        // genuinely an image -> a real problem
        service.storageLocationService.isActuallyAnImage('img-zero') >> true
        service.imageThumbnailer = Stub(IImageThumbnailer) {
            generateThumbnails(_, _, _) >> []
        }
        def appender = attachListAppender()
        def loader = service.&ensureThumbnailExistsCacheLoader.curry(null).curry(operations)

        when: 'the loader runs generation which returns zero thumbnails'
        loader.call(Pair.of('img-zero', ''))

        then: 'a WARN is emitted and it does not use the misleading "reported success" wording'
        def warns = appender.list.findAll { it.level == Level.WARN }
        warns.size() == 1
        !warns[0].formattedMessage.contains('reported success')
        warns[0].formattedMessage.contains('no results')

        cleanup:
        detachListAppender(appender)
    }

    def "derivative loader executor gauges are registered against the meter registry"() {
        given: 'a real ThreadPoolTaskExecutor-backed derivative loader and a meter registry'
        def registry = new SimpleMeterRegistry()
        def taskExecutor = new ThreadPoolTaskExecutor()
        taskExecutor.corePoolSize = 1
        taskExecutor.maxPoolSize = 1
        taskExecutor.threadNamePrefix = 'derivative-loader-test-'
        taskExecutor.initialize()
        service.meterRegistry = registry
        service.derivativeLoaderExecutor = taskExecutor

        when: 'the service initialises'
        service.init()

        then: 'the queue depth, active count and completed task count gauges are registered'
        registry.get('imagestore.derivative.loader.queue.depth').gauge() != null
        registry.get('imagestore.derivative.loader.active').gauge() != null
        registry.get('imagestore.derivative.loader.completed').gauge() != null

        cleanup:
        taskExecutor.shutdown()
    }

    def "derivative loader metric registration is a no-op when there is no meter registry"() {
        given: 'no meter registry present'
        service.meterRegistry = null
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor

        when: 'the service initialises'
        service.init()

        then: 'registration is skipped without throwing'
        notThrown(Exception)
    }

    def "derivative loader metric registration tolerates a non-thread-pool executor"() {
        given: 'a meter registry but a non ThreadPoolTaskExecutor derivative loader (eg inline test executor)'
        def registry = new SimpleMeterRegistry()
        service.meterRegistry = registry
        service.derivativeLoaderExecutor = { Runnable task -> task.run() } as Executor

        when: 'the service initialises'
        service.init()

        then: 'no gauges are registered but initialisation does not fail'
        registry.find('imagestore.derivative.loader.queue.depth').gauge() == null
        notThrown(Exception)
    }

    private ListAppender<ILoggingEvent> attachListAppender() {
        Logger serviceLogger = (Logger) LoggerFactory.getLogger(ImageStoreService)
        def appender = new ListAppender<ILoggingEvent>()
        appender.start()
        serviceLogger.addAppender(appender)
        return appender
    }

    private void detachListAppender(ListAppender<ILoggingEvent> appender) {
        ((Logger) LoggerFactory.getLogger(ImageStoreService)).detachAppender(appender)
        appender?.stop()
    }

}
