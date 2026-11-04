package au.org.ala.images

import au.org.ala.images.metrics.MetricsSupport
import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.tiling.DefaultZoomFactorStrategy
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.ImageTilerResults
import au.org.ala.images.tiling.OnDemandImageTiler
import au.org.ala.images.tiling.TileGenerationResult
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.util.ImageReaderUtils
import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.Cache
import com.google.common.io.Files as GFiles
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.io.ByteSource
import grails.gorm.transactions.NotTransactional
import grails.web.mapping.LinkGenerator
import groovy.transform.Immutable
import groovy.transform.NamedVariant
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.Gauge
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.io.inputstream.ZipInputStream
import net.lingala.zip4j.model.FileHeader
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.tuple.Pair
import org.apache.tika.Tika
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.web.multipart.MultipartFile

import javax.annotation.PostConstruct
import javax.imageio.IIOException
import javax.imageio.ImageIO
import javax.imageio.ImageReadParam
import javax.imageio.ImageReader
import java.awt.Color
import java.awt.Rectangle
import java.awt.image.BufferedImage
import java.nio.file.Files

import org.grails.orm.hibernate.cfg.GrailsHibernateUtil
import software.amazon.awssdk.awscore.exception.AwsServiceException

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.ExecutionException
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.function.BiFunction
import java.util.function.Consumer

@Slf4j
class ImageStoreService implements MetricsSupport {

    public static final int TILE_SIZE = 256

    // Upper bound for the original-image ingest retry backoff; exponential delays are capped here.
    private static final long INGEST_RETRY_MAX_DELAY_MS = 10_000L

    def grailsApplication
    def auditService
    LinkGenerator grailsLinkGenerator
    StorageLocationService storageLocationService
    ImageOptimisationService imageOptimisationService

    @Autowired @Qualifier('imageThumbnailer')
    IImageThumbnailer imageThumbnailer
    @Autowired @Qualifier('imageTiler')
    IImageTiler imageTiler
    @Autowired @Qualifier('onDemandImageTiler')
    IOnDemandImageTiler onDemandImageTiler

    @Value('${placeholder.sound.thumbnail}')
    Resource audioThumbnail

    @Value('${placeholder.sound.large}')
    Resource audioLargeThumbnail

    @Value('${placeholder.document.thumbnail}')
    Resource documentThumbnail

    @Value('${placeholder.document.large}')
    Resource documentLargeThumbnail

    @Value('${image.store.lookup.cache.thumbnailsConfig:maximumSize=10000}')
    String thumbnailLookupCacheConfig = 'maximumSize=10000'

    @Value('${image.store.lookup.cache.tilesConfig:maximumSize=10000}')
    String tileLookupCacheConfig = 'maximumSize=10000'

    @Value('${image.store.lookup.cache.originalConfig:maximumSize=10000}')
    String originalLookupCacheConfig = 'maximumSize=10000'


    @Value('${thumbnail.concurrency.level:-1}')
    int thumbnailConcurrencyLevel = -1

    @Value('${thumbnail.concurrency.timeout:30}')
    int thumbnailConcurrencyTimeout = 30

    @Value('${tiling.concurrency.level:2}')
    int tileConcurrencyLevel = 2

    @Value('${tiling.concurrency.timeout:30}')
    int tileConcurrencyTimeout = 30

    @Value('${derivative.loader.timeoutSeconds:30}')
    int derivativeLoadTimeoutSeconds = 30

    @Autowired @Qualifier('derivativeLoaderExecutor')
    Executor derivativeLoaderExecutor

    @Value('${images.disableCache:false}')
    boolean disableCache = false

    @Value('${image.store.thumbnail.negativeCache.ttlSeconds:60}')
    int thumbnailNegativeCacheTtlSeconds = 60


    @Value('${tiling.onDemand.enabled:false}')
    boolean onDemandTilingEnabled = false

    private Semaphore thumbnailSemaphore
    private Semaphore tilingSemaphore

    @PostConstruct
    void initSemaphores() {
        if (thumbnailConcurrencyLevel > 0) {
            thumbnailSemaphore = new Semaphore(thumbnailConcurrencyLevel)
        } else {
            thumbnailSemaphore = new Semaphore(Runtime.getRuntime().availableProcessors())
        }
        tilingSemaphore = new Semaphore(tileConcurrencyLevel)
    }


    AsyncCache<Pair<String, String>, ImageInfo> thumbnailCache
    AsyncCache<Pair<String, Point>, ImageInfo> tileCache
    Cache<String, ImageInfo> originalCache
    // Thumbnails whose original is missing from storage (FileNotFoundException); short TTL so a storage fix recovers.
    Cache<Pair<String, String>, Boolean> thumbnailNegativeCache

    @PostConstruct
    @NotTransactional
    def init() {
        thumbnailCache = Caffeine.from(thumbnailLookupCacheConfig).buildAsync()
        tileCache = Caffeine.from(tileLookupCacheConfig).buildAsync()
        originalCache = Caffeine.from(originalLookupCacheConfig).build()
        thumbnailNegativeCache = Caffeine.newBuilder().expireAfterWrite(thumbnailNegativeCacheTtlSeconds, TimeUnit.SECONDS).build()
        registerDerivativeLoaderMetrics()
    }

    /**
     * Exposes the derivative-loader thread pool's queue depth, active thread count and completed
     * task count as Micrometer gauges so operators can see loader saturation/backlog. The gauges
     * re-read the live {@link ThreadPoolExecutor} on every scrape, so they cost nothing between
     * observations. No-op when no MeterRegistry is present or the executor is not a Spring
     * {@link ThreadPoolTaskExecutor} (eg the inline test executor).
     */
    private void registerDerivativeLoaderMetrics() {
        if (!meterRegistry) {
            log.debug('No MeterRegistry available; derivative loader executor metrics will not be recorded')
            return
        }
        ThreadPoolExecutor derivativeLoaderPool = unwrapDerivativeLoaderThreadPool()
        if (!derivativeLoaderPool) {
            log.debug('derivativeLoaderExecutor is not a ThreadPoolTaskExecutor ({}); executor metrics will not be recorded', derivativeLoaderExecutor?.class?.name)
            return
        }
        Gauge.builder('imagestore.derivative.loader.queue.depth', derivativeLoaderPool, { pool -> pool.queue.size() } as java.util.function.ToDoubleFunction<ThreadPoolExecutor>)
                .description('Derivative loader executor queued (waiting) task count')
                .register(meterRegistry)
        Gauge.builder('imagestore.derivative.loader.active', derivativeLoaderPool, { pool -> pool.activeCount } as java.util.function.ToDoubleFunction<ThreadPoolExecutor>)
                .description('Derivative loader executor active (running) thread count')
                .register(meterRegistry)
        Gauge.builder('imagestore.derivative.loader.completed', derivativeLoaderPool, { pool -> pool.completedTaskCount } as java.util.function.ToDoubleFunction<ThreadPoolExecutor>)
                .description('Derivative loader executor completed task count')
                .register(meterRegistry)
    }

    private ThreadPoolExecutor unwrapDerivativeLoaderThreadPool() {
        if (!(derivativeLoaderExecutor instanceof ThreadPoolTaskExecutor)) {
            return null
        }
        try {
            return ((ThreadPoolTaskExecutor) derivativeLoaderExecutor).threadPoolExecutor
        } catch (IllegalStateException executorNotInitialised) {
            log.debug('derivativeLoaderExecutor ThreadPoolExecutor not yet initialised: {}', executorNotInitialised.message)
            return null
        }
    }

    @NotTransactional
    def clearThumbnailLookupCache() {
        thumbnailCache.invalidateAll()
        thumbnailNegativeCache.invalidateAll()
    }

    @NotTransactional
    def clearTileLookupCache() {
        tileCache.invalidateAll()
    }
    
    @NotTransactional
    def clearOriginalLookupCache() {
        originalCache.invalidateAll()
    }

    @NotTransactional
    def clearTileLookupCacheForImage(String imageIdentifier) {
        // Find all cache entries for this image identifier and invalidate them
        tileCache.asMap().keySet().findAll { it.left == imageIdentifier }.each { key ->
            tileCache.synchronous().invalidate(key)
        }
    }

    @NotTransactional
    def clearTilesForImage(String imageIdentifier) {

        StorageOperations operations

        Image.withTransaction {
            def image = Image.findByImageIdentifier(imageIdentifier, [ cache: true ])
            if (image) {
                operations = GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).asStandaloneStorageOperations()
                image.save()
            } else {
                log.warn("No image found with identifier ${imageIdentifier} to clear tiles for")
            }
        }
        if (operations) {
            operations.clearTilesForImage(imageIdentifier)
        }
        clearTileLookupCacheForImage(imageIdentifier)
        auditService.log(imageIdentifier, "Tiles cleared", "N/A")
    }

    @NotTransactional
    ImageDescriptor storeImage(ByteSource imageBytes, StorageOperations operations, String contentType, String originalFilename, String contentDisposition = null) {
        return recordTime('imagestore.store', 'Time to store image to storage', [contentType: contentType?.split('/')[0] ?: 'unknown']) {
            def uuid = UUID.randomUUID().toString()
            if (log.isTraceEnabled()) {
                log.trace('Storing image {} with content type {}, original filename {}, content disposition {} to {}', uuid, contentType, originalFilename, contentDisposition, operations)
            }
            def imgDesc = new ImageDescriptor(imageIdentifier: uuid)
            def optimiseResult = optimiseImage(imageBytes: imageBytes, operations: operations,
                    contentType: contentType, originalFilename:  originalFilename, contentDisposition: contentDisposition,
                    uuid: uuid)
            def finalByteSource = optimiseResult.byteSource
            def finalContentType = optimiseResult.contentType
            def cleanup = optimiseResult.cleanup
            def extractedMetadata = optimiseResult.extractedMetadata

            // Store extracted metadata in descriptor for use by caller
            if (extractedMetadata) {
                imgDesc.extractedMetadata = extractedMetadata
            }

            try {
                storeOriginalWithRetry(operations, uuid, finalByteSource, finalContentType, contentDisposition)
                def filename = ImageUtils.getFilename(originalFilename)
                if (finalContentType?.toLowerCase()?.startsWith('image')) {
                    if (log.isTraceEnabled()) {
                        log.trace('Getting image dimensions for image {}, filename {}, content type {}', uuid, filename, finalContentType)
                    }
                    def dimensions = ImageReaderUtils.getImageDimensions(finalByteSource, filename)
                    if (dimensions) {
                        imgDesc.height = dimensions.height
                        imgDesc.width = dimensions.width
                        // precalculate the number of zoom levels
                        def pyramid = new DefaultZoomFactorStrategy(TILE_SIZE).getZoomFactors(dimensions.width, dimensions.height)
                        imgDesc.zoomLevels = pyramid.length
                    }
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace('Skipping calculating dimensions for image {}, filename {}, content type {}', uuid, filename, contentType)
                    }
                }

                incrementCounter('imagestore.store.count', 'Images stored', [contentType: finalContentType?.split('/')[0] ?: 'unknown'])
                return imgDesc
            } finally {
                // clean up any optimiser files
                cleanup()
            }
        }
    }

    /**
     * Stores the original image bytes with a bounded retry so that a single transient failure
     * (for example the S3 blocking-body subscribe timeout) does not permanently lose the original.
     * Each attempt opens a fresh stream from the re-openable {@code finalByteSource}; the stream
     * handed to {@code operations.store} is never reused across attempts. Synchronous and runs on
     * the calling background ingest thread.
     */
    private void storeOriginalWithRetry(StorageOperations operations, String uuid, ByteSource finalByteSource, String finalContentType, String contentDisposition) {
        int maxAttempts = Math.max(1, grailsApplication.config.getProperty('images.storage.ingest.retry.max.attempts', Integer, 3))
        long baseDelayMs = grailsApplication.config.getProperty('images.storage.ingest.retry.base.delay.ms', Long, 500L)

        for (int attempt = 1; ; attempt++) {
            try {
                operations.store(uuid, finalByteSource.openStream(), finalContentType, contentDisposition, finalByteSource.sizeIfKnown().orNull())
                return
            } catch (Exception exception) {
                if (!isIngestUploadRetryable(exception) || attempt >= maxAttempts) {
                    throw exception
                }
                long backoffMs = ingestRetryBackoffMs(attempt, baseDelayMs)
                log.warn('Transient failure storing original image {} (attempt {}/{}): {}. Retrying in {} ms.', uuid, attempt, maxAttempts, exception.message, backoffMs)
                try {
                    Thread.sleep(backoffMs)
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt()
                    exception.addSuppressed(interrupted)
                    throw exception
                }
            }
        }
    }

    /**
     * Classifies whether an original-image ingest failure is transient and worth retrying.
     * Retryable: the blocking-body subscribe-timeout {@link IllegalStateException}, IOExceptions,
     * and S3 throttling / 5xx (or otherwise SDK-retryable) service errors. Deterministic 4xx
     * errors such as NoSuchBucket or AccessDenied are NOT retried; they fail fast.
     */
    private static boolean isIngestUploadRetryable(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.cause) {
            if (isSubscribeTimeout(cause) || cause instanceof IOException) {
                return true
            }
            if (cause instanceof AwsServiceException) {
                def serviceException = (AwsServiceException) cause
                if (serviceException.isThrottlingException() || serviceException.isRetryableException()) {
                    return true
                }
                int statusCode = serviceException.statusCode()
                if (statusCode >= 500) {
                    return true
                }
                if (statusCode >= 400 && statusCode < 500) {
                    return false
                }
            }
        }
        return false
    }

    private static boolean isSubscribeTimeout(Throwable throwable) {
        return throwable instanceof IllegalStateException &&
                throwable.message != null &&
                throwable.message ==~ /(?s).*was not made within .* seconds.*/
    }

    private static long ingestRetryBackoffMs(int attempt, long baseDelayMs) {
        long exponential = baseDelayMs * (1L << (attempt - 1))
        long capped = Math.min(exponential, INGEST_RETRY_MAX_DELAY_MS)
        long half = capped.intdiv(2L)
        long jitter = ThreadLocalRandom.current().nextLong(half + 1L)
        return half + jitter
    }

    @NamedVariant
    private Map optimiseImage(ByteSource imageBytes, StorageOperations operations, String contentType, String originalFilename, String contentDisposition, String uuid) {
        def optimiseEnabled = grailsApplication.config.getProperty('images.optimisation.enabled', Boolean, false)
        def finalByteSource = imageBytes
        def finalContentType = contentType
        def cleanup = { -> }
        def extractedMetadata = null
        // Outside if scope for cleanup
        File tmpDir
        File toStoreFile
        if (optimiseEnabled) {

            // Always realize ByteSource to temp file so we can run optimisation pre-storage
            tmpDir = Files.createTempDirectory("imgstore-$uuid").toFile()
//                tmpDir = new File(System.getProperty('java.io.tmpdir'), 'imgstore-' + uuid)
            if (!tmpDir.mkdirs() && !tmpDir.exists()) {
                log.error("Couldn't make $tmpDir, expect further complications...")
            }
            String ext = ImageOptimisationService.inferExtensionForUrlOrContentType(originalFilename, contentType)
            File realized = new File(tmpDir, 'original.' + ext)
            realized.withOutputStream { os -> imageBytes.copyTo(os) }

            toStoreFile = realized

            if ((contentType?.toLowerCase()?.startsWith('image'))) {
                // Extract metadata BEFORE optimisation to preserve EXIF data that may be stripped
                try {
                    realized.withInputStream { InputStream is ->
                        extractedMetadata = ImageService.getImageMetadataFromBytes(is, originalFilename)
                    }
                    log.debug('Extracted metadata from original image {} before optimisation: {} keys', uuid, extractedMetadata?.size() ?: 0)
                } catch (Throwable t) {
                    log.warn('Metadata extraction before optimisation failed for {}: {}', uuid, t.message)
                }


                boolean keepBackup = grailsApplication.config.getProperty('images.optimisation.keepOriginalBackup', Boolean, false)
                // Optionally store pre-optimised original as a backup alongside
                if (keepBackup) {
                    try {
                        String backupName = 'original_backup.' + ext
                        operations.storeAnywhere(uuid, new BufferedInputStream(new FileInputStream(realized)), backupName, contentType, contentDisposition, realized.length())
                    } catch (Throwable t) {
                        log.warn('Failed to store original backup for {}: {}', uuid, t.message)
                    }
                }

                try {
                    def optResult = imageOptimisationService.optimise(realized, contentType)
                    if (optResult) {
                        toStoreFile = optResult.optimisedFile ?: realized
                        if (optResult.outputContentType) {
                            finalContentType = optResult.outputContentType
                        }
                    }
                } catch (Throwable t) {
                    log.warn('Image optimisation failed for {}: {}', uuid, t.message, t)
                }
            }

            finalByteSource = GFiles.asByteSource(toStoreFile)

            toStoreFile.deleteOnExit()
            tmpDir.deleteOnExit()
            cleanup = { ->
                try {
                    if (toStoreFile && toStoreFile.parentFile && toStoreFile.parentFile != tmpDir) {
                        FileUtils.deleteQuietly(toStoreFile.parentFile)
                    }
                    if (tmpDir) {
                        FileUtils.deleteQuietly(tmpDir)
                    }
                } catch (Throwable ignore) {}
            }
        }

        return [
                byteSource: finalByteSource,
                contentType: finalContentType,
                cleanup: cleanup,
                extractedMetadata: extractedMetadata
        ]

    }

    // Replacements for Image.x() methods that call into the storage operations

    byte[] retrieveImageBytes(Image image) {
        return storageLocationService.getStorageOperationsForImage(image).retrieve(image.imageIdentifier)
    }

//    @Transactional(readOnly = true)
    InputStream retrieveImageInputStream(String imageIdentifier) {
        return storageLocationService.getStorageOperationsForImage(imageIdentifier).originalInputStream(imageIdentifier, null)
    }

    InputStream retrieveImageInputStream(Image image) {
        return storageLocationService.getStorageOperationsForImage(image).originalInputStream(image.imageIdentifier, null)
    }

    boolean isImageStored(Image image) {
        storageLocationService.getStorageOperationsForImage(image).stored(image.imageIdentifier)
    }

    long consumedSpace(Image image) {
        return storageLocationService.getStorageOperationsForImage(image).consumedSpace(image.imageIdentifier)
    }

    boolean deleteStored(Image image) {
        storageLocationService.getStorageOperationsForImage(image).deleteStored(image.imageIdentifier)
    }

    void storeTileZipInputStream(Image image, String fileName, String contentType, long length, ZipInputStream inputStream) {
        if (fileName.contains("..") || fileName.startsWith("/") || fileName.startsWith("\\")) {
            throw new IllegalArgumentException("Potentially malicious filename: ${fileName}")
        }
        def ops = storageLocationService.getStorageOperationsForImage(image)
        ops.storeTileZipInputStream(image.imageIdentifier, fileName, contentType, length, inputStream)
    }


    void migrateTo(Image image, StorageLocation destination) {
        storageLocationService.getStorageOperationsForImage(image).migrateTo(image.imageIdentifier, image.mimeType, destination)
    }

    // End replacements for Image.x() methods

    Map retrieveImageRectangle(Image parentImage, int x, int y, int width, int height) {
        return recordTime('imagestore.retrieve.rectangle', 'Time to retrieve image rectangle', [width: width.toString(), height: height.toString()]) {
            def results = [bytes: null, contentType: ""]

            if (parentImage) {
                def imageBytes = retrieveImageBytes(parentImage)
                ImageReaderUtils.withImageReader(ByteSource.wrap(imageBytes), { reader ->
                    Rectangle stripRect = new Rectangle(x, y, width, height);
                    ImageReadParam params = reader.getDefaultReadParam();
                    params.setSourceRegion(stripRect);
                    params.setSourceSubsampling(1, 1, 0, 0);
                    // This may fail if there is not enough heap!
                    BufferedImage subimage = reader.read(0, params);
                    def bos = new ByteArrayOutputStream()
                    if (!ImageIO.write(subimage, "PNG", bos)) {
                        log.debug("Could not create subimage in PNG format. Giving up")
                        incrementCounter('imagestore.retrieve.rectangle.failure', 'Failed rectangle retrievals')
                        return
                    } else {
                        results.contentType = "image/png"
                    }
                    results.bytes = bos.toByteArray()
                    bos.close()
                    incrementCounter('imagestore.retrieve.rectangle.success', 'Successful rectangle retrievals')
                } as Consumer<ImageReader>)
            }

            return results
        }
    }

    Map getAllUrls(String imageIdentifier) {
        def results = [:]
        // TODO use named URLS?

        results.imageUrl = getImageUrl(imageIdentifier)
        results.thumbUrl = getImageThumbUrl(imageIdentifier)
        results.largeThumbUrl = getImageThumbLargeUrl(imageIdentifier)
        results.squareThumbUrl = getThumbUrlByName(imageIdentifier, 'square')
        results.tilesUrlPattern = getImageTilesUrlPattern(imageIdentifier)

        return results
    }

    String getImageUrl(String imageIdentifier) {
        def a = imageIdentifier[-1]
        def b = imageIdentifier[-2]
        def c = imageIdentifier[-3]
        def d = imageIdentifier[-4]
        return grailsLinkGenerator.link(absolute: true, controller: 'image', action: 'getOriginalFile', id: imageIdentifier, params: [a: a, b: b, c: c, d: d])
    }

    String getImageThumbUrl(String imageIdentifier) {
        def a = imageIdentifier[-1]
        def b = imageIdentifier[-2]
        def c = imageIdentifier[-3]
        def d = imageIdentifier[-4]
        return grailsLinkGenerator.link(absolute: true, controller: 'image', action: 'proxyImageThumbnail', id: imageIdentifier, params: [a: a, b: b, c: c, d: d])
    }

    String getImageThumbLargeUrl(String imageIdentifier) {
        getThumbUrlByName(imageIdentifier, 'large')
    }

    String getImageThumbXLargeUrl(String imageIdentifier) {
        getThumbUrlByName(imageIdentifier, 'xlarge')
    }

    String getImageThumbCentreCropLargeUrl(String imageIdentifier) {
        getThumbUrlByName(imageIdentifier, 'centre_crop_large')
    }

    String getImageThumbCentreCropUrl(String imageIdentifier) {
        getThumbUrlByName(imageIdentifier, 'centre_crop')
    }

    String getThumbUrlByName(String imageIdentifier, String name) {
        if (name == 'thumbnail') {
            return getImageThumbUrl(imageIdentifier)
        }
        def a = imageIdentifier[-1]
        def b = imageIdentifier[-2]
        def c = imageIdentifier[-3]
        def d = imageIdentifier[-4]
        def type = name.startsWith('thumbnail_') ? name.substring('thumbnail_'.length()) : name
        return grailsLinkGenerator.link(absolute: true, controller: 'image', action: 'proxyImageThumbnailType', id: imageIdentifier, params: [thumbnailType: type, a: a, b: b, c: c, d: d])
    }

    String getImageSquareThumbUrl(String imageIdentifier, String backgroundColor) {
        def type
        if (backgroundColor) {
            type = "thumbnail_square_${backgroundColor}"
        } else {
            type = "thumbnail_square"
        }
        return getThumbUrlByName(imageIdentifier, type)
    }

    String getImageTilesUrlPattern(String imageIdentifier) {
        def a = imageIdentifier[-1]
        def b = imageIdentifier[-2]
        def c = imageIdentifier[-3]
        def d = imageIdentifier[-4]
        def pattern = grailsLinkGenerator.link(absolute: true, controller: 'image', action: 'proxyImageTile', id: imageIdentifier, params: [x: '{x}', y: '{y}', z: '{z}', a: a, b: b, c: c, d: d])
        // XXX hack this result to remove the URL encoded placeholders
        return pattern.replace('%7Bz%7D', '{z}').replace('%7Bx%7D', '{x}').replace('%7By%7D', '{y}')
    }

    List<ThumbnailingResult> generateAudioThumbnails(Image image) {
        return []
    }

    List<ThumbnailingResult> generateDocumentThumbnails(Image image) {
        return []
    }

    /**
     * Create a number of thumbnail artifacts for an image, one that preserves the aspect ratio of the original image, another drawing a scale image on a transparent
     * square with a constrained maximum dimension of config item "imageservice.thumbnail.size", and a series of square jpeg thumbs with different coloured backgrounds
     * (jpeg thumbs are much smaller, and load much faster than PNG).
     *
     * The first thumbnail (preserved aspect ratio) is of type JPG to conserve disk space, whilst the square thumb is PNG as JPG does not support alpha transparency
     * @param imageIdentifier The id of the image to thumb
     */
    List<ThumbnailingResult> generateImageThumbnails(Image image) {
//        def imageBytes = image.retrieve()
        def byteSource = new ByteSource() {
            @Override
            InputStream openStream() throws IOException {
                return ImageStoreService.this.retrieveImageInputStream(image)
            }
        }
        // Ingest/eager path only: bound thumbnail generation concurrency with the semaphore so a
        // burst of background-ingest tasks cannot stampede the CPU. The on-demand derivative path
        // deliberately does NOT use this semaphore — the derivativeLoaderExecutor pool is its sole
        // concurrency limiter (a second gate here could serialize the pool threads when storage is slow).
        if (!thumbnailSemaphore.tryAcquire(thumbnailConcurrencyTimeout, TimeUnit.SECONDS)) {
            incrementCounter('imagestore.thumbnail.timeout', 'Thumbnail generation timeouts', [type: 'all'])
            throw new GenerateDerivativeTimeout("Could not acquire thumbnail semaphore to thumb image ${image.imageIdentifier} in ${thumbnailConcurrencyTimeout}s")
        }
        try {
            return generateThumbnailsImpl(byteSource, image.imageIdentifier, image.storageLocation.asStandaloneStorageOperations())
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt()
            throw new IllegalStateException("Interrupted while thumbnailing image ${image.imageIdentifier}", ie)
        } finally {
            thumbnailSemaphore.release()
        }
    }

    @NotTransactional
    List<ThumbnailingResult> generateImageThumbnail(String imageIdentifier, StorageOperations operations, String type) {
        // TODO Maybe this byteSource should cache the result?
        def byteSource = new ByteSource() {
            @Override
            InputStream openStream() throws IOException {
                return operations.originalInputStream(imageIdentifier, null)
            }
        }
        return generateThumbnailsImpl(byteSource, imageIdentifier, operations, type)
    }

    private List<ThumbnailingResult> generateThumbnailsImpl(ByteSource byteSource, String imageIdentifier, StorageOperations operations, String type = null) {
        return recordTime('imagestore.thumbnail.generate', 'Time to generate thumbnails', [type: type ?: 'all', count: type == null ? '6' : '1']) {
            def ct = new CodeTimer("Generating ${type != null ? 1 : 6} thumbnails for image ${imageIdentifier}").tap { debug() }
            def t = imageThumbnailer
    //        def imageIdentifier = image.imageIdentifier
            int size = grailsApplication.config.getProperty('imageservice.thumbnail.size') as Integer
            List<ThumbDefinition> thumbDefs = new ArrayList<ThumbDefinition>(type == null ? 6 : 1)
            if ('thumbnail'.equalsIgnoreCase(type) || ''.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(new ThumbDefinition(size, false, null, "thumbnail"))
            }
            if ('thumbnail_square'.equalsIgnoreCase(type) || 'square'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(new ThumbDefinition(size, true, null, "thumbnail_square"))
            }
            if ('thumbnail_square_black'.equalsIgnoreCase(type) || 'square_black'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(new ThumbDefinition(size, true, Color.black, "thumbnail_square_black"))
            }
            if ('thumbnail_square_white'.equalsIgnoreCase(type) || 'square_white'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(new ThumbDefinition(size, true, Color.white, "thumbnail_square_white"))
            }
            if ('thumbnail_square_darkGray'.equalsIgnoreCase(type) || 'thumbnail_square_darkGrey'.equalsIgnoreCase(type) || 'square_darkGray'.equalsIgnoreCase(type) || 'square_darkGrey'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(new ThumbDefinition(size, true, Color.darkGray, "thumbnail_square_darkGray"))
            }
            if ('thumbnail_large'.equalsIgnoreCase(type) || 'large'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(new ThumbDefinition(650, false, null, "thumbnail_large"))
            }
            if ('thumbnail_xlarge'.equalsIgnoreCase(type) || 'xlarge'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(new ThumbDefinition(1024, false, null, "thumbnail_xlarge"))
            }
            if ('thumbnail_centre_crop'.equalsIgnoreCase(type) || 'centre_crop'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(ThumbDefinition.centreCrop(size, "thumbnail_centre_crop"))
            }
            if ('thumbnail_centre_crop_large'.equalsIgnoreCase(type) || 'centre_crop_large'.equalsIgnoreCase(type) || type == null) {
                thumbDefs.add(ThumbDefinition.centreCrop(650, "thumbnail_centre_crop_large"))
            }

            // No thumbnailSemaphore here: this runs on the derivativeLoaderExecutor pool (on-demand
            // path) which is the sole concurrency limiter. The eager ingest path guards separately in
            // generateImageThumbnails(Image).
            List<ThumbnailingResult> results
            try {
                results = t.generateThumbnails(
                        byteSource,
                        operations.thumbnailByteSinkFactory(imageIdentifier),
                        thumbDefs
                )
                incrementCounter('imagestore.thumbnail.success', 'Successful thumbnail generations', [type: type ?: 'all'])
            } catch (Exception e) {
                incrementCounter('imagestore.thumbnail.error', 'Thumbnail generation errors', [type: type ?: 'all', error: e.class.simpleName])
                throw e
            }

            auditService.log(imageIdentifier, "Thumbnails created", "N/A")
            ct.stop(true)
            return results
        }
    }

    void generateTMSTiles(String imageIdentifier) {
        def image = Image.findByImageIdentifier(imageIdentifier, [ cache: true ])
        generateTMSTiles(image)
    }


    ImageTilerResults generateTMSTiles(Image image, Integer z = null) {
        return generateTMSTiles(image.imageIdentifier, GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).asStandaloneStorageOperations(), image.zoomLevels, z)
    }

    @NotTransactional
    ImageTilerResults generateTMSTiles(String imageIdentifier, StorageOperations operations, int zoomLevels, Integer z = null) {
        return recordTime('imagestore.tile.generate', 'Time to generate TMS tiles', [level: z != null ? 'single' : 'all', z: z?.toString() ?: 'all']) {
            log.debug("Generating TMS compatible tiles for image ${imageIdentifier}, zoom level ${z}")
            def ct = new CodeTimer("Tiling image ${imageIdentifier}, z index: $z")

            def results
            if (tilingSemaphore.tryAcquire(tileConcurrencyTimeout, TimeUnit.SECONDS)) {
                try {
                    if (z != null) {
                        results = tileImageLevel(imageIdentifier, operations, z)
                    } else {
                        results = tileImage(imageIdentifier, operations)
                    }
                    if (results.success) {
                        incrementCounter('imagestore.tile.success', 'Successful tile generations', [level: z != null ? 'single' : 'all'])
                    } else {
                        incrementCounter('imagestore.tile.failure', 'Failed tile generations', [level: z != null ? 'single' : 'all'])
                    }
                } catch (Exception e) {
                    incrementCounter('imagestore.tile.error', 'Tile generation errors', [level: z != null ? 'single' : 'all', error: e.class.simpleName])
                    throw e
                } finally {
                    tilingSemaphore.release()
                }
            } else {
                incrementCounter('imagestore.tile.timeout', 'Tile generation timeouts', [level: z != null ? 'single' : 'all'])
                throw new GenerateDerivativeTimeout("Could not acquire tiling semaphore to tile image ${imageIdentifier} in ${tileConcurrencyTimeout}s")
            }

            if (results.success) {
                if (zoomLevels != results.zoomLevels) {
                    // only update the zoom levels if they have changed
                    // run the update concurrently so as not to block a tile http request
                    Image.async.task {
                        withTransaction {
                            def updates = Image.executeUpdate("update Image set zoomLevels = :zoomLevels where imageIdentifier = :imageIdentifier and zoomLevels != :zoomLevels", [zoomLevels: results.zoomLevels, imageIdentifier: imageIdentifier])
                            if (updates < 1) {
                                log.debug("Zoom levels were already ${results.zoomLevels} for ${imageIdentifier}")
                            }
                        }
                    }
                }
            } else {
                log.warn("Image tiling for $imageIdentifier with z = $z failed! Results zoomLevels: ${results.zoomLevels}")
            }
            auditService.log(imageIdentifier, "TMS tiles generated", "N/A")
            ct.stop(true)

            return results
        }
    }


    private ImageTilerResults tileImageLevel(String imageIdentifier, StorageOperations operations, int z) {
        def tiler = imageTiler
        return tiler.tileImage(
                operations.originalInputStream(imageIdentifier, null),
                new TilerSink.PathBasedTilerSink(operations.tilerByteSinkFactory(imageIdentifier)),
                z,
                z
        )
    }

    private ImageTilerResults tileImage(String imageIdentifier, StorageOperations operations) {
        def tiler = imageTiler
        return tiler.tileImage(
                operations.originalInputStream(imageIdentifier, null),
                new TilerSink.PathBasedTilerSink(operations.tilerByteSinkFactory(imageIdentifier))
        )
    }

    boolean storeTilesArchiveForImage(Image image, MultipartFile zipFile) {

        if (isImageStored(image)) {
            def stagingFile = Files.createTempFile('image-service', '.zip').toFile()
            stagingFile.deleteOnExit()

            // copy the zip file to the staging area
            zipFile.inputStream.withStream { stream ->
                FileUtils.copyInputStreamToFile(stream, stagingFile)
            }

            def szf = new ZipFile(stagingFile)
            def tika = new Tika()
            for (FileHeader fh : szf.getFileHeaders()) {
                if (fh.isDirectory()) continue
                String fileName = fh.fileName
                if (fileName.contains("..") || fileName.startsWith("/") || fileName.startsWith("\\")) {
                    log.warn("Skipping potentially malicious zip entry: ${fileName}")
                    continue
                }
                szf.getInputStream(fh).withStream { stream ->
                    def contentType = tika.detect(stream, fileName)
                    def length = fh.uncompressedSize
                    storeTileZipInputStream(image, fileName, contentType, length, szf.getInputStream(fh))
                }
            }

            // TODO: validate the extracted contents
            auditService.log(image.imageIdentifier, "Image tiles stored from zip file (outsourced job?)", "N/A")

            // Now clean up!
            FileUtils.deleteQuietly(stagingFile)
            return true
        }
        return false
    }

    long getRepositorySizeOnDisk() {
        // TODO replace with per repository size
        def fssls = FileSystemStorageLocation.list()
        return fssls.sum {
            def dir = new File(it.basePath)
            dir.exists() ? FileUtils.sizeOfDirectory(dir) : 0
        } ?: 0
    }

    /**
     * Migrate the given image from its current storageLocation to the given
     * StorageLocation.
     * @param image The image to migrate
     * @param sl The destination storage location
     */
    void migrateImage(Image image, StorageLocation sl) {
        recordTime('imagestore.migrate', 'Time to migrate image between storage locations', [from: image.storageLocation?.class?.simpleName ?: 'unknown', to: sl?.class?.simpleName ?: 'unknown']) {
            try {
                migrateTo(image, sl)
                incrementCounter('imagestore.migrate.success', 'Successful image migrations', [from: image.storageLocation?.class?.simpleName ?: 'unknown', to: sl?.class?.simpleName ?: 'unknown'])
            } catch (Exception e) {
                log.error("Unable to migrate image {} to storage location {}, rolling back changes...", image.imageIdentifier, sl)
                incrementCounter('imagestore.migrate.error', 'Failed image migrations', [from: image.storageLocation?.class?.simpleName ?: 'unknown', to: sl?.class?.simpleName ?: 'unknown', error: e.class.simpleName])
                // rollback any files migrated
                sl.deleteStored(image.imageIdentifier)
                throw e
            }
        }

    }

    private String normaliseThumbnailType(String type) {
        def typeLowerCase = type.toLowerCase()
        if (type.contains('darkGrey')) {
            type = type.replace('darkGrey', 'darkGray')
        }
        if (typeLowerCase.startsWith('thumbnail_')) {
            return type.substring('thumbnail_'.length())
        } else if (typeLowerCase == 'thumbnail') {
            return ''
        } else {
            return type
        }
    }

    private ImageInfo ensureThumbnailExists(String imageIdentifier, String dataResourceUid, StorageOperations operations, String type, boolean refresh = false) {
        type = normaliseThumbnailType(type)
        def key = Pair.of(imageIdentifier, type)
        def loader = this.&ensureThumbnailExistsCacheLoader.curry(dataResourceUid).curry(operations)

        // TODO undefined behaviour if already loading when invalidate is called.
        if (refresh) {
            thumbnailCache.synchronous().invalidate(key)
            thumbnailNegativeCache.invalidate(key)
        }
        if (!disableCache && thumbnailNegativeCache.getIfPresent(key)) {
            incrementCounter('imagestore.thumbnail.negativecache.hit', 'Thumbnail negative cache hits', [type: type ?: 'default'])
            return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: true, contentType: type == 'square' ? 'image/png' : 'image/jpeg')
        }
        return (disableCache ? loader.call(key) : awaitDerivativeLoad(thumbnailCache, key, loader)) ?: new ImageInfo(exists: false, imageIdentifier: imageIdentifier, contentType: type == 'square' ? 'image/png' : 'image/jpeg', shouldExist: true)
    }

    private ImageInfo ensureThumbnailExistsCacheLoader(String dataResourceUid, StorageOperations operations, Pair<String, String> pair) {
        def imageIdentifierArg = pair.left
        def typeArg = pair.right
        try {
            def info = operations.thumbnailImageInfo(imageIdentifierArg, typeArg)
            def exists = info.exists
            if (!exists) {
                def results = generateImageThumbnail(imageIdentifierArg, operations, typeArg)
                if (results.size() > 0) {
                    // A concurrent request may have negatively cached this key while we generated
                    thumbnailNegativeCache.invalidate(pair)
                    // Create the ImageThumbnail records asynchronously so we don't block any http requests
                    // waiting on the thumbnail to be generated
                    ImageThumbnail.async.task {
                        ImageThumbnail.withTransaction {
                            // These are deprecated, but we'll update them anyway...
                            def defThumb = results.find { it.thumbnailName.equalsIgnoreCase("thumbnail") }
                            if (defThumb) {
                                def thumbWidth = defThumb?.width ?: 0
                                def thumbHeight = defThumb?.height ?: 0
                                Image.executeUpdate("update Image set thumbWidth = :width, thumbHeight = :height where imageIdentifier = :imageIdentifier", [width: thumbWidth, height: thumbHeight, imageIdentifier: imageIdentifierArg])
                            }
                            def squareThumb = results.find({ it.thumbnailName.equalsIgnoreCase("thumbnail_square")})
                            if (squareThumb) {
                                def squareThumbSize = squareThumb?.width ?: 0
                                Image.executeUpdate("update Image set squareThumbSize = :square where imageIdentifier = :imageIdentifier", [square: squareThumbSize, imageIdentifier: imageIdentifierArg])
                            }

                            def image = Image.findByImageIdentifier(imageIdentifierArg)
                            def imageThumbs = results?.collect { th ->
                                def imageThumb = ImageThumbnail.findByImageAndName(image, th.thumbnailName)
                                if (imageThumb) {
                                    imageThumb.height = th.height
                                    imageThumb.width = th.width
                                    imageThumb.isSquare = th.square
                                } else {
                                    imageThumb = new ImageThumbnail(image: image, name: th.thumbnailName, height: th.height, width: th.width, isSquare: th.square)
                                }
                                imageThumb
                            }
                            ImageThumbnail.saveAll(imageThumbs)
                        }
                    }
                } else {
                    // Generation ran but produced zero thumbnails. Distinguish the expected case
                    // (audio/document content, for which no thumbnails are expected) from a genuine
                    // image that failed to produce any thumbnail. The discriminator is the DB
                    // mimeType via isActuallyAnImage: true -> image, false -> audio/doc, null ->
                    // not found (treated as an image so a real problem still surfaces as a WARN).
                    Boolean actuallyAnImage = storageLocationService.isActuallyAnImage(imageIdentifierArg)
                    if (actuallyAnImage == Boolean.FALSE) {
                        log.debug("No thumbnails generated for ${imageIdentifierArg} of type ${typeArg}: not an image, no thumbnails expected")
                    } else {
                        log.warn("Thumbnail generation for image ${imageIdentifierArg} of type ${typeArg} produced no results for an image")
                    }
                }
                info = operations.thumbnailImageInfo(imageIdentifierArg, typeArg)

            }
            // override these based on behaviour of the thumbnailer
            info.contentType = typeArg == 'square' ? 'image/png' : 'image/jpeg'
            info.extension = typeArg == 'square' ? 'png' : 'jpg'
            info.dataResourceUid = dataResourceUid
            return info
        } catch (NotAnImageException e) {
            // Fast-fail for non-image content - don't cache, but log and propagate
            log.warn("Attempted to generate thumbnail for non-image content: ${e.message}")
            incrementCounter('imagestore.thumbnail.notanimage', 'Thumbnail generation skipped - not an image', [mimeType: e.actualMimeType])
            return null // don't cache this error
        } catch (GenerateDerivativeTimeout e) {
            // A saturated generator is a request-level availability failure, not a missing thumbnail.
            throw e
        } catch (e) {
            def rootCause = ExceptionUtils.getRootCause(e)
            if (rootCause instanceof FileNotFoundException) {
                if (disableCache) {
                    log.warn("Original image ${imageIdentifierArg} not found in storage; cannot generate thumbnail of type ${typeArg}: ${e.message}")
                } else {
                    thumbnailNegativeCache.put(pair, Boolean.TRUE)
                    incrementCounter('imagestore.thumbnail.negativecache.store', 'Thumbnail negative cache stores', [type: typeArg ?: 'default'])
                    log.warn("Original image ${imageIdentifierArg} not found in storage; cannot generate thumbnail of type ${typeArg}. Suppressing repeat attempts for ${thumbnailNegativeCacheTtlSeconds}s: ${e.message}")
                }
            } else if (rootCause instanceof IIOException && rootCause.message?.contains('Unsupported marker type 0x13')) {
                log.warn("Error generating thumbnail for image ${imageIdentifierArg} of type ${typeArg} because the image appears to be corrupt: ${e.message}")
            } else {
                log.error("Error generating thumbnail for image ${imageIdentifierArg} of type ${typeArg}", e)
            }
            return null // don't cache this error
        }
    }

    @Immutable
    static final class Point {
        int x
        int y
        int z
    }

    private ImageInfo ensureTileExists(String identifier, String dataResourceUid, int zoomLevels, StorageOperations operations, int x, int y, int z, boolean refresh = false) {
        if (zoomLevels > 0 && z > zoomLevels) {
            // requested zoom level is beyond the zoom levels of the image so the tile will never exist
            return new ImageInfo(exists: false, imageIdentifier: identifier, shouldExist: false, contentType: 'image/png')
        }

        def loader = this.&ensureTileExistsCacheLoader.curry(zoomLevels).curry(operations)
        def originKey = Pair.of(identifier, new Point(0,0,z))
        def key = Pair.of(identifier, new Point(x, y, z))

        // TODO undefined behaviour if already loading when invalidate is called.
        if (refresh) {
            if (onDemandTilingEnabled) {
                tileCache.synchronous().invalidate(key)
            } else {
                tileCache.synchronous().invalidateAll([originKey, key])
            }
        }

        if (onDemandTilingEnabled) {
            // experimental: With on-demand tiling, attempt to fetch/generate exactly the requested tile
            def tileInfo = (disableCache ? loader.call(key) : awaitDerivativeLoad(tileCache, key, loader)) ?: new ImageInfo(exists: false, imageIdentifier: identifier, shouldExist: false, contentType: 'image/png')
            tileInfo.dataResourceUid = dataResourceUid
            return tileInfo
        } else {
            // Regular behaviour:
            // First check the origin tile for the zoom level, if it doesn't exist then we can generate
            // the whole set of tiles for the level
            def originInfo = (disableCache ? loader.call(originKey) : awaitDerivativeLoad(tileCache, originKey, loader)) ?: new ImageInfo(exists: false, imageIdentifier: identifier, shouldExist: true, contentType: 'image/png')

            // then if the origin was requested, return the origin info
            // or if the origin doesn't exist then any tile for the given zoom level won't exist either
            // so return the non-existent origin info
            if (x == 0 && y == 0 || !originInfo.exists) {
                originInfo.dataResourceUid = dataResourceUid
                return originInfo
            } else {
                // otherwise now we get the info for the tile that was actually requested and cache it
                def tileInfo = (disableCache ? loader.call(key) : awaitDerivativeLoad(tileCache, key, loader)) ?: new ImageInfo(exists: false, imageIdentifier: identifier, shouldExist: false, contentType: 'image/png') // shouldExist is actually unknown here because we don't know the tile bounds
                tileInfo.dataResourceUid = dataResourceUid
                return tileInfo
            }
        }
    }

    private ImageInfo ensureTileExistsCacheLoader(int zoomLevels, StorageOperations operations, Pair<String, Point> imageIdentifierAndZLevel) {
        def imageIdentifier = imageIdentifierAndZLevel.left
        def x = imageIdentifierAndZLevel.right.x
        def y = imageIdentifierAndZLevel.right.y
        def zLevel = imageIdentifierAndZLevel.right.z
        try {
            // get the origin tile for the given zoom level
            def info = operations.tileImageInfo(imageIdentifier, x, y, zLevel)
            def exists = info.exists
            if (!exists && (zoomLevels < 1 || zLevel <= zoomLevels)) {
                if (onDemandTilingEnabled) {
                    // experimental: generate only the requested tile
                    try {
                        def result = generateSingleTile(imageIdentifier, operations, zLevel, x, y)
                        if (result.success) {
                            incrementCounter('imagestore.tile.ondemand.success', 'Successful on-demand tile generations')
                            return operations.tileImageInfo(imageIdentifier, x, y, zLevel)
                        } else if (result.coordinateError) {
                            incrementCounter('imagestore.tile.ondemand.outofbounds', 'On-demand tile out-of-bounds', [reason: result.status.name()])
                            return ImageInfo.notFound() // if the tile is out of bounds then we can cache the result
                        } else if (result.status == TileGenerationResult.Status.NOT_AN_IMAGE || result == TileGenerationResult.Status.NO_IMAGE_READER) {
                            return ImageInfo.notFound() // TODO need a better way to indicate this
                        } else {
                            incrementCounter('imagestore.tile.ondemand.failure', 'Failed on-demand tile generations', [reason: result.status.name()])
                            return null // TODO should we cache an IO error?
                        }
                    } catch (GenerateDerivativeTimeout e) {
                        // Preserve generator saturation so the request can return a controlled 503.
                        throw e
                    } catch (Exception e) {
                        incrementCounter('imagestore.tile.ondemand.error', 'On-demand tile generation errors', [error: e.class.simpleName])
                        log.error("Error generating on-demand tile for image ${imageIdentifier} with co-ordinates x:${x}, y:${y}, z:${z}", e)
                        return null
                    }
                } else if (x == 0 && y == 0) {
                    // regular behavior: generate the whole level when origin is requested
                    def results = generateTMSTiles(imageIdentifier, operations, zoomLevels, zLevel)
                    if (results.success) {
                        info = operations.tileImageInfo(imageIdentifier, 0, 0, zLevel)
                    }
                }
            }
            return info
        } catch (GenerateDerivativeTimeout e) {
            // A saturated generator is a request-level availability failure, not a missing tile.
            throw e
        } catch (e) {
            def rootCause = ExceptionUtils.getRootCause(e)
            if (rootCause instanceof FileNotFoundException) {
                log.error("Error generating tiles for image ${imageIdentifier} with zoom level ${zLevel} because ${e.message}")
            } else if (rootCause instanceof IIOException && rootCause.message?.contains('Unsupported marker type 0x13')) {
                log.error("Error generating tiles for image ${imageIdentifier} with zoom level ${zLevel} because the image appears to be corrupt: ${e.message}")
            } else {
                if (x == 0 && y == 0) {
                    log.error("Error generating tiles for image ${imageIdentifier} with zoom level ${zLevel}", e)
                } else {
                    log.error("Error getting tile info for image ${imageIdentifier} with co-ordinates x:${x}, y:${y}, z:${zLevel}", e)
                }
            }

            return null // don't cache this error
        }
    }

    private TileGenerationResult generateSingleTile(String imageIdentifier, StorageOperations operations, int z, int x, int y) {
        return recordTime('imagestore.tile.generate.ondemand', 'Time to generate a single TMS tile on-demand', [z: z.toString(), x: x.toString(), y: y.toString()]) {
            if (tilingSemaphore.tryAcquire(tileConcurrencyTimeout, TimeUnit.SECONDS)) {
                try {
                    def input = operations.originalInputStream(imageIdentifier, null)
                    def sink = new TilerSink.PathBasedTilerSink(operations.tilerByteSinkFactory(imageIdentifier))
                    return onDemandImageTiler.generateTile(input, sink, z, x, y)
                } finally {
                    tilingSemaphore.release()
                }
            } else {
                incrementCounter('imagestore.tile.ondemand.timeout', 'On-demand tile generation timeouts')
                throw new GenerateDerivativeTimeout("Could not acquire tiling semaphore to generate on-demand tile for ${imageIdentifier} in ${tileConcurrencyTimeout}s")
            }
        }
    }

    @NotTransactional
    ImageInfo originalImageInfo(String imageIdentifier) {
        if (!imageIdentifier) {
            return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'application/octet-stream')
        }
        return recordTime('imagestore.info.original', 'Time to get original image info') {

            def result = originalCache.get(imageIdentifier, { key ->
                // Try to get storage operations without DB lookup first (single-storage optimisation)
                StorageOperations operations = storageLocationService.getStorageOperationsWithoutDbLookup()
                if (operations) {
                    // FAST PATH: Try to get original image info from storage
                    incrementCounter('imagestore.info.original.lookup.nodbhit', 'Original info lookups without DB hit')

                    def storageInfo = operations.originalImageInfo(imageIdentifier)
                    if (storageInfo.exists) {
                        // don't lookup the image in the db, trust the storage operations for stuff like
                        // etag, last modified, content type, extension
                        incrementCounter('imagestore.info.original.found.nodbhit', 'Original found without full DB hit')
                        storageInfo.shouldExist = true
                        return storageInfo
                    } else {
                        incrementCounter('imagestore.info.original.notfound', 'Original image not found')
                        return null
                    }
                }
                // Fall back to full database lookup
                Image image
                operations = Image.withTransaction(readOnly: true) {
                    image = Image.findByImageIdentifier(imageIdentifier, [ cache: true, fetch: [ storageLocation: 'join' ] ])
                    storageLocationService.getStorageOperationsForImage(image)
                }
                def imageInfo = operations.originalImageInfo(imageIdentifier)
                if (imageInfo.exists) {
                    // Enhance storage info with DB metadata
                    if (image) {
                        imageInfo.dataResourceUid = image.dataResourceUid
                        imageInfo.etag = image.contentSHA1Hash
                        imageInfo.lastModified = image.dateUploaded
                        imageInfo.contentType = image.mimeType
                        imageInfo.extension = image.extension
                        imageInfo.shouldExist = true
                    }

                    return imageInfo
                }

            })
            if (result != null) {
                incrementCounter('imagestore.info.original.cache.hit', 'Original image info cache hit')
                return result
            }
            incrementCounter('imagestore.info.original.notfound', 'Original image not found')
            // Don't cache not-found responses (they might be uploaded later)
            return null
        } ?: new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'application/octet-stream')
    }

    @NotTransactional
    ImageInfo thumbnailImageInfo(String imageIdentifier, String type, boolean refresh = false) {
        if (!imageIdentifier) {
            return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/jpeg')
        }
        return recordTime('imagestore.info.thumbnail', 'Time to get thumbnail image info', [type: type ?: 'default', refresh: refresh.toString()]) {
            // Try to get storage operations without DB lookup first (single-storage optimisation)
            StorageOperations operations = storageLocationService.getStorageOperationsWithoutDbLookup()
            String dataResourceUid = null
            String mimeType = null

            if (operations) {
                // FAST PATH: Assume it's an image (vast majority of cases)
                // Only check mimeType if the thumbnail operation indicates it might not be an image
                incrementCounter('imagestore.info.thumbnail.lookup.nodbhit', 'Thumbnail info lookups without DB hit', [type: type ?: 'default'])

                ImageInfo info
                try {
                    info = ensureThumbnailExists(imageIdentifier, null, operations, type, refresh)
                    if (info && info.exists) {
                        incrementCounter('imagestore.info.thumbnail.found', 'Thumbnail found', [type: type ?: 'default'])
                        return info
                    }
                    // Thumbnail doesn't exist - need to check if it's because it's not an image
                    // or because we need full DB lookup for generation
                } catch (NotAnImageException e) {
                    // Caught during generation attempt - it's audio/document
                    // Fall through to mime type check below
                    log.debug("Detected non-image during thumbnail attempt: {}", e.message)
                }

                // Check if it's actually an image (only when thumbnail doesn't exist or generation failed)
                Boolean isImage = storageLocationService.isActuallyAnImage(imageIdentifier)

                if (isImage == null) {
                    // Image not found in database
                    incrementCounter('imagestore.info.thumbnail.notfound', 'Thumbnail not found - image not found', [type: type ?: 'default'])
                    return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/jpeg')
                } else if (!isImage) {
                    // It's audio/document - serve placeholder
                    mimeType = storageLocationService.getMimeType(imageIdentifier)
                    incrementCounter('imagestore.info.thumbnail.placeholder', 'Placeholder thumbnail used (fast-fail)', [mimeType: mimeType.split('/')[0]])
                    log.debug("Fast-fail: {} is {}, serving placeholder instead of thumbnail", imageIdentifier, mimeType)

                    return nonImageThumbnailImageInfo(mimeType, type, imageIdentifier)
                } else if (info != null) { // isImage == true && !info.exists
                    log.debug("Confirmed {} is an image but thumbnail of type {} does not exist in storage", imageIdentifier, type)
                    incrementCounter('imagestore.info.thumbnail.notfound', 'Thumbnail not found', [type: type ?: 'default'])
                    return info
                }
            }

            // Fall back to database lookup
            Image image
            Image.withNewTransaction(readOnly: true) {
                image = Image.findByImageIdentifier(imageIdentifier, [ cache: true, fetch: [ storageLocation: 'join' ] ])
                if (image) {
                    if (!operations) {
                        operations = storageLocationService.getStorageOperationsForImage(image)
                    }
                    dataResourceUid = image.dataResourceUid
                    mimeType = image.mimeType
                }
            }
            if (image) {
                if (mimeType.startsWith('image/')) {
                    incrementCounter('imagestore.info.thumbnail.lookup', 'Thumbnail info lookups', [type: type ?: 'default', mimeType: 'image'])
                    def info = ensureThumbnailExists(imageIdentifier, dataResourceUid, operations, type, refresh)
                    if (info) {
                        if (info.exists) {
                            incrementCounter('imagestore.info.thumbnail.found', 'Thumbnail found', [type: type ?: 'default'])
                        } else {
                            incrementCounter('imagestore.info.thumbnail.notfound', 'Thumbnail not found', [type: type ?: 'default'])
                        }
                        return info
                    }
                } else {
                    incrementCounter('imagestore.info.thumbnail.placeholder', 'Placeholder thumbnail used', [mimeType: mimeType.split('/')[0]])
                    return nonImageThumbnailImageInfo(mimeType, type, imageIdentifier)
                }
            }
            incrementCounter('imagestore.info.thumbnail.notfound', 'Thumbnail not found - image not found', [type: type ?: 'default'])
            return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/jpeg')
        }
    }

    private ImageInfo nonImageThumbnailImageInfo(String mimeType, String type, String imageIdentifier, String dataResourceUid = null) {
        def resource
        if (mimeType.startsWith('audio/')) {
            resource = (type == 'large' || type == 'thumbnail_large') ? audioLargeThumbnail : audioThumbnail
        } else {
            resource = (type == 'large' || type == 'thumbnail_large') ? documentLargeThumbnail : documentThumbnail
        }

        return new ImageInfo(
                exists: true,
                imageIdentifier: imageIdentifier,
                dataResourceUid: dataResourceUid,
                length: resource.contentLength(),
                lastModified: new Date(resource.lastModified()),
                contentType: 'image/png',
                extension: 'png',
                inputStreamSupplier: { range -> range.wrapInputStream(resource.inputStream) },
                shouldExist: true
        )
    }

    @NotTransactional
    ImageInfo tileImageInfo(String imageIdentifier, int x, int y, int z, boolean refresh = false) {
        if (!imageIdentifier) {
            return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/png')
        }
        return recordTime('imagestore.info.tile', 'Time to get tile image info', [z: z.toString(), refresh: refresh.toString()]) {
            // Try to get storage operations without DB lookup first (single-storage optimisation)
            StorageOperations operations = storageLocationService.getStorageOperationsWithoutDbLookup()
            String dataResourceUid = null
            Integer zoomLevels = null
            String mimeType = null

            if (operations) {
                // FAST PATH: Assume it's an image (vast majority of cases)
                // Check if tile exists in storage first
                incrementCounter('imagestore.info.tile.lookup.nodbhit', 'Tile info lookups without DB hit', [z: z.toString()])

                // No zoom level info here, so we'll pass 0 to skip zoom level check
                // and have to calculate it if the tile doesn't exist
                def quickInfo = ensureTileExists(imageIdentifier, dataResourceUid, 0, operations, x, y, z, refresh)
                if (quickInfo.exists) {
                    // Tile exists - must be an image, return immediately (common case, 0 DB queries!)
                    incrementCounter('imagestore.info.tile.found.nodbhit', 'Tile found without DB hit', [z: z.toString()])
                    return quickInfo
                }

                // Tile doesn't exist - check if it's because it's not an image
                // Only do this check now (lazy evaluation for non-image minority case)
                Boolean isImage = storageLocationService.isActuallyAnImage(imageIdentifier)

                if (isImage == null) {
                    // Image not found in database
                    incrementCounter('imagestore.info.tile.notfound', 'Tile not found - image not found', [z: z.toString()])
                    return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/png')
                } else if (!isImage) {
                    // It's audio/document - tiles don't exist for non-images
                    mimeType = storageLocationService.getMimeType(imageIdentifier)
                    log.debug("Fast-fail: {} is {}, tiles do not exist for non-image content", imageIdentifier, mimeType)
                    incrementCounter('imagestore.info.tile.notfound', 'Tile not found - not an image', [z: z.toString(), mimeType: mimeType.split('/')[0]])
                    return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/png')
                } else { // isImage == true && !quickInfo.exists
                    // It IS an image but tile doesn't exist in storage
                    // so fall through to DB lookup below to get zoom levels
                    incrementCounter('imagestore.info.tile.notfound', 'Tile not found', [z: z.toString(), shouldExist: quickInfo.shouldExist.toString()])
                    return quickInfo
                }
            }

            // Fall back to database lookup
            Image image
            Image.withNewTransaction(readOnly: true) {
                image = Image.findByImageIdentifier(imageIdentifier, [ cache: true, fetch: [ storageLocation: 'join' ] ])
                if (image) {
                    if (!operations) {
                        operations = storageLocationService.getStorageOperationsForImage(image)
                    }
                    dataResourceUid = image.dataResourceUid
                    zoomLevels = image.zoomLevels
                    mimeType = image.mimeType
                }
            }
            if (image) {
                if (mimeType.startsWith('image/')) {
                    incrementCounter('imagestore.info.tile.lookup', 'Tile info lookups', [z: z.toString()])
                    def info = ensureTileExists(imageIdentifier, dataResourceUid, zoomLevels, operations, x, y, z, refresh)
                    if (info.exists) {
                        incrementCounter('imagestore.info.tile.found', 'Tile found', [z: z.toString()])
                    } else {
                        incrementCounter('imagestore.info.tile.notfound', 'Tile not found', [z: z.toString(), shouldExist: info.shouldExist.toString()])
                    }
                    return info
                } else {
                    // Non-image content doesn't have tiles
                    log.debug("Tile requested for non-image content: {} is {}", imageIdentifier, mimeType)
                    incrementCounter('imagestore.info.tile.notfound', 'Tile not found - not an image', [z: z.toString(), mimeType: mimeType.split('/')[0]])
                    return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/png')
                }
            }

            incrementCounter('imagestore.info.tile.notfound', 'Tile not found - image not found', [z: z.toString()])
            return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/png')
        }
    }

    static final class GenerateDerivativeTimeout extends RuntimeException {
        GenerateDerivativeTimeout(String message) {
            super(message, null, false, false)
        }
    }

    static final class DerivativeLoadTimeout extends RuntimeException {
        DerivativeLoadTimeout(String message) {
            super(message, null, false, false)
        }
    }

    static final class DerivativeLoadRejected extends RuntimeException {
        DerivativeLoadRejected(String message, Throwable cause = null) {
            super(message, cause, false, false)
        }
    }

    /**
     * Submits {@code loader} for {@code key} to the derivative loader, coalescing concurrent requests for the
     * same key. The submit (which may block in CallerBlocksPolicy) happens outside the cache's map computation
     * so it never holds Caffeine locks.
     */
    private <K> ImageInfo awaitDerivativeLoad(AsyncCache<K, ImageInfo> cache, K key, Closure<ImageInfo> loader) {
        CompletableFuture<ImageInfo> placeholder = new CompletableFuture<>()
        CompletableFuture<ImageInfo> future = cache.get(key, { k, e -> placeholder } as BiFunction<K, Executor, CompletableFuture<ImageInfo>>)
        if (future.is(placeholder)) {
            submitDerivativeLoad(cache, key, loader, placeholder, derivativeLoaderExecutor)
        }

        try {
            return future.get(derivativeLoadTimeoutSeconds, TimeUnit.SECONDS)
        } catch (TimeoutException e) {
            cache.asMap().remove(key, future)
            throw new DerivativeLoadTimeout("Timed out waiting ${derivativeLoadTimeoutSeconds}s for derivative loader result for ${key}")
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt()
            throw new IllegalStateException("Interrupted while waiting for derivative loader result for ${key}", e)
        } catch (ExecutionException e) {
            Throwable cause = e.cause
            if (cause instanceof RejectedExecutionException || cause instanceof DerivativeLoadRejected) {
                throw new DerivativeLoadRejected("Derivative loader is overloaded for ${key}", cause)
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause
            }
            throw new IllegalStateException("Derivative loader failed for ${key}", cause)
        }
    }

    private <K> void submitDerivativeLoad(AsyncCache<K, ImageInfo> cache, K key, Closure<ImageInfo> loader, CompletableFuture<ImageInfo> result, Executor executor) {
        try {
            executor.execute({
                try {
                    result.complete(loader.call(key))
                } catch (Throwable t) {
                    result.completeExceptionally(t)
                }
            } as Runnable)
        } catch (RejectedExecutionException e) {
            cache.asMap().remove(key, result)
            result.completeExceptionally(e)
            String poolState = describePoolState(executor)
            log.debug('Derivative load rejected for {} ({})', key, poolState)
            throw new DerivativeLoadRejected("Derivative loader is overloaded for ${key} (${poolState})", e)
        }
    }

    private static String describePoolState(Executor executor) {
        if (executor instanceof ThreadPoolTaskExecutor) {
            try {
                ThreadPoolExecutor pool = ((ThreadPoolTaskExecutor) executor).threadPoolExecutor
                return "poolSize=${pool.poolSize} activeCount=${pool.activeCount} queueSize=${pool.queue.size()} queueRemainingCapacity=${pool.queue.remainingCapacity()} maximumPoolSize=${pool.maximumPoolSize}"
            } catch (IllegalStateException ignored) {
                // executor not initialised
            }
        }
        return "executor ${executor?.class?.name}"
    }
}
