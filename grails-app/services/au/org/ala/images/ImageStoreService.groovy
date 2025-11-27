package au.org.ala.images

import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.thumb.ImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.tiling.DefaultZoomFactorStrategy
import au.org.ala.images.tiling.ImageTiler3
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.ImageTilerResults
import au.org.ala.images.tiling.TileFormat
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.util.ImageReaderUtils
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.io.ByteSource
import grails.gorm.transactions.NotTransactional
import grails.gorm.transactions.Transactional
import grails.web.mapping.LinkGenerator
import groovy.transform.Immutable
import groovy.util.logging.Slf4j
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.FileHeader
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.tuple.Pair
import org.apache.tika.Tika
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.web.multipart.MultipartFile

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import javax.imageio.IIOException
import javax.imageio.ImageIO
import javax.imageio.ImageReadParam
import java.awt.Color
import java.awt.Rectangle
import java.awt.image.BufferedImage
import java.nio.file.Files

import org.grails.orm.hibernate.cfg.GrailsHibernateUtil

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Slf4j
class ImageStoreService {

    public static final int TILE_SIZE = 256

    def grailsApplication
    def auditService
    LinkGenerator grailsLinkGenerator

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

    @Value('${tiling.levelThreads:2}')
    int tilingLevelThreads = 2

    @Value('${tiling.ioVirtualThreads:true}')
    boolean tilingIoVirtualThreads = true

    @Value('${tiling.ioThreads:2}')
    int tilingIoThreads = 2

    Cache<Pair<String, String>, ImageInfo> thumbnailCache
    Cache<Pair<String, Point>, ImageInfo> tileCache

    ExecutorService tilingIoPool
    ExecutorService tilingWorkPool

    @PostConstruct
    @NotTransactional
    def init() {
        thumbnailCache = Caffeine.from(thumbnailLookupCacheConfig).build()
        tileCache = Caffeine.from(tileLookupCacheConfig).build()
        tilingIoPool = tilingIoVirtualThreads ? Executors.newVirtualThreadPerTaskExecutor() : Executors.newFixedThreadPool(tilingIoThreads)
        tilingWorkPool = Executors.newFixedThreadPool(tilingLevelThreads)
    }

    @PreDestroy
    @NotTransactional
    void destroy() {
        closePool(tilingWorkPool, 10, TimeUnit.SECONDS)
        closePool(tilingIoPool, 10, TimeUnit.SECONDS)
    }

    private static void closePool(ExecutorService pool, long timeout, TimeUnit unit) {
        boolean terminated = pool.isTerminated()
        if (!terminated) {
            pool.shutdown()
            boolean interrupted = false
            while (!terminated) {
                try {
                    terminated = pool.awaitTermination(timeout, unit);
                } catch (InterruptedException e) {
                    if (!interrupted) {
                        pool.shutdownNow();
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @NotTransactional
    def clearThumbnailLookupCache() {
        thumbnailCache.invalidateAll()
    }

    @NotTransactional
    def clearTileLookupCache() {
        tileCache.invalidateAll()
    }
    
    @NotTransactional
    def clearTileLookupCacheForImage(String imageIdentifier) {
        // Find all cache entries for this image identifier and invalidate them
        tileCache.asMap().keySet().findAll { it.left == imageIdentifier }.each { key ->
            tileCache.invalidate(key)
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
        def uuid = UUID.randomUUID().toString()
        if (log.isTraceEnabled()) {
            log.trace('Storing image {} with content type {}, original filename {}, content disposition {} to {}', uuid, contentType, originalFilename, contentDisposition, operations)
        }
        def imgDesc = new ImageDescriptor(imageIdentifier: uuid)
        operations.store(uuid, imageBytes.openStream(), contentType, contentDisposition, imageBytes.sizeIfKnown().orNull())
        def filename = ImageUtils.getFilename(originalFilename)
        if (contentType?.toLowerCase()?.startsWith('image')) {
            if (log.isTraceEnabled()) {
                log.trace('Getting image dimensions for image {}, filename {}, content type {}', uuid, filename, contentType)
            }
            def dimensions = ImageReaderUtils.getImageDimensions(imageBytes, filename) // TODO replace original filename with a temp filename with extension determine by content type?
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

        return imgDesc
    }

    @Transactional(readOnly = true)
    InputStream retrieveImageInputStream(String imageIdentifier) {
        return Image.findByImageIdentifier(imageIdentifier, [ cache: true, fetch: [ storageLocation: 'join'] ] ).originalInputStream()
    }

    Map retrieveImageRectangle(Image parentImage, int x, int y, int width, int height) {

        def results = [bytes: null, contentType: ""]

        if (parentImage) {
            def imageBytes = parentImage.retrieve()
            def reader = ImageReaderUtils.findCompatibleImageReader(imageBytes);
            if (reader) {
                try {
                    Rectangle stripRect = new Rectangle(x, y, width, height);
                    ImageReadParam params = reader.getDefaultReadParam();
                    params.setSourceRegion(stripRect);
                    params.setSourceSubsampling(1, 1, 0, 0);
                    // This may fail if there is not enough heap!
                    BufferedImage subimage = reader.read(0, params);
                    def bos = new ByteArrayOutputStream()
                    if (!ImageIO.write(subimage, "PNG", bos)) {
                        log.debug("Could not create subimage in PNG format. Giving up")
                        return null
                    } else {
                        results.contentType = "image/png"
                    }
                    results.bytes = bos.toByteArray()
                    bos.close()
                } finally {
                    reader.dispose()
                }
            } else {
                throw new RuntimeException("No appropriate reader for image type!");
            }
        }

        return results
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
                return image.originalInputStream()
            }
        }
        return generateThumbnailsImpl(byteSource, image.imageIdentifier, image.storageLocation.asStandaloneStorageOperations())
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
        def ct = new CodeTimer("Generating ${type != null ? 1 : 6} thumbnails for image ${imageIdentifier}")
        def t = new ImageThumbnailer()
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

        def results = t.generateThumbnailsNoIntermediateEncode(
                byteSource,
                operations.thumbnailByteSinkFactory(imageIdentifier),
                thumbDefs
        )

        auditService.log(imageIdentifier, "Thumbnails created", "N/A")
        ct.stop(true)
        return results
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
        log.debug("Generating TMS compatible tiles for image ${imageIdentifier}, zoom level ${z}")
        def ct = new CodeTimer("Tiling image ${imageIdentifier}, z index: $z")

        def results
        if (z != null) {
            results = tileImageLevel(imageIdentifier, operations, z)
        } else {
            results = tileImage(imageIdentifier, operations)
        }
        if (results.success) {
            if (zoomLevels != results.zoomLevels) {
                // only update the zoom levels if they have changed
                // run the update concurrently so as not to block a tile http request
                Image.async.task {
                    withTransaction {
                        def updates = Image.executeUpdate("update Image set zoomLevels = :zoomLevels where imageIdentifier = :imageIdentifier and zoomLevels != :zoomLevels", [zoomLevels: results.zoomLevels, imageIdentifier: imageIdentifier])
                        if (updates < 1) {
                            log.warn("Failed to update zoom levels for image ${imageIdentifier}")
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

    private ImageTilerResults tileImageLevel(String imageIdentifier, StorageOperations operations, int z) {
        def config = new ImageTilerConfig(tilingIoPool, tilingWorkPool, TILE_SIZE, 6, TileFormat.JPEG)
        config.setTileBackgroundColor(new Color(221, 221, 221))
        def tiler = new ImageTiler3(config)
        return tiler.tileImage(
                operations.originalInputStream(imageIdentifier, null),
                new TilerSink.PathBasedTilerSink(operations.tilerByteSinkFactory(imageIdentifier)),
                z,
                z
        )
    }

    private ImageTilerResults tileImage(String imageIdentifier, StorageOperations operations) {
        def config = new ImageTilerConfig(tilingIoPool, tilingWorkPool, TILE_SIZE, 6, TileFormat.JPEG)
        config.setTileBackgroundColor(new Color(221, 221, 221))
        def tiler = new ImageTiler3(config)
        return tiler.tileImage(
                operations.originalInputStream(imageIdentifier, null),
                new TilerSink.PathBasedTilerSink(operations.tilerByteSinkFactory(imageIdentifier))
        )
    }

    boolean storeTilesArchiveForImage(Image image, MultipartFile zipFile) {

        if (image.stored()) {
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
                szf.getInputStream(fh).withStream { stream ->
                    def contentType = tika.detect(stream, fh.fileName)
                    def length = fh.uncompressedSize
                    GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).storeTileZipInputStream(image.imageIdentifier, fh.fileName, contentType, length, szf.getInputStream(fh))
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
        try {
            image.migrateTo(sl)
        } catch (Exception e) {
            log.error("Unable to migrate image {} to storage location {}, rolling back changes...", image.imageIdentifier, sl)
            // rollback any files migrated
            sl.deleteStored(image.imageIdentifier)
            throw e
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
            thumbnailCache.invalidate(key)
        }
        return thumbnailCache.get(key, loader) ?: new ImageInfo(exists: false, imageIdentifier: imageIdentifier, contentType: type == 'square' ? 'image/png' : 'image/jpeg', shouldExist: true)
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
                }
                info = operations.thumbnailImageInfo(imageIdentifierArg, typeArg)

            }
            // override these based on behaviour of the thumbnailer
            info.contentType = typeArg == 'square' ? 'image/png' : 'image/jpeg'
            info.extension = typeArg == 'square' ? 'png' : 'jpg'
            info.dataResourceUid = dataResourceUid
            return info
        } catch (e) {
            def rootCause = ExceptionUtils.getRootCause(e)
            if (rootCause instanceof FileNotFoundException) {
                log.error("Error generating thumbnail for image ${imageIdentifierArg} of type ${typeArg} because ${e.message}")
            } else if (rootCause instanceof IIOException && rootCause.message?.contains('Unsupported marker type 0x13')) {
                log.error("Error generating thumbnail for image ${imageIdentifierArg} of type ${typeArg} because the image appears to be corrupt: ${e.message}")
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
            tileCache.invalidateAll([originKey, key])
        }
        // First check the origin tile for the zoom level, if it doesn't exist then we can generate
        // the whole set of tiles for the level
        def originInfo = tileCache.get(originKey, loader) ?: new ImageInfo(exists: false, imageIdentifier: identifier, shouldExist: true, contentType: 'image/png')

        // then if the origin was requested, return the origin info
        // or if the origin doesn't exist then any tile for the given zoom level won't exist either
        // so return the non-existent origin info
        if (x == 0 && y == 0 || !originInfo.exists) {
            originInfo.dataResourceUid = dataResourceUid
            return originInfo
        } else {
            // otherwise now we get the info for the tile that was actually requested and cache it
            def tileInfo = tileCache.get(key, loader) ?: new ImageInfo(exists: false, imageIdentifier: identifier, shouldExist: false, contentType: 'image/png') // shouldExist is actually unknown here because we don't know the tile bounds
            tileInfo.dataResourceUid = dataResourceUid
            return tileInfo
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
            // currently we only generate tiles when the leve's origin is requested
            // because each level is generated in one go.
            if (x == 0 && y == 0 && !exists && (zoomLevels < 1 || zLevel <= zoomLevels)) {
                def results = generateTMSTiles(imageIdentifier, operations, zoomLevels, zLevel)
                if (results.success) {
                    info = operations.tileImageInfo(imageIdentifier, 0, 0, zLevel)
                }
            }
            return info
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

    ImageInfo originalImageInfo(String imageIdentifier) {
        def image = Image.findByImageIdentifier(imageIdentifier, [ cache: true, fetch: [ storageLocation: 'join' ] ])
        if (image) {
            def imageInfo = image.storageLocation.originalImageInfo(image.imageIdentifier)
            // override these to match original behaviour
            imageInfo.dataResourceUid = image.dataResourceUid
            imageInfo.etag = image.contentSHA1Hash
            imageInfo.lastModified = image.dateUploaded
            imageInfo.contentType = image.mimeType
            imageInfo.extension = image.extension
            imageInfo.shouldExist = true
            return imageInfo
        }
        return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'application/octet-stream')
    }

    @NotTransactional
    ImageInfo thumbnailImageInfo(String imageIdentifier, String type, boolean refresh = false) {
        Image image
        StorageOperations operations = null
        String dataResourceUid = null
        Image.withNewTransaction(readOnly: true) {
            image = Image.findByImageIdentifier(imageIdentifier, [ cache: true, fetch: [ storageLocation: 'join' ] ])
            if (image) {
                operations = GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).asStandaloneStorageOperations()
                dataResourceUid = image.dataResourceUid
            }
        }
        if (image) {
            if (image.mimeType.startsWith('image/')) {
                def info = ensureThumbnailExists(imageIdentifier, dataResourceUid, operations, type, refresh)
                if (info) {
                    return info
                }
            } else {
                def resource
                if (image.mimeType.startsWith('audio/')) {
                    if (type == 'large' || type == 'thumbnail_large') {
                        resource = audioLargeThumbnail
                    } else {
                        resource = audioThumbnail
                    }
                } else {
                    if (type == 'large' || type == 'thumbnail_large') {
                        resource = documentLargeThumbnail
                    } else {
                        resource = documentThumbnail
                    }
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
        }
        return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/jpeg')
    }

    @NotTransactional
    ImageInfo tileImageInfo(String imageIdentifier, int x, int y, int z, boolean refresh = false) {

        Image image
        StorageOperations operations = null
        String dataResourceUid = null
        Image.withNewTransaction(readOnly: true) {
            image = Image.findByImageIdentifier(imageIdentifier, [ cache: true, fetch: [ storageLocation: 'join' ] ])
            if (image) {
                operations = GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).asStandaloneStorageOperations()
                dataResourceUid = image.dataResourceUid
            }
        }
        if (image) {
            if (image.mimeType.startsWith('image/')) {
                def info = ensureTileExists(imageIdentifier, dataResourceUid, image.zoomLevels, operations, x, y, z, refresh)
                return info
            }
        }

        return new ImageInfo(exists: false, imageIdentifier: imageIdentifier, shouldExist: false, contentType: 'image/png')
    }

    long consumedSpace(Image image) {
        image.consumedSpace()
    }
}
