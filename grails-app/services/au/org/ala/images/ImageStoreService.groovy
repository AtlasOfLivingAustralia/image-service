package au.org.ala.images

import au.org.ala.images.thumb.ImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.tiling.ImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.ImageTilerResults
import au.org.ala.images.tiling.TileFormat
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.util.ImageReaderUtils
import grails.gorm.transactions.Transactional
import grails.web.mapping.LinkGenerator
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.FileHeader
import org.apache.commons.io.FileUtils
import org.apache.tika.Tika
import org.springframework.web.multipart.MultipartFile
import javax.imageio.ImageIO
import javax.imageio.ImageReadParam
import java.awt.Color
import java.awt.Rectangle
import java.awt.image.BufferedImage
import java.nio.file.Files

import org.grails.orm.hibernate.cfg.GrailsHibernateUtil

class ImageStoreService {

    def grailsApplication
    def logService
    def auditService
    LinkGenerator grailsLinkGenerator

    ImageDescriptor storeImage(byte[] imageBytes, StorageLocation sl, String contentType, String contentDisposition = null) {
        def uuid = UUID.randomUUID().toString()
        def imgDesc = new ImageDescriptor(imageIdentifier: uuid)
        sl.store(uuid, imageBytes, contentType, contentDisposition)
        def reader = ImageReaderUtils.findCompatibleImageReader(imageBytes)
        if (reader) {
            imgDesc.height = reader.getHeight(0)
            imgDesc.width = reader.getWidth(0)
            reader.dispose()
        }
        return imgDesc
    }

    @Transactional(readOnly = true)
    byte[] retrieveImage(String imageIdentifier) {
        return Image.findByImageIdentifier(imageIdentifier, [ cache: true] ).retrieve()
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
                        logService.debug("Could not create subimage in PNG format. Giving up")
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
        def imageBytes = image.retrieve()
        return generateThumbnailsImpl(imageBytes, image)
    }

    private List<ThumbnailingResult> generateThumbnailsImpl(byte[] imageBytes, Image image) {
        def t = new ImageThumbnailer()
        def imageIdentifier = image.imageIdentifier
        int size = grailsApplication.config.getProperty('imageservice.thumbnail.size') as Integer
        def thumbDefs = [
            new ThumbDefinition(size, false, null, "thumbnail"),
            new ThumbDefinition(size, true, null, "thumbnail_square"),
            new ThumbDefinition(size, true, Color.black, "thumbnail_square_black"),
            new ThumbDefinition(size, true, Color.white, "thumbnail_square_white"),
            new ThumbDefinition(size, true, Color.darkGray, "thumbnail_square_darkGray"),
            new ThumbDefinition(650, false, null, "thumbnail_large"),
        ]
        def results = t.generateThumbnails(imageBytes, GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).thumbnailByteSinkFactory(image.imageIdentifier), thumbDefs as List<ThumbDefinition>)
        auditService.log(imageIdentifier, "Thumbnails created", "N/A")
        return results
    }

    void generateTMSTiles(String imageIdentifier) {
        logService.log("Generating TMS compatible tiles for image ${imageIdentifier}")
        def ct = new CodeTimer("Tiling image ${imageIdentifier}")
        def image = Image.findByImageIdentifier(imageIdentifier, [ cache: true])

        def results = tileImage(image)
        if (results.success) {
            if (image) {
                image.zoomLevels = results.zoomLevels
                image.save(flush: true, failOnError: true)
            } else {
                logService.log("Image not found in database! ${imageIdentifier}")
            }
        } else {
            logService.log("Image tiling failed! ${results}");
        }
        auditService.log(imageIdentifier, "TMS tiles generated", "N/A")
        ct.stop(true)
    }

    private static ImageTilerResults tileImage(Image image) {
        def config = new ImageTilerConfig(2,2,256, 6, TileFormat.JPEG)
        config.setTileBackgroundColor(new Color(221, 221, 221))
        def tiler = new ImageTiler(config)
        return tiler.tileImage(image.originalInputStream(), new TilerSink.PathBasedTilerSink(GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).tilerByteSinkFactory(image.imageIdentifier)))
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

    // delegating methods for Unit Testing
    InputStream originalInputStream(Image image, Range range) {
        image.originalInputStream(range)
    }

    long thumbnailStoredLength(Image image) {
        image.thumbnailStoredLength()
    }

    InputStream thumbnailInputStream(Image image, Range range) {
        image.thumbnailInputStream(range)
    }

    long thumbnailTypeStoredLength(Image image, String type) {
        image.thumbnailTypeStoredLength(type)
    }

    InputStream thumbnailTypeInputStream(Image image, String type, Range range) {
        image.thumbnailTypeInputStream(type, range)
    }

    long tileStoredLength(Image image, int x, int y, int z) {
        image.tileStoredLength(x, y, z)
    }

    InputStream tileInputStream(Image image, Range range, int x, int y, int z) {
        image.tileInputStream(range, x, y, z)
    }

    long consumedSpace(Image image) {
        image.consumedSpace()
    }
}
