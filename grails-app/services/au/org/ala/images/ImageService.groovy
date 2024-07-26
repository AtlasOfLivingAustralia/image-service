package au.org.ala.images

import au.org.ala.images.metadata.MetadataExtractor
import au.org.ala.images.metrics.MetricsSupport
import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.tiling.TileFormat
import com.google.common.hash.Hashing
import com.google.common.io.ByteSource
import com.google.common.io.Files
import com.google.common.util.concurrent.Striped
import com.opencsv.CSVParserBuilder
import com.opencsv.CSVWriterBuilder
import com.opencsv.RFC4180ParserBuilder
import grails.gorm.transactions.NotTransactional
import grails.gorm.transactions.Transactional
import grails.orm.HibernateCriteriaBuilder
import grails.web.servlet.mvc.GrailsParameterMap
import groovy.transform.NamedVariant
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import groovy.util.logging.Slf4j
import groovyx.gpars.AsyncException
import groovyx.gpars.GParsPool
import jsr166y.ForkJoinPool
import okhttp3.HttpUrl
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DatumWriter
import org.apache.commons.codec.binary.Base64
import org.apache.commons.imaging.Imaging
import org.apache.commons.imaging.common.ImageMetadata
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata
import org.apache.commons.imaging.formats.tiff.TiffField
import org.apache.commons.imaging.formats.tiff.constants.ExifTagConstants
import org.apache.commons.imaging.formats.tiff.taginfos.TagInfo
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.RandomUtils
import org.apache.commons.lang3.StringUtils
import org.apache.tika.mime.MimeType
import org.apache.tika.mime.MimeTypes
import org.codehaus.groovy.runtime.StackTraceUtils
import org.hibernate.FlushMode
import org.hibernate.ScrollMode
import org.hibernate.StaleStateException
import org.springframework.beans.factory.annotation.Value
import org.springframework.orm.hibernate5.HibernateOptimisticLockingFailureException
import org.springframework.transaction.TransactionStatus
import org.springframework.web.multipart.MultipartFile

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Timestamp
import java.sql.Types
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.Lock
import java.util.function.Supplier

import org.imgscalr.Scalr
import org.springframework.beans.factory.annotation.Value

import java.awt.image.BufferedImage
import javax.imageio.ImageIO

import static grails.web.http.HttpHeaders.USER_AGENT

@Slf4j
class ImageService implements MetricsSupport {

    static final String EXPORT_IMAGES_SQL = """SELECT * FROM export_images;"""
    static final String EXPORT_MAPPING_SQL = """SELECT * FROM export_mapping;"""

    static final String DEFAULT_DATE_FORMAT = "dd-MMM-yyyy";
    static final String DEFAULT_TIMESTAMP_FORMAT = "dd-MMM-yyyy HH:mm:ss";

    def dataSource
    def imageStoreService
    def tagService
    def grailsApplication
    def auditService
    def sessionFactory
    def imageService
    def elasticSearchService
    def settingService
    def collectoryService
    def downloadService
    def storageLocationService

    final static List<String> SUPPORTED_UPDATE_FIELDS = [
        "audience",
        "contributor",
        "created",
        "creator",
        "description",
        "license",
        "publisher",
        "references",
        "rights",
        "rightsHolder",
        "source",
        "title",
        "type"
    ]

    // missing \p{Unassigned}\p{Surrogate]\p{Control} from regex as Unicode character classes unsupported in PG.
    // TODO use jooq to generate these
    final EXPORT_DATASET_SQL = '''
SELECT
  image_identifier AS "imageID",
  NULLIF(regexp_replace(regexp_replace(unnest_url, '://[^/@]+@', '://', 'g'),        '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "identifier",
  NULLIF(regexp_replace(audience,          '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "audience",
  NULLIF(regexp_replace(contributor,       '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "contributor",
  NULLIF(regexp_replace(created,           '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "created",
  NULLIF(regexp_replace(creator,           '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "creator",
  NULLIF(regexp_replace(description,       '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "description",
  NULLIF(regexp_replace(mime_type,         '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "format",
  NULLIF(regexp_replace(license,           '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "license",
  NULLIF(regexp_replace(publisher,         '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "publisher",
  NULLIF(regexp_replace(dc_references,     '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "references",
  NULLIF(regexp_replace(rights_holder,     '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "rightsHolder",
  NULLIF(regexp_replace(source,            '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "source",
  NULLIF(regexp_replace(title,             '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "title",
  NULLIF(regexp_replace(type,              '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '') AS "type"
FROM (
  SELECT
    i.image_identifier,
    i.audience,
    i.contributor,
    i.created,
    i.creator,
    i.description,
    i.mime_type,
    i.license,
    i.publisher,
    i.dc_references,
    i.rights_holder,
    i.source,
    i.title,
    i.type,
    CASE
      WHEN i.alternate_filename IS NOT NULL THEN
        ARRAY_APPEND(i.alternate_filename, i.original_filename)
      ELSE
        ARRAY[i.original_filename]
    END AS all_urls
  FROM image i
  WHERE i.data_resource_uid = ? AND i.date_deleted IS NULL
) AS subquery,
unnest(subquery.all_urls) AS unnest_url;
'''

    final EXPORT_DATASET_MAPPING_SQL = '''
SELECT
  image_identifier AS "imageID",
  regexp_replace(unnest_url, '://[^/@]+@', '://', 'g') AS "url"
FROM (
  SELECT
    image_identifier,
    CASE
      WHEN alternate_filename IS NOT NULL THEN
        ARRAY_APPEND(alternate_filename, original_filename)
      ELSE
        ARRAY[original_filename]
    END AS all_urls
  FROM
    image
  WHERE
    data_resource_uid = ? AND date_deleted IS NULL
) AS subquery,
unnest(all_urls) AS unnest_url;
    '''

    private static Queue<BackgroundTask> _backgroundQueue = new ConcurrentLinkedQueue<BackgroundTask>()
    private static Queue<BackgroundTask> _tilingQueue = new ConcurrentLinkedQueue<BackgroundTask>()

    private static int BACKGROUND_TASKS_BATCH_SIZE = 100

    @Value('${batch.purge.fetch.size:100}')
    int purgeFetchSize = 100

    @Value('${batch.optimisticLocking.maxRetries:3}')
    int maxLockingRetries = 3

    @Value('${batch.optimisticLocking.maxSleepMs:2000}')
    int maxLockingSleepMs = 2000

    @Value('${batch.optimisticLocking.stripes:250}')
    int optimisticLockingStripes = 250

    @Value('${imageservice.images.maxWidth:-1}')
    int maxImageWidth = -1 // -1 to disable resizing

    Map imagePropertyMap = null

    private Striped<Lock> imageUrlLocks// = Striped.lock(256)
    private Striped<Lock> imageIdLocks// = Striped.lock(256)
    private Striped<Lock> md5HashLocks// = Striped.lock(256)

    @PostConstruct
    def initStripes() {
        imageUrlLocks = Striped.lock(optimisticLockingStripes)
        imageIdLocks = Striped.lock(optimisticLockingStripes)
        md5HashLocks = Striped.lock(optimisticLockingStripes)
    }

    @PostConstruct
    def initImagePropertyMap() {
        if (!imagePropertyMap) {
            def properties = new Image().getProperties().keySet()
            imagePropertyMap = [:]
            properties.each { imagePropertyMap.put(it.toLowerCase(), it) }
        }
    }

    ImageStoreResult storeImage(MultipartFile imageFile, String uploader, Map metadata = [:]) {

        if (imageFile) {
            // Store the image
            def originalFilename = imageFile.originalFilename
//            def bytes = imageFile?.bytes
            def bytes = ByteSource.wrap(imageFile.inputStream.bytes)
            def result = storeImageBytes(bytes, originalFilename, imageFile.size, imageFile.contentType, uploader, false, metadata)
            auditService.log(result.image,"Image stored from multipart file ${originalFilename}", uploader ?: "<unknown>")
            return result
        }
        return null
    }

    def dumpQueueToFile(){
        def fw = new FileWriter(new File("/tmp/backgroundQueue.txt"))
        _backgroundQueue.each {
            fw.write(it.toString() + "\n")
        }
        fw.flush()
        fw.close()
    }


    ImageStoreResult storeImageFromUrl(String imageUrl, String uploader, Map metadata = [:]) {
        if (imageUrl) {
            try {
                def image = Image.byOriginalFileOrAlternateFilename(imageUrl) // findByOriginalFilename(imageUrl)
                if (image && imageStoreService.isImageStored(image)) {
                    scheduleMetadataUpdate(image.imageIdentifier, metadata)
                    return new ImageStoreResult(image, true, image.alternateFilename?.contains(imageUrl) ?: false)
                }
                def uri = new URI(imageUrl)

                def response = downloadService.createHttpResponse(uri, imageUrl)

                def contentType = null

                //detect from dc:mimetype field
                if (metadata.mimeType){
                    try {
                        MimeType mimeType = new MimeTypes().forName(metadata.mimeType)
                        metadata.extension = mimeType.getExtension()
                        contentType = mimeType.toString()
                    } catch (Exception e){
                        log.debug("Un-parseable mime type supplied: " + metadata.mimeType)
                    }
                }

                //detect from dc:format field
                if(contentType == null && metadata.format){
                    try {
                        MimeType mimeType = new MimeTypes().forName(metadata.format)
                        metadata.extension = mimeType.getExtension()
                        contentType = mimeType.toString()
                    } catch (Exception e){
                        log.debug("Un-parseable mime type supplied: " + metadata.format)
                    }
                }

                def urlContentType = response.getContentType()
                if (!contentType && urlContentType) {
                    try {
                        MimeType mimeType = new MimeTypes().forName(urlContentType)
                        metadata.extension = mimeType.getExtension()
                        contentType = mimeType.toString()
                    } catch (Exception e){
                        log.debug("Un-parseable mime type supplied: " + metadata.format)
                    }
                }

                def result
                try (ByteSource byteSource = downloadService.createByteSourceFromHttpResponse(response, metadata.extension, imageUrl)) {
                    //detect from file
                    if (contentType == null){
                        contentType = downloadService.detectMimeType(byteSource, imageUrl)
                    }

                    result = storeImageBytes(byteSource, imageUrl, byteSource.size(), contentType, uploader, true, metadata)
                }

                auditService.log(result.image, "Image downloaded from ${imageUrl}", uploader ?: "<unknown>")
                return result
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex)
            }
        }
        return null
    }


    def getImageUrl(Map<String, String> imageSource){
        if (imageSource.sourceUrl) return imageSource.sourceUrl
        if (imageSource.imageUrl) return imageSource.imageUrl
        imageSource.identifier
    }

    /**
     * Find an existing Image for a supplied URL and optional dataResourceUid.
     */
    private Image findExistingImageByUrlAndDataResourceUid(String imageUrl, String dataResourceUid) {
        Image image = null
        if (!imageUrl) return null

        // Try to resolve via image-service style URL (no need for extra work if this hits)
        image = findImageInImageServiceUrl(imageUrl)

        // Fallbacks for non image-service URLs
        if (!image) {
            if (imageUrl.startsWith("http")) {
                image = Image.byOriginalFileOrAlternateFilename(imageUrl)
            } else {
                image = Image.findByOriginalFilenameAndDataResourceUid(imageUrl, dataResourceUid, [ cache: false ])
            }
        }

        return image
    }

    /**
     * Find an image by discovering an image id from a URL
     * @param url The URL
     * @return The existing image or null if none exists
     */
    Image findImageInImageServiceUrl(String url) {
        def imageId = findImageIdInImageServiceUrl(url)
        return imageId ? Image.findByImageIdentifier(imageId) : null
    }

    private final IMAGE_SERVICE_URL_SUFFIXES = [
            'original',
            'thumbnail',
            'thumbnail_large',
            'thumbnail_xlarge',
            'thumbnail_square',
            'thumbnail_square_black',
            'thumbnail_square_white',
            'thumbnail_square_darkGrey',
            'thumbnail_square_darkGray',
            'thumbnail_centre_crop',
            'thumbnail_centre_crop_large',
    ] as Set

    boolean validateThumbnailType(String thumbnailType) {
        if (thumbnailType.startsWith('thumbnail')) {
            return IMAGE_SERVICE_URL_SUFFIXES.contains(thumbnailType)
        }
        return IMAGE_SERVICE_URL_SUFFIXES.contains('thumbnail' + (thumbnailType ? '_' + thumbnailType : ''))
    }

    String findImageIdInImageServiceUrl(String imageUrl) {
        // is it as image service URL?
        // if so, no need to load the image, use the identifier.....
        def imageID = ''

        if (isImageServiceUrl(imageUrl)){

            def imageHttpUrl = HttpUrl.parse(imageUrl)
            if (imageHttpUrl?.queryParameterNames()?.contains('id')) {
                imageID = imageHttpUrl.queryParameter('id')
            } else if (imageHttpUrl?.queryParameterNames()?.contains('imageId')) {
                imageID = imageHttpUrl.queryParameter('imageId')
            } else if (!imageHttpUrl?.pathSegments()?.empty) {
                imageID = imageHttpUrl.pathSegments().last()
            }
            if (IMAGE_SERVICE_URL_SUFFIXES.contains(imageID) && (imageHttpUrl?.pathSegments()?.size() ?: 0) >= 2) {
                imageID = imageHttpUrl.pathSegments()[-2]
            }
        }
        return imageID
    }

    boolean isImageServiceUrl(String url) {
        def imageServiceUrls = grailsApplication.config.getProperty('imageServiceUrls', List, [])
        boolean isRecognised = imageServiceUrls.any { imageServiceUrl -> url.startsWith(imageServiceUrl) }
        return isRecognised
    }

    /**
     * Batch update supporting bulk updates
     *
     * @param batch
     * @param uploader
     * @return
     */
    Map batchUpdate(List<Map<String, String>> batch, String uploader) {
        def results = [:]
        Image.withNewTransaction {
            sessionFactory.currentSession.setFlushMode(FlushMode.MANUAL)
            try {
                batch.each { imageSource ->

                    def imageUrl = getImageUrl(imageSource) as String
                    if (imageUrl) {
                        Image image = findExistingImageByUrlAndDataResourceUid(imageUrl, imageSource.dataResourceUid)

                        if (!image) {
                            def result = [success: false, alreadyStored: false]
                            try {
                                def uri = new URI(imageUrl)
//                                def bytes = url.getBytes(connectTimeout: connectTimeoutMs, readTimeout: readTimeoutMs, requestProperties: [(USER_AGENT): userAgent()])
                                def contentType
                                ImageStoreResult storeResult
                                try (def bytes = downloadService.createByteSourceFromUrl(uri, null, imageUrl)) {
                                    contentType = downloadService.detectMimeType(bytes, imageUrl)
                                    storeResult = storeImageBytes(bytes, imageUrl, bytes.size(),
                                            contentType, uploader, true, imageSource)
                                }
                                result.imageId = storeResult.image.imageIdentifier
                                result.image = storeResult.image
                                result.success = true
                                result.alreadyStored = storeResult.alreadyStored
                                result.metadataUpdated = false
                            } catch (HttpImageUploadException e) {
                                log.warn("Unable to load image from URL: {}. Logging as failed URL, HTTP status: {}, Message: {}", imageUrl, e.statusCode, e.message)
                                downloadService.logBadUrl(imageUrl, e.statusCode, e.message)
                                result.message = ex.message
                            } catch(Exception ex) {
                                //log to batch update error file
                                if (log.isDebugEnabled()) {
                                    log.error("Problem storing image - " + ex.getMessage(), ex)
                                } else {
                                    log.error("Problem storing image - " + ex.getMessage())

                                }
                                downloadService.logBadUrl(imageUrl, null, ex.message)
                                result.message = ex.message
                            }
                            results[imageUrl] = result
                        } else {

                            def metadataUpdated = false

                            SUPPORTED_UPDATE_FIELDS.each { updateField ->

                                if (image[updateField] != imageSource[updateField]){
                                    image[updateField] = imageSource[updateField]
                                    metadataUpdated = true
                                }
                            }

                            if (metadataUpdated){
                                image.save()
                            }

                            //update metadata if required
                            results[imageUrl] = [success: true,
                                                 imageId: image.imageIdentifier,
                                                 image: image,
                                                 alreadyStored: true,
                                                 metadataUpdated: metadataUpdated
                            ]
                        }
                    }
                }
            } finally {
                sessionFactory.currentSession.flush()
                sessionFactory.currentSession.setFlushMode(FlushMode.AUTO)
            }
        }

        results
    }

    /*
     * Helper methods for uploadImage
     */
    private static final String METRIC_UPLOAD = 'image.upload'

    private Map preLockLookup(String imageUrl, String dataResourceUid) {
        String imageIdForLock = null
        Map result = null
        // Pre-lock lightweight lookup to see if there is already an Image we can lock on
        try {
            imageIdForLock = Image.withTransaction(readOnly: true) {
                Image preExisting = findExistingImageByUrlAndDataResourceUid(imageUrl, dataResourceUid)

                // Fail fast before locking here if this is a bad URL and we have already tried loading it before
                if (!preExisting && downloadService.isBadUrl(imageUrl)) {
                    log.debug("We have already attempted to load {} without success. Skipping.", imageUrl)
                    incrementCounter('image.upload.skipped', 'Images skipped due to bad URL')
                    result = resultFailure(alreadyStored: false)
                }

                return preExisting?.imageIdentifier
            }
        } catch (Throwable t) {
            log.debug("uploadImage: pre-lock lookup failed (will fallback to URL lock) for {}: {}", imageUrl, t.message)
        }
        // Couldn't find a pre-existing image by URL, try to extract imageId from image-service style URL
        // and lock on it anyway
        if (!imageIdForLock && isImageServiceUrl(imageUrl)) {
            imageIdForLock = findImageIdInImageServiceUrl(imageUrl)
        }
        return [imageIdForLock: imageIdForLock, earlyResult: result]
    }

    private Lock acquireLock(String imageIdForLock, String imageUrl) {
        Lock lock
        if (imageIdForLock) {
            log.trace("uploadImage: acquiring lock for imageId: {} (derived from URL: {})", imageIdForLock, imageUrl)
            lock = imageIdLocks.get(imageIdForLock)
        } else {
            log.trace("uploadImage: acquiring lock for imageUrl: {}", imageUrl)
            lock = imageUrlLocks.get(imageUrl)
        }
        log.trace("uploadImage: got lock: {}", lock)
        return lock
    }

    private <T> T withOptimisticRetry(Closure<T> action) {
        withOptimisticRetry("unknown context", action)
    }
    private <T> T withOptimisticRetry(String context, Closure<T> action) {
        withOptimisticRetry(context, null, false, action)
    }

    private <T> T withOptimisticRetry(String context, Supplier<T> failureResultSupplier, Closure<T> action) {
        withOptimisticRetry(context, failureResultSupplier, false, action)
    }

    private <T> T withOptimisticRetry(String context, Supplier<T> failureResultSupplier, boolean allowNullResult, Closure<T> action) {
        context = context ?: ""
        int maxRetries = maxLockingRetries
        int retryCount = 0
        while (retryCount < maxRetries) {
            try {
                return action.call()
            } catch (HibernateOptimisticLockingFailureException | StaleStateException ex) {
                retryCount++
                if (retryCount >= maxRetries) {
                    log.error("Failed {} after {} retries due to optimistic locking failure", context, maxRetries)
                    if (failureResultSupplier) {
                        def result = failureResultSupplier.get()
                        if (result != null) return result
                        if (allowNullResult) return null
                    }
                    throw ex
                } else {
                    def sleepTime = RandomUtils.nextInt(0, maxLockingSleepMs)
                    log.warn("Optimistic locking failure {}, retry {}/{}", context, retryCount, maxRetries)
                    log.trace("Sleeping for {} ms before retrying", sleepTime)
                    Thread.sleep(sleepTime) // Brief exponential backoff
                }
            }
        }
        return null
    }

    private Map updateExistingImage(String imageUrl, Map imageSource) {
        return Image.withTransaction { TransactionStatus status ->
            log.trace("uploadImage: inside Image.withTransaction for imageUrl: {}", imageUrl)
            // TODO should I use the imageIdForLock to load the image directly here instead of searching again?
            def image = findExistingImageByUrlAndDataResourceUid(imageUrl, imageSource.dataResourceUid)
            log.trace("uploadImage: after DB lookup, image: {} for imageUrl: {}", image, imageUrl)

            if (image) {
                log.trace("uploadImage: found existing image: {} for imageUrl: {}", image, imageUrl)
                def metadataUpdated = applySupportedMetadata(image, imageSource)

                log.trace("uploadImage: metadataUpdated={} for imageUrl: {}", metadataUpdated, imageUrl)
                if (metadataUpdated){
                    if (image.isDirty()) {
                        image.save()
                        status.flush()
                    } else {
                        log.trace("uploadImage: metadataUpdated but image not dirty, no save required for imageUrl: {}", imageUrl)
                    }
                } else {
                    status.setRollbackOnly()
                }
                log.trace("uploadImage: completed metadata update for imageUrl: {}", imageUrl)

                incrementCounter('image.upload.duplicate', 'Duplicate image uploads')
                //update metadata if required
                log.trace("uploadImage: returning existing image for imageUrl: {}", imageUrl)
                return [success: true,
                        imageId: image.imageIdentifier,
                        image: image,
                        alreadyStored: true,
                        metadataUpdated: metadataUpdated,
                        isDuplicate: image.alternateFilename?.contains(imageUrl) ?: false
                ]
            }
            log.trace("uploadImage: no existing image found for imageUrl: {}", imageUrl)
            return null // new image
        }
    }

    private boolean applySupportedMetadata(Image image, Map imageSource) {
        def metadataUpdated = false

        SUPPORTED_UPDATE_FIELDS.each { updateField ->
            def propertyName = hasImageCaseFriendlyProperty(image, updateField)
            if (propertyName) {
                if (image[propertyName] != imageSource[updateField]){
                    log.trace("uploadImage: updating field: {} from value: {} to value: {} for image: {}", propertyName, imageSource[updateField], image.imageIdentifier)
                    image[propertyName] = imageSource[updateField]
                    metadataUpdated = true
                }
            } else {
                log.warn("uploadImage: image does not have property: {} for image: {}", updateField, image.imageIdentifier)
            }
        }
        return metadataUpdated
    }

    private Map downloadAndStore(String imageUrl, String uploader, Map imageSource) {
        def result = null
        def bytes
        try {
            log.trace("uploadImage: loading image bytes from URL: {}", imageUrl)
            def uri = new URI(imageUrl)
            bytes = downloadService.createByteSourceFromUrl(uri, null, imageUrl)
            log.trace("uploadImage: loaded image bytes from URL: {}", imageUrl)
        } catch (Exception e) {
            log.error("Unable to load image from URL: {}. Logging as failed URL, Exception: {}", imageUrl, e.message)
            if (e instanceof HttpImageUploadException) {
                downloadService.logBadUrl(imageUrl, e.statusCode, e.message)
            } else {
                downloadService.logBadUrl(imageUrl, null, e.message)
            }
            result = resultFailure(alreadyStored: false)
        }
        if (!result) {
            try {
                log.trace("uploadImage: detecting content type for URL: {}", imageUrl)
                def contentType = downloadService.detectMimeType(bytes, imageUrl)
                log.trace("uploadImage: storing image bytes for URL: {} with content type {}", imageUrl, contentType)
                ImageStoreResult storeResult = storeImageBytes(bytes, imageUrl, bytes.size(),
                        contentType, uploader, true, imageSource)
                log.trace("uploadImage: stored image bytes for URL: {}", imageUrl)
                result = resultSuccess(storeResult, false)
            } catch (Exception ex) {
                //log to batch update error file
                log.error("Problem storing image - ", ex)
                result = resultFailure(message: ex.message)
                recordError('uploadImage', [error: ex.class.simpleName])
            } finally {
                bytes?.close()
            }
        }
        return result
    }

    private Map resultSuccess(ImageStoreResult r, boolean metadataUpdated=false) {
        def result = [success: true,
                      imageId: r.image.imageIdentifier,
                      image: r.image,
                      alreadyStored: r.alreadyStored,
                      isDuplicate: r.isDuplicate,
                      metadataUpdated: metadataUpdated
        ]
        return result
    }

    @NamedVariant
    private Map resultFailure(String message = null, Boolean alreadyStored = null) {
        def result = [success: false]
        if (message) {
            result.message = message
        }
        if (alreadyStored != null) {
            result.alreadyStored = alreadyStored
        }
        return result
    }

    @NotTransactional // transactions managed in method
    Map uploadImage(Map imageSource, String uploader) {
        log.trace("uploadImage: called with imageSource: {}", imageSource)
        return recordTime(METRIC_UPLOAD, 'Time to upload and process an image') {
            try {
                def imageUrl = getImageUrl(imageSource) as String
                if (!imageUrl) return resultFailure()

                def pre = preLockLookup(imageUrl, imageSource.dataResourceUid) // returns (imageIdForLock, earlyResult?)
                if (pre.earlyResult) return pre.earlyResult

                def result = null
                Lock lock = acquireLock(pre.imageIdForLock, imageUrl)
                lock.lock()
                try {
                    result = withOptimisticRetry("updating metadata for $imageUrl") {
                        updateExistingImage(imageUrl, imageSource)
                    }
                    if (!result) {
                        // New image, download and store
                        result = downloadAndStore(imageUrl, uploader, imageSource)
                    }
                } finally {
                    lock.unlock()
                }
                if (result?.success) {
                    log.trace("uploadImage: successful upload for imageUrl: {}", imageUrl)
                    recordSuccess('uploadImage', [alreadyStored: result.alreadyStored?.toString() ?: 'false'])
                }
                log.trace("uploadImage: returning result for imageUrl: {}: {}", imageUrl, result)
                return result
            } catch (Exception e) {
                recordError('uploadImage', [error: e.class.simpleName])
                throw e
            }
        }
    }

    int getImageTaskQueueLength() {
        return _backgroundQueue.size()
    }

    int getTilingTaskQueueLength() {
        return _tilingQueue.size()
    }

    def clearImageTaskQueue(){
        return _backgroundQueue.clear()
    }

    def clearTilingTaskQueueLength() {
        return _tilingQueue.clear()
    }

    @Synchronized
    def updateMetadata(String imageId, Map metadata) {

        def image = Image.findByImageIdentifier(imageId, [ cache: true])

        if (image) {
            boolean toSave = false
            def toUpdate = [:]
            metadata.each { kvp ->

                if (image.hasProperty(kvp.key) && kvp.value) {
                    if (!(kvp.key in ["dateTaken", "dateUploaded", "id"])) {
                        if (image[kvp.key] != kvp.value) {
                            toUpdate[kvp.key] = kvp.value
                            toSave = true
                        }
                    }
                }
            }
            if (toSave) {
                //this has been changed to use executeUpdate to avoid
                // StaleStateExceptions which are thrown due to
                // this method being called on the same image multiple times
                // by multiple threads.
                def query  = toUpdate.keySet().collect{"${it}=:${it}" }.join(", ")
                def fullQuery = "update Image i set " + query +
                        " where i.imageIdentifier =:imageIdentifier"
                toUpdate['imageIdentifier'] = imageId
                Image.executeUpdate(fullQuery, toUpdate)
            }
        }
    }


    /**
     * Store the bytes for an image.
     *
     * @param bytes Should be a byte source that is relatively cheap to open as it will be opened multiple times
     * @param originalFilename
     * @param filesize
     * @param contentType
     * @param uploaderId
     * @param createDuplicates
     * @param metadata
     * @return
     */
    @NotTransactional // transactions managed in method
    ImageStoreResult storeImageBytes(ByteSource bytes, String originalFilename, long filesize, String contentType,
                                     String uploaderId, boolean createDuplicates, Map metadata = [:]) {
        log.trace("storeImageBytes: called with originalFilename: {}, filesize: {}, contentType: {}, uploaderId: {}, createDuplicates: {}, metadata: {}", originalFilename, filesize, contentType, uploaderId, createDuplicates, metadata)
        ImageStoreResult result

        def md5Hash = bytes.hash(Hashing.md5()).asBytes().encodeAsHex() // DigestUtils.digest(DigestUtils.getDigest('MD5'), bytes.openStream())
        log.trace("storeImageBytes: calculated MD5 hash: {} for originalFilename: {}", md5Hash, originalFilename)

        // Use striped lock to prevent concurrent processing of the same md5Hash
        def lock = md5HashLocks.get(md5Hash)
        lock.lock()
        try {
            if (maxImageWidth != -1) {
                bytes = resizeImageIfNeeded(bytes.openStream().bytes, contentType, maxImageWidth) as ByteSource
                filesize = (long) bytes.size()
                md5Hash = bytes.hash(Hashing.md5()).asBytes().encodeAsHex()
            }

            //check for existing image using MD5 hash
            def image = Image.findByContentMD5Hash(md5Hash)
            def preExisting = false
            def isDuplicate = false
            // On final retry failure, just return that it's a duplicate without updating
            // Needs a new session because we're outside the previous transaction
            // which feels a bit wrong but avoids further complications
            def failureResultSupplier = { ->
                def image = Image.withSession { Image.findByContentMD5Hash(md5Hash) }
                image ? new ImageStoreResult(image, true, true) : null
            } as Supplier<ImageStoreResult>
            //check for existing image using MD5 hash with retry logic for optimistic locking
            result = withOptimisticRetry("updating image $originalFilename", failureResultSupplier) {
                def transactionResult = Image.withNewSession { Image.withNewTransaction { TransactionStatus status ->
                    log.trace("storeImageBytes: inside Image.withTransaction for originalFilename: {}", originalFilename)
                    def image = recordTime("image.lookup.md5", "Time to lookup image by MD5 hash") {
                        Image.findByContentMD5Hash(md5Hash, [ cache: false ])?.refresh()
                    }
                    log.trace("storeImageBytes: lookup by MD5 hash returned image: {} for originalFilename: {} ", image, originalFilename)

                    preExisting = image != null
                    if (image) {
                        boolean updated = false
                        if (image.dateDeleted) {
                            log.warn("Deleted Image ${image.originalFilename} has been re-uploaded.  Will undelete.")
                            image.dateDeleted = null //reset date deleted if image resubmitted...
                            updated = true
                        }
                        if (createDuplicates && image.originalFilename != originalFilename) {
                            log.info("Existing image found at different URL ${image.originalFilename} to ${originalFilename}. Will add duplicate.")

                            // we have seen this image before, but the URL has changed at source
                            // so lets update it so that subsequent loads dont need
                            // to re-download this image
                            if (image.alternateFilename == null) {
                                image.alternateFilename = []
                                updated = true
                            }
                            if (!image.alternateFilename.contains(originalFilename)) {
                                image.alternateFilename += originalFilename
                                updated = true
                            }
                            isDuplicate = true
                        } else if (!createDuplicates && image.originalFilename != originalFilename) {
                            log.warn("Got a pre-existing image to store {} but it already exists at {}", originalFilename, image.imageIdentifier)
                        } else {
                            log.info("Image already exists for ${originalFilename}.")
                        }

                        try {
                            if (updated) {
                                image = image.save(failOnError: true)
                                status.flush()
                                log.trace("storeImageBytes: updated existing image record for originalFilename: {}", originalFilename)
                            }
                        } catch (Exception ex) {
                            if (ex instanceof HibernateOptimisticLockingFailureException || ex instanceof StaleStateException) {
                                throw ex //rethrow to be handled by outer retry logic
                            } else {
                                // other exception saving image
                                log.error("Problem updating image {}  -", originalFilename, ex)
                            }
                        }

                        return new ImageStoreResult(image, preExisting, isDuplicate)
                    } else {
                        // no existing image found,
                        return null
                    }
                } }// transaction -> session
                log.trace("storeImageBytes: completed Image.withTransaction for originalFilename: {} with result {}", originalFilename, transactionResult)
                return transactionResult
            }

            if (!result) {
                def defaultStorages = storageLocationService.getDefaultStorageOperationsId()
                log.trace("storeImageBytes: obtained StorageLocations: {} for originalFilename: {}", defaultStorages, originalFilename)

                if (!defaultStorages) {
                    throw new IllegalStateException("No default storage locations configured - cannot store image")
                }
                def defaultStorage = defaultStorages[0]

                log.trace("storeImageBytes: storing new image for originalFilename: {}", originalFilename)
                def sha1Hash = bytes.hash(Hashing.sha1()).asBytes().encodeAsHex() //DigestUtils.digest(DigestUtils.getDigest('SHA-1'), bytes.openStream()).encodeAsHex()
                log.trace("storeImageBytes: calculated SHA1 hash: {} for originalFilename: {}", sha1Hash, originalFilename)

                def imgDesc = imageStoreService.storeImage(bytes, defaultStorage.operations, contentType, originalFilename)
                log.trace("storeImageBytes: imageStoreService.storeImage returned imgDesc: {} for originalFilename: {}", imgDesc, originalFilename)

                // Create the image record, and set the various attributes
                Image image = new Image(
                        imageIdentifier: imgDesc.imageIdentifier,
                        contentMD5Hash: md5Hash,
                        contentSHA1Hash: sha1Hash,
                        uploader: uploaderId
                )

                if (metadata.extension) {
                    image.extension = metadata.extension
                } else {
                    // this is known to be problematic
                    def extension = 'jpg'
                    def filename = ImageUtils.getFilename(originalFilename)
                    if (filename) {
                        extension = FilenameUtils.getExtension(filename) ?: 'jpg'
                    }
                    log.trace("storeImageBytes: determined extension: {} for originalFilename: {}", extension, originalFilename)
                    image.extension = extension
                }

                image.height = imgDesc.height
                image.width = imgDesc.width
                image.zoomLevels = imgDesc.zoomLevels
                image.fileSize = bytes.size()
                image.mimeType = contentType
                image.dateUploaded = new Date()
                image.originalFilename = originalFilename
                image.dateTaken = contentType.toLowerCase().startsWith('image') ? getImageTakenDate(bytes, originalFilename) ?: image.dateUploaded : image.dateUploaded

                //update metadata stored in the `image` table
                setMetadataOnImage(metadata, image)

                result = Image.withTransaction {
                    for (storage in defaultStorages) {
                        storage.applyToImage(image)
                    }
                    try {
                        log.trace("storeImageBytes: saving new image record for originalFilename: {}", originalFilename)
                        //try to match licence
                        updateLicence(image)
                        image = image.save(failOnError: true)
                        log.trace("storeImageBytes: saved new image record ({}:{}) for originalFilename: {}", image.id, image.imageIdentifier, originalFilename)
                    } catch (Exception ex) {
                        log.error("Problem saving image {}  -", originalFilename, ex)
                    }
                    return new ImageStoreResult(image, preExisting, isDuplicate)
                }
            }
        } catch (Exception ex) {
            log.error("Problem storing image ${originalFilename} - " + ex.getMessage())
        } finally {
            lock.unlock()
        }
        log.trace("storeImageBytes: returning result: {} for originalFilename: {}", result, originalFilename)
        return result
    }

    private Map<Object, Object> setMetadataOnImage(Map metadata, image) {
        metadata.each { kvp ->
            def propertyName = hasImageCaseFriendlyProperty(image, kvp.key)
            if (propertyName && kvp.value) {
                if (!(propertyName in ["dateTaken", "dateUploaded", "id"])) {
                    image[propertyName] = kvp.value
                }
            }
        }
    }

    def hasImageCaseFriendlyProperty(Image image, String propertyName) {
        imagePropertyMap.get(propertyName.toLowerCase())
    }

    def schedulePostIngestTasks(Long imageId, String identifier, String fileName, String uploaderId){
//        scheduleArtifactGeneration(imageId, uploaderId)
        scheduleImageIndex(imageId)
        scheduleImageMetadataPersist(imageId,identifier, fileName,  MetaDataSourceType.Embedded, uploaderId)
    }

    def scheduleNonImagePostIngestTasks(Long imageId){
        scheduleImageIndex(imageId)
    }

    Map getMetadataItemValuesForImages(List<Image> images, String key, MetaDataSourceType source = MetaDataSourceType.SystemDefined) {
        if (!images || !key) {
            return [:]
        }
        def results = ImageMetaDataItem.executeQuery("select md.value, md.image.id from ImageMetaDataItem md where md.image in (:images) and lower(name) = :key and source=:source", [images: images, key: key.toLowerCase(), source: source])
        def fr = [:]
        results.each {
            fr[it[1]] = it[0]
        }
        return fr
    }

    Map getAllUrls(String imageIdentifier) {
        return imageStoreService.getAllUrls(imageIdentifier)
    }

    String getImageUrl(String imageIdentifier) {
        return imageStoreService.getImageUrl(imageIdentifier)
    }

    String getImageThumbUrl(String imageIdentifier) {
        return imageStoreService.getImageThumbUrl(imageIdentifier)
    }

    String getImageThumbLargeUrl(String imageIdentifier) {
        return imageStoreService.getImageThumbLargeUrl(imageIdentifier)
    }

    String getImageThumbXLargeUrl(String imageIdentifier) {
        return imageStoreService.getImageThumbXLargeUrl(imageIdentifier)
    }

    String getImageCentreCropLargeThumbUrl(String imageIdentifier) {
        return imageStoreService.getImageThumbCentreCropLargeUrl(imageIdentifier)
    }

    String getImageCentreCropThumbUrl(String imageIdentifier) {
        return imageStoreService.getImageThumbCentreCropUrl(imageIdentifier)
    }

    String getImageSquareThumbUrl(String imageIdentifier, String backgroundColor = null) {
        return imageStoreService.getImageSquareThumbUrl(imageIdentifier, backgroundColor)
    }

    List<String> getAllThumbnailUrls(String imageIdentifier) {
        def results = []
        def image = Image.findByImageIdentifier(imageIdentifier, [ cache: true])
        if (image) { // TODO we should be able to give all thumbnail URLs here as they are generated on demand.
            def thumbs = ImageThumbnail.findAllByImage(image)
            thumbs?.each { thumb ->
                results << imageStoreService.getThumbUrlByName(imageIdentifier, thumb.name)
            }
        }
        results
    }

    String getImageTilesUrlPattern(String imageIdentifier) {
        return imageStoreService.getImageTilesUrlPattern(imageIdentifier)
    }

    Image updateLicence(Image image){
        if (image.license){

            def license = License.findByAcronymOrNameOrUrlOrImageUrl(image.license,image.license,image.license,image.license)
            if (license){
                image.recognisedLicense = license
            } else {
                def licenceMapping = LicenseMapping.findByValue(image.license)
                if (licenceMapping){
                    image.recognisedLicense = licenceMapping.license
                } else {
                    image.recognisedLicense = null
                }
            }
        } else {
            image.recognisedLicense = null
        }
        image
    }

    //this is slow on large tables
    def updateLicences(){
        log.info("Updating license mapping for all images")
        def licenseMapping = LicenseMapping.findAll()
        licenseMapping.each {
            log.info("Updating license mapping for string matching: " + it.value)
            Image.executeUpdate("Update Image i set i.recognisedLicense = :recognisedLicense " +
                    " where " +
                    " i.license = :license" +
                    "", [recognisedLicense: it.license, license: it.value])
        }

        def licenses = License.findAll()
        log.info("Updating licenses  for all images - using acronym, name and url")
        licenses.each  { license ->
            [license.url, license.name, license.acronym].each { licenceValue ->
                Image.executeUpdate("Update Image i set i.recognisedLicense = :recognisedLicense " +
                        " where " +
                        " i.license = :license" +
                        "", [recognisedLicense: license, license: licenceValue])
            }
        }
        log.info("Licence refresh complete")
    }

    private static Date getImageTakenDate(ByteSource bytes, String originalFilename) {
        log.trace('getImageTakenDate {}', originalFilename)
        def filename = ImageUtils.getFilename(originalFilename)
        log.debug("getImageTakenDate {} got filename {}", originalFilename, filename)
        try {
            ImageMetadata metadata = Imaging.getMetadata(bytes.openStream(), filename)
            log.trace('getImageTakenDate {} got metadata ', filename)
            if (metadata && metadata instanceof JpegImageMetadata) {
                JpegImageMetadata jpegMetadata = metadata

                def date = getImageTagValue(jpegMetadata, ExifTagConstants.EXIF_TAG_DATE_TIME_ORIGINAL)
                log.trace('getImageTakenDate {} got image tag value {}', filename, date)
                if (date) {
                    def sdf = new SimpleDateFormat("yyyy:MM:dd hh:mm:ss")
                    return sdf.parse(date.toString())
                }
            } else {
                log.trace('getImageTakenDate {} found metadata of type {}', filename, metadata?.class?.name)
            }
        } catch (Exception ex) {
            log.trace('getImageTakenDate {} error', filename, ex)
            return null
        }
    }

    private static Object getImageTagValue(JpegImageMetadata jpegMetadata, TagInfo tagInfo) {
        log.trace('getImageTagValue {}', tagInfo)
        TiffField field = jpegMetadata.findExifValue(tagInfo)
        if (field) {
            log.trace('getImageTagValue {}, found field {}', tagInfo, field)
            return field.value
        } else {
            log.trace('getImageTagValue {}, no field found', tagInfo)
        }
        return null
    }

    static Map<String, Object> getImageMetadataFromBytes(byte[] bytes, String filename) {
        def extractor = new MetadataExtractor()
        return extractor.readMetadata(bytes, filename)
    }

    static Map<String, Object> getImageMetadataFromBytes(InputStream inputStream, String filename) {
        def extractor = new MetadataExtractor()
        return extractor.readMetadata(inputStream, filename)
    }

    def scheduleArtifactGeneration(long imageId, String userId) {
        scheduleBackgroundTask(new ImageBackgroundTask(imageId, this, ImageTaskType.Thumbnail, userId))
        _tilingQueue.add(new ImageBackgroundTask(imageId, this, ImageTaskType.TMSTile, userId))
    }

    def scheduleThumbnailGeneration(long imageId, String userId) {
        scheduleBackgroundTask(new ImageBackgroundTask(imageId, this, ImageTaskType.Thumbnail, userId))
    }

    def scheduleTileGeneration(long imageId, String userId) {
        _tilingQueue.add(new ImageBackgroundTask(imageId, this, ImageTaskType.TMSTile, userId))
    }

    def scheduleKeywordRebuild(long imageId, String userId) {
        scheduleBackgroundTask(new ImageBackgroundTask(imageId, this, ImageTaskType.KeywordRebuild, userId))
    }

    def scheduleImageDeletion(long imageId, String userId) {
        scheduleBackgroundTask(new ImageBackgroundTask(imageId, this, ImageTaskType.Delete, userId))
    }

    def scheduleMetadataUpdate(String imageIdentifier, Map metadata) {
        scheduleBackgroundTask(new ImageMetadataUpdateBackgroundTask(imageIdentifier, metadata, imageService))
    }

    def scheduleImageIndex(long imageId) {
        scheduleBackgroundTask(new IndexImageBackgroundTask(imageId, elasticSearchService))
    }

    def scheduleImageMetadataPersist(long imageId, String imageIdentifier, String fileName,  MetaDataSourceType metaDataSourceType, String uploaderId){
        scheduleBackgroundTask(new ImageMetadataPersistBackgroundTask(imageId, imageIdentifier, fileName, metaDataSourceType, uploaderId, imageService, imageStoreService))
    }

    def scheduleBackgroundTask(BackgroundTask task) {
        _backgroundQueue.add(task)
    }

    def schedulePollInbox(String userId) {
        def task = new PollInboxBackgroundTask(this, userId)
        scheduleBackgroundTask(task)
        return task.batchId
    }

    private volatile ForkJoinPool pool
    private final def $lock = new Object[0]

    private def getBackgroundTasksPool(int batchThreads) {
        if (!pool) {
            synchronized ($lock) {
                if (!pool) {
                    pool = new ForkJoinPool(
                            batchThreads,
                            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                            uncaughtExceptionHandler,
                            false
                    )
                }
            }
        }
        return pool
    }

    @PreDestroy
    def shutdownPool() {
        if (pool) {
            pool.shutdown()
        }
    }

    void processBackgroundTasks() {
        int taskCount = 0
        BackgroundTask task = null

        Integer batchThreads = settingService.getBackgroundTasksThreads()

        def theseTasks = new ArrayList<Closure<?>>(BACKGROUND_TASKS_BATCH_SIZE)

        while (taskCount < BACKGROUND_TASKS_BATCH_SIZE && (task = _backgroundQueue.poll()) != null) {
            if (task) {
                theseTasks.add(task.&doExecute)
                taskCount++
            }
        }

        try {
            GParsPool.withExistingPool(getBackgroundTasksPool(batchThreads)) {
                GParsPool.executeAsyncAndWait(theseTasks)
            }
        } catch (AsyncException e) {
            log.error("Errors executing background tasks ({})", e.concurrentExceptions.size())
            e.concurrentExceptions.each {
                log.error("Background task error", StackTraceUtils.sanitize(it))
            }
        } catch (Exception e) {
            log.error("Exception executing background tasks in batch", e)
        }
    }

    private final static Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        void uncaughtException(Thread t, Throwable e) {
            ImageService.log.error("Error processing background thread ${t.name}:", e)
        }
    }


    void processTileBackgroundTasks() {
        int taskCount = 0
        BackgroundTask task = null
        while (taskCount < BACKGROUND_TASKS_BATCH_SIZE && (task = _tilingQueue.poll()) != null) {
            if (task) {
                task.doExecute()
                taskCount++
            }
        }
    }

    boolean isImageType(Image image) {
        return image.mimeType?.toLowerCase()?.startsWith("image/")
    }

    boolean isAudioType(Image image) {
        return image.mimeType?.toLowerCase()?.startsWith("audio/")
    }

    List<ThumbnailingResult> generateImageThumbnails(Image image) {
        List<ThumbnailingResult> results
        if (isAudioType(image)) {
            results = imageStoreService.generateAudioThumbnails(image)
        } else if (isImageType(image)) {
            results = imageStoreService.generateImageThumbnails(image)
        } else {
            results = imageStoreService.generateDocumentThumbnails(image)
        }

        // These are deprecated, but we'll update them anyway...
        if (results) {
            def defThumb = results.find { it.thumbnailName.equalsIgnoreCase("thumbnail")}
            image.thumbWidth = defThumb?.width ?: 0
            image.thumbHeight = defThumb?.height ?: 0
            image.squareThumbSize = results.find({ it.thumbnailName.equalsIgnoreCase("thumbnail_square")})?.width ?: 0
        }
        results?.each { th ->
            def imageThumb = ImageThumbnail.findByImageAndName(image, th.thumbnailName)
            if (imageThumb) {
                imageThumb.height = th.height
                imageThumb.width = th.width
                imageThumb.isSquare = th.square
            } else {
                imageThumb = new ImageThumbnail(image: image, name: th.thumbnailName, height: th.height, width: th.width, isSquare: th.square)
                imageThumb.save(flush:true)
            }
        }
    }

    void generateTMSTiles(String imageIdentifier) {
        imageStoreService.generateTMSTiles(imageIdentifier)
    }

    def deleteImage(Image image, String userId) {

        if (image) {

            deleteRelatedArtefacts(image)

            // Delete from the index...
            elasticSearchService.deleteImage(image)

            //soft deletes
            image.dateDeleted = new Date()
            image.save(flush: true, failonerror: true)

            auditService.log(image?.imageIdentifier, "Image deleted", userId)

            return true
        }

        return false
    }

    private def deleteRelatedArtefacts(Image i){

        // delete metadata
        def metadata = ImageMetaDataItem.where {
            image == i
        }.deleteAll()
        log.debug("Deleted $metadata ImageMetaDataItems for $i")

        def outSourcedJobs = OutsourcedJob.where {
            image == i
        }.deleteAll()
        log.debug("Deleted $outSourcedJobs OutsourcedJobs for $i")

        // need to delete it from user selections
        def selected = SelectedImage.where {
            image == i
        }.deleteAll()
        log.debug("Deleted $selected SelectedImages for $i")

        // Need to delete tags
        def tags = ImageTag.where {
            image == i
        }.deleteAll()
        log.debug("Deleted $tags ImageTags for $i")

        // Delete keywords
        def keywords = ImageKeyword.where {
            image == i
        }.deleteAll()
        log.debug("Deleted $keywords ImageKeywords for $i")

        // If this image is a subimage, also need to delete any subimage rectangle records
        def subimagesRef = Subimage.where {
            subimage == i
        }.deleteAll()
        log.debug("Deleted $subimagesRef Subimages for $i")

        // This image may also be a parent image
        def subimages = Subimage.findAllByParentImage(i)
        subimages.each { subimage ->
            // need to detach this image from the child images, but we do not actually delete the sub images. They
            // will live on as root images in their own right
            subimage.subimage.parent = null
            subimage.subimage.save()
            subimage.delete()
        }

        // check for images that have this image as the parent and detach it
        def children = Image.findAllByParent(i)
        children.each { Image child ->
            child.parent = null
            child.save()
        }

        // thumbnail records...
        def thumbs = ImageThumbnail.where {
            image == i
        }.deleteAll()
        log.debug("Deleted $thumbs ImageThumbnails for $i")
    }

    @Transactional
    def purgeAllDeletedImages() {
        /* Can't unit test this because:
        - A regular GORM unit test doesn't work with criteria.scroll
        - A HibernateSpec unit test can't confirm that the records are deleted due to the way
          it wraps a test in a transaction
         */
        try {
            Image.withSession { session ->
                log.info("Purge All Deleted Images starting")
                HibernateCriteriaBuilder c = Image.createCriteria()

                // https://github.com/grails/grails-data-mapping/issues/714
                // Manually run the criteria for GORM so that it actually causes Postgres to scroll
                def criteria = c.buildCriteria {
                    isNotNull('dateDeleted')
                }
                criteria.flushMode = FlushMode.MANUAL
                criteria.cacheable = false
                criteria.fetchSize = purgeFetchSize
                def results = criteria.scroll(ScrollMode.FORWARD_ONLY)

                def count = 0
                while(results.next()) {
                    Image image = ((Image)results.get()[0])
                    deleteImagePurge(image)
                    count++
                    if (count % purgeFetchSize == 0) {
                        log.info("Purge All Deleted Images deleted ${count} images")
                        session.flush()
                        session.clear() // The session will accrete a number of AuditMessages which we don't want to hang on to a reference to for a large delete
                        log.debug("Purge All Deleted Images flushed session")
                    }
                }
                session.flush()
                log.info("Purge All Deleted Images completed deleting ${count} images")
            }
        } catch (e) {
            log.error("Exception while purging images", e)
        }
    }

    def deleteImagePurge(Image image) {
        if (image && image.dateDeleted) {
            deleteRelatedArtefacts(image)
            if (!imageStoreService.deleteStored(image)) {
                log.warn("Unable to delete stored data for ${image.imageIdentifier}")
            }
            // Remove from storage location
//            image.storageLocation.removeFromImages(image)
            //hard delete
            image.delete()
            return true
        }
        return false
    }

    List<File> listStagedImages() {
        def files = []
        def inboxLocation = grailsApplication.config.getProperty('imageservice.imagestore.inbox') as String
        def inboxDirectory = new File(inboxLocation)
        inboxDirectory.eachFile { File file ->
            files << file
        }
        return files
    }

    Image importFileFromInbox(File file, String batchId, String userId) {

        if (!file || !file.exists()) {
            throw new RuntimeException("Could not read file ${file?.absolutePath} - Does not exist")
        }

        Image image = null

        Image.withNewTransaction {

            def fieldDefinitions = ImportFieldDefinition.list()

            // Create the image domain object
            def bytes = Files.asByteSource(file)
            def mimeType = detectMimeType(bytes, file.name)
            image = storeImageBytes(bytes, file.name, file.length(),mimeType, userId, false).image

            auditService.log(image, "Imported from ${file.absolutePath}", userId)

            if (image && batchId) {
                setMetaDataItem(image, MetaDataSourceType.SystemDefined,  "importBatchId", batchId)
            }

            // Is there any extra data to be applied to this image?
            if (fieldDefinitions) {
                fieldDefinitions.each { fieldDef ->
                    setMetaDataItem(image, MetaDataSourceType.SystemDefined, fieldDef.fieldName, ImportFieldValueExtractor.extractValue(fieldDef, file))
                }
            }
//            generateImageThumbnails(image)

            image.save(flush: true, failOnError: true)
        }

        // If we get here, and the image is not null, it means it has been committed to the database and we can remove the file from the inbox
        if (image) {
            if (!FileUtils.deleteQuietly(file)) {
                file.deleteOnExit()
            }
            // schedule an index
            scheduleImageIndex(image.id)
            // also we should do the thumb generation (we'll defer tiles until after the load, as it will slow everything down)
//            scheduleTileGeneration(image.id, userId)
        }
        return image
    }

    def pollInbox(String batchId, String userId) {
        def inboxLocation = grailsApplication.config.getProperty('imageservice.imagestore.inbox') as String
        def inboxDirectory = new File(inboxLocation)

        inboxDirectory.eachFile { File file ->
            _backgroundQueue.add(new ImportFileBackgroundTask(file, this, batchId, userId))
        }
    }

    private static String sanitizeString(Object value) {
        if (value) {
            value = value.toString()
        } else {
            return ""
        }

        def bytes = value?.getBytes("utf8")

        def hasZeros = bytes.contains(0)
        if (hasZeros) {
            return Base64.encodeBase64String(bytes)
        } else {
            return StringUtils.trimToEmpty(value)
        }
    }

    def updateImageMetadata(Image image, Map metadata){
        scheduleMetadataUpdate(image.imageIdentifier, metadata)
//        imageService.updateMetadata(image.imageIdentifier, metadata)
    }

    def setMetaDataItem(Image image, MetaDataSourceType source, String key, String value, String userId = "<unknown") {

        value = sanitizeString(value)
        if (image && image.id && StringUtils.isNotEmpty(key?.trim())) {

            if (value.length() > 8000) {
                auditService.log(image, "Cannot set metdata item '${key}' because value is too big! First 25 bytes=${value.take(25)}", userId)
                return false
            }

            // See if we already have an existing item...
            def existing = ImageMetaDataItem.findByImageAndNameAndSource(image, key, source)
            if (existing) {
                existing.value = value
                existing.save()
            } else {
                if (value){
                    image.addToMetadata(new ImageMetaDataItem(image: image, name: key, value: value, source: source)).save()
                }
            }
            return true
        } else {
            log.debug("Not Setting metadata item! Image ${image?.id} key: ${key} value: ${value}")
        }

        return false
    }

    def setMetadataItemsByImageId(Long imageId, Map<String, String> metadata, MetaDataSourceType source, String userId) {
        def image = Image.get(imageId)
        if (image) {
            return setMetadataItems(image, metadata, source, userId)
        }
        return false
    }

    @Transactional
    def setMetadataItems(Image image, Map<String, Object> metadata, MetaDataSourceType source, String userId) {
        if (!userId) {
            userId = "<unknown>"
        }
        metadata.each { kvp ->
            def value = sanitizeString(kvp.value?.toString())
            def key = kvp.key
            if (image && StringUtils.isNotEmpty(key?.trim())) {

                if (value.length() > 8000) {
                    auditService.log(image, "Cannot set metdata item '${key}' because value is too big! First 25 bytes=${value.take(25)}", userId)
                    return false
                }

                // See if we already have an existing item...
                def existing = ImageMetaDataItem.findByImageAndNameAndSource(image, key, source)
                if (existing) {
                    existing.value = value
                } else {
//                    log.info("Storing metadata: ${image.title}, name: ${key}, value: ${value}, source: ${source}")
                    if(key && value) {
                        def md = new ImageMetaDataItem(image: image, name: key, value: value, source: source)
                        md.save(failOnError: true)
                        image.addToMetadata(md)
                    }
                }

                auditService.log(image, "Metadata item ${key} set to '${value?.take(25)}' (truncated) (${source})", userId)
            } else {
                log.debug("Not Setting metadata item! Image ${image?.id} key: ${key} value: ${value}")
            }
        }
        image.save()
        return true
    }

    @Transactional
    def removeMetaDataItem(Image image, String key, MetaDataSourceType source, String userId="<unknown>") {
        def count = 0
        def items = ImageMetaDataItem.findAllByImageAndNameAndSource(image, key, source)
        if (items) {
            items.each { md ->
                count++
                md.delete()
            }
            scheduleImageIndex(image.id)
        }
        auditService.log(image, "Delete metadata item ${key} (${count} items)", userId)
        return count > 0
    }


    Image createSubimage(Image parentImage, int x, int y, int width, int height, String userId, Map metadata = [:]) {

        if (x < 0) {
            x = 0;
        }
        if (y < 0) {
            y = 0;
        }

        def results = imageStoreService.retrieveImageRectangle(parentImage, x, y, width, height)
        if (results.bytes) {
            int subimageIndex = Subimage.countByParentImage(parentImage) + 1
            def filename = "${parentImage.originalFilename}_subimage_${subimageIndex}"
            def subimage = storeImageBytes(ByteSource.wrap(results.bytes),filename, results.bytes.length, results.contentType, userId, false, metadata).image

            def subimageRect = new Subimage(parentImage: parentImage, subimage: subimage, x: x, y: y, height: height, width: width)
            subimage.parent = parentImage
            subimageRect.save(flush:true)

            auditService.log(parentImage, "Subimage created ${subimage.imageIdentifier}", userId)
            auditService.log(subimage, "Subimage created from parent image ${parentImage.imageIdentifier}", userId)

//            scheduleArtifactGeneration(subimage.id, userId)
            scheduleImageIndex(subimage.id)

            return subimage
        }
    }

    Map getImageInfoMap(Image image) {
        def map = [
                imageId: image.imageIdentifier,
                height: image.height,
                width: image.width,
                tileZoomLevels: image.zoomLevels,
                thumbHeight: image.thumbHeight,
                thumbWidth: image.thumbWidth,
                filesize: image.fileSize,
                mimetype: image.mimeType,
                creator: image.creator,
                title: image.title,
                description: image.description,
                rights: image.rights,
                rightsHolder: image.rightsHolder,
                license: image.license
        ]
        def urls = getAllUrls(image.imageIdentifier)
        urls.each { kvp ->
            map[kvp.key] = kvp.value
        }
        return map
    }

    def createNextTileJob() {
        def task = _tilingQueue.poll() as ImageBackgroundTask
        if (task == null) {
            return [success:false, message:"No tiling jobs available at this time."]
        } else {
            if (task) {
                def image = Image.get(task.imageId)
                // Create a new pending job
                def ticket = UUID.randomUUID().toString()
                def job = new OutsourcedJob(image: image, taskType: ImageTaskType.TMSTile, expectedDurationInMinutes: 15, ticket: ticket)
                job.save()
                return [success: true, imageId: image.imageIdentifier, jobTicket: ticket, tileFormat: TileFormat.JPEG]
            } else {
                return [success:false, message: "Internal error!"]
            }
        }
    }

    def resetImageLinearScale(Image image) {
        image.mmPerPixel = null;
        image.calibratedByUser = null
        image.save(flush:true)
        scheduleImageIndex(image.id)
    }

    def calibrateImageScale(Image image, double pixelLength, double actualLength, String units, String userId) {

        double scale = 1.0
        switch (units) {
            case "inches":
                scale = 25.4
                break;
            case "metres":
                scale = 1000
                break;
            case "feet":
                scale = 304.8
                break;
            default: // unrecognized units, or mm
                break;
        }

        def mmPerPixel = (actualLength * scale) / pixelLength

        image.mmPerPixel = mmPerPixel
        image.calibratedByUser = userId
        image.save(flush:true)
        scheduleImageIndex(image.id)

        return mmPerPixel
    }

    def setHarvestable(Image image, Boolean harvestable, String userId) {
        image.setHarvestable(harvestable)
        image.save()
        scheduleImageIndex(image.id)
        auditService.log(image, "Harvestable set to ${harvestable}", userId)
    }

    /**
     *
     * @param maxRows
     * @param offset
     * @return a map with two keys - 'data' a list of maps containing the harvestable data, and 'columnHeadings', a list of strings with the distinct list of columns
     */
    def getHarvestTabularData(int maxRows = -1, int offset = 0) {

        def params = [:]
        if (maxRows > 0) {
            params.max = maxRows
        }

        if (offset > 0) {
            params.offset = offset
        }

        def images = Image.findAllByHarvestable(true)
        if (!images) {
            return [columnHeaders: ["imageUrl", "occurrenceId"], data: []]
        }

        def c = ImageMetaDataItem.createCriteria()
        // retrieve just the relevant metadata rows
        def metaDataRows = c.list {
            inList("image", images)
            or {
                eq("source", MetaDataSourceType.SystemDefined)
                eq("source", MetaDataSourceType.UserDefined)
            }
        }

        def metaDataMappedbyImage = metaDataRows.groupBy {
            it.image.id
        }

        def columnHeaders = ['imageUrl', 'occurrenceId']

        def tabularData = []

        images.each { image ->
            def map =  [occurrenceId: image.imageIdentifier, 'imageUrl': imageService.getImageUrl(image.imageIdentifier)]
            def imageMetadata = metaDataMappedbyImage[image.id]
            imageMetadata.each { md ->
                if (md.value) {
                    map[md.name] = md.value
                    if (!columnHeaders.contains(md.name)) {
                        columnHeaders << md.name
                    }
                }
            }
            tabularData << map
        }

        return [data: tabularData, columnHeaders: columnHeaders]
    }

    def deleteIndex() {
        elasticSearchService.reinitialiseIndex()
    }

    /**
     * Retrieve image via numeric ID or guid.
     * @param params
     * @return
     */
    def getImageFromParams(params) {
        def id = params.int("id")
        def image = null
        if (id != null) {
            image = Image.findById(id, [ cache:true ])
        }
//        def image = Image.findById(params.int("id"))
        if (!image) {
            String guid = params.id // maybe the id is a guid?
            if (!guid) {
                guid = params.imageId
            }

            if (guid) {
                image = Image.findByImageIdentifier(guid, [ cache: true])
            }
        }
        return image
    }

    def searchResultsToImageInfoList(List<Map<String,Object>> searchResults, Boolean includeTags = false, Boolean includeMetadata = false) {
        def results = searchResults?.collect { image ->
            searchResultToImageInfo(image, includeTags, includeMetadata)
        }
        return results
    }
    
    def imageListToImageInfoList(List<Image> images, Boolean includeTags = false, Boolean includeMetadata = false) {
        def results = images?.collect { image ->
            def map = [:]
            addImageInfoToMap(image, map, includeTags, includeMetadata)
            map
        }
        return results
    }

    def searchResultToImageInfo(Map<String,Object> searchResult, Boolean includeTags = false, Boolean includeMetadata = false) {
        // harmonise potential original filename keys but otherwise passthrough the ES result unchanged
        if (searchResult.containsKey('originalfilename')) {
            searchResult.originalFilename = UrlUtils.stripCredentials(searchResult.remove('originalfilename'))
        }
        if (searchResult.containsKey('originalFileName')) {
            searchResult.originalFilename = UrlUtils.stripCredentials(searchResult.remove('originalFileName'))
        }
        if (searchResult.containsKey('originalFilename')) {
            searchResult.originalFilename = UrlUtils.stripCredentials(searchResult['originalFilename'])
        }
        return searchResult
//        def map = [:]
//        map.imageIdentifier = searchResult.imageIdentifier
//        map.mimeType = searchResult.format
//        map.originalFileName = UrlUtils.maskCredentials(searchResult.originalfilename ?: searchResult.originalFilename, false)
//        map.sizeInBytes = searchResult.fileSize
//        map.rights = searchResult.rights ?: ''
//        map.rightsHolder = searchResult.rightsHolder ?: ''
//        map.dateUploaded = searchResult.dateUploaded ?: null
//        map.dateTaken = searchResult.dateTaken ?: null
//        if (map.mimeType && map.mimeType.startsWith('image')){
//            map.imageUrl = getImageUrl(searchResult.imageIdentifier)
//            map.tileUrlPattern = "${getImageTilesUrlPattern(searchResult.imageIdentifier)}"
//            map.mmPerPixel = searchResult.mmPerPixel ?: ''
//            map.height = searchResult.height
//            map.width = searchResult.width
//            map.tileZoomLevels = searchResult.zoomLevels ?: 0
//        }
//        map.description = searchResult.description ?: ''
//        map.title = searchResult.title ?: ''
//        map.type = searchResult.type ?: ''
//        map.audience = searchResult.audience ?: ''
//        map.references = searchResult.references ?: ''
//        map.publisher = searchResult.publisher ?: ''
//        map.contributor = searchResult.contributor ?: ''
//        map.created = searchResult.created ?: ''
//        map.source = searchResult.source ?: ''
//        map.creator = searchResult.creator ?: ''
//        map.license = searchResult.license ?: ''
//        if (searchResult.recognisedLicense) {
//            map.recognisedLicence = [
//                    'acronym' : searchResult.recognisedLicense.acronym,
//                    'name' : searchResult.recognisedLicense.name,
//                    'url' : searchResult.recognisedLicense.url,
//                    'imageUrl' : searchResult.recognisedLicense.imageUrl
//            ]
//        } else {
//            map.recognisedLicence = null
//        }
//        map.dataResourceUid = searchResult.dataResourceUid ?: ''
//        map.occurrenceID = searchResult.occurrenceId ?: ''
//
//        if (collectoryService) {
//            collectoryService.addMetadataForResource(map)
//        }
//        return map
    }
    
    def addImageInfoToMap(Image image, Map results, Boolean includeTags, Boolean includeMetadata) {

        results.imageIdentifier = image.imageIdentifier
        results.mimeType = image.mimeType
        results.originalFileName = UrlUtils.stripCredentials(image.originalFilename)
        results.sizeInBytes = image.fileSize
        results.rights = image.rights ?: ''
        results.rightsHolder = image.rightsHolder ?: ''
        results.dateUploaded = image.dateUploaded ? image.dateUploaded.format( "yyyy-MM-dd HH:mm:ss") : null
        results.dateTaken = image.dateTaken ? image.dateTaken.format( "yyyy-MM-dd HH:mm:ss") : null
        if (results.mimeType && results.mimeType.startsWith('image')){
            results.imageUrl = getImageUrl(image.imageIdentifier)
            results.tileUrlPattern = "${getImageTilesUrlPattern(image.imageIdentifier)}"
            results.mmPerPixel = image.mmPerPixel ?: ''
            results.height = image.height
            results.width = image.width
            results.tileZoomLevels = image.zoomLevels ?: 0
        }
        results.description = image.description ?: ''
        results.title = image.title ?: ''
        results.type = image.type ?: ''
        results.audience = image.audience ?: ''
        results.references = image.references ?: ''
        results.publisher = image.publisher ?: ''
        results.contributor = image.contributor ?: ''
        results.created = image.created ?: ''
        results.source = image.source ?: ''
        results.creator = image.creator ?: ''
        results.license = image.license ?: ''
        if (image.recognisedLicense) {
            results.recognisedLicence = [
                    'acronym' : image.recognisedLicense.acronym,
                    'name' : image.recognisedLicense.name,
                    'url' : image.recognisedLicense.url,
                    'imageUrl' : image.recognisedLicense.imageUrl
            ]
        } else {
            results.recognisedLicence = null
        }
        results.dataResourceUid = image.dataResourceUid ?: ''
        results.occurrenceID = image.occurrenceId ?: ''

        if (collectoryService) {
            collectoryService.addMetadataForResource(results)
        }

        if (includeTags) {
            results.tags = []
            def imageTags = ImageTag.findAllByImage(image)
            imageTags?.each { imageTag ->
                results.tags << imageTag.tag.path
            }
        }

        if (includeMetadata) {
            results.metadata = []
            def metaDataList = ImageMetaDataItem.findAllByImage(image)
            metaDataList?.each { md ->
                results.metadata << [key: md.name, value: md.value, source: md.source]
            }
        }
    }

    def UUID_PATTERN = ~/\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b/

    @NotTransactional // will start a new transaction if required
    def getImageGUIDFromParams(GrailsParameterMap params) {

        if (params.id) {
            //if it a GUID, avoid database trip if possible....
            if (UUID_PATTERN.matcher(params.id).matches()){
                return params.id
            }
            if (params.id ) {
                def identifer = Image.withNewTransaction(readOnly: true) {
                     Image.findById(params.int("id"), [ cache: true ])?.imageIdentifier
                }
                if (identifer) {
                    return identifer
                }
            }
        } else if (params.imageId) {
            //if it a GUID, avoid database trip if possible....
            if (UUID_PATTERN.matcher(params.imageId).matches()){
                return params.imageId
            }
            if (params.imageId) {
                def identifer = Image.withNewTransaction(readOnly: true) {
                    Image.findById(params.int("imageId"), [ cache: true ])?.imageIdentifier
                }
                if (identifer) {
                    return identifer
                }
            }
        }
        return null
    }

    /**
     * Export CSV.
     *
     * @param outputStream
     * @return
     */
    def exportCSV(OutputStream outputStream) {
        eachRowToCSV(outputStream.newWriter('UTF-8'), EXPORT_IMAGES_SQL)
    }

    def exportAvro(OutputStream outputStream) {
        eachRowToAvro(outputStream, EXPORT_IMAGES_SQL)
    }

    /**
     * Export Mapping CSV.
     *
     * @param outputStream
     * @return
     */
    def exportMappingCSV(OutputStream outputStream) {
        eachRowToCSV(outputStream.newWriter('UTF-8'), EXPORT_MAPPING_SQL)
    }

    def exportMappingAvro(OutputStream outputStream) {
        eachRowToAvro(outputStream, EXPORT_MAPPING_SQL)
    }

    /**
     * Export Dataset Mapping CSV.
     *
     * @param outputStream
     * @return
     */
    def exportDatasetMappingCSV(String datasetID, OutputStream outputStream) {
        eachRowToCSV(outputStream.newWriter('UTF-8'), EXPORT_DATASET_MAPPING_SQL, [datasetID], ',', '\\')
    }

    def exportDatasetMappingAvro(String datasetID, OutputStream outputStream) {
        eachRowToAvro(outputStream, EXPORT_DATASET_MAPPING_SQL, [datasetID])
    }

    def exportDatasetCSV(String datasetID, OutputStream outputStream) {
        eachRowToCSV(outputStream.newWriter('UTF-8'), EXPORT_DATASET_SQL, [datasetID])
    }

    def exportDatasetAvro(String datasetID, OutputStream outputStream) {
        eachRowToAvro(outputStream, EXPORT_DATASET_SQL, [datasetID])
    }

    /**
     * Stream the results from an SQL query to a CSV
     * @param writer The ultimate location of the CSV
     * @param sql The SQL to execute - must not contain embedded variables or this could lead to SQLi
     * @param params The SQL params to be passed to the query
     * @param separator The value separator for the CSV output
     * @param escape The character that should appear before a data character that matches the QUOTE value. The default is the same as the QUOTE value (so that the quoting character is doubled if it appears in the data).
     * @return The CSV results will have been written to a writer
     */
    private def eachRowToCSV(Writer writer, String sql, List<Object> params = [], String separator = ",", String escape = '"', String quote = '"') {
        def csvWriter = new CSVWriterBuilder(writer)
                .withParser(
                        // Use a RFC 4180 compliant CSV formatter unless the caller requires a separate escape character
                        escape == quote ?
                                new RFC4180ParserBuilder()
                                    .withQuoteChar(quote as char)
                                    .withSeparator(separator as char)
                                .build()
                        :
                            new CSVParserBuilder()
                                .withSeparator(separator as char)
                                .withEscapeChar(escape as char)
                                .withQuoteChar(quote as char)
                            .build())
                .build()

        eachRowTo(sql, params) { rs ->
            csvWriter.writeAll(rs, true)
        }
        writer.flush()
    }

    /**
     * Pass the results of the SQL query through a function that turns the query result metadata into an AVRO schema
     * and then each row in the result set becomes a record in the resulting file.  The AVRO file is written to the
     * given OutputStream but the OutputStream is not closed.
     *
     * @param outputStream The output stream to write the AVRO file to
     * @param sql The SQL query to run
     * @param params The parameters for the SQL query
     */
    private def eachRowToAvro(OutputStream outputStream, String sql, List<Object> params = []) {
        DataFileWriter<GenericRecord> dataFileWriter = null

        eachRowTo(sql, params) {rs ->
            def schema = avroSchema(rs)

            DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<GenericRecord>(schema)
            dataFileWriter = new DataFileWriter<GenericRecord>(avroWriter)
            dataFileWriter.create(schema, outputStream)

            ResultSetMetaData metadata = rs.getMetaData()
            while (rs.next()) {
                def rb = new GenericRecordBuilder(schema)
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    def label = metadata.getColumnLabel(i)
                    def value = getColumnValue(rs, metadata.getColumnType(i), i, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT)
                    rb.set(label, value)
                }
                dataFileWriter.append(rb.build())
            }
        }
        dataFileWriter?.flush()
    }

    /**
     * Helper function that turns the metadata from a ResultSet into an AVRO schema
     * @param rs The SQL result set
     * @return The AVRO schema for the result set
     */
    private def avroSchema(ResultSet rs) {
        ResultSetMetaData metadata = rs.getMetaData()
        def schemaAssembler = SchemaBuilder.builder()
                .record("record")
                .fields()
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            String label = metadata.getColumnLabel(i)
            int type = metadata.getColumnType(i)
            switch (type) {
                case Types.BOOLEAN:
                    schemaAssembler.optionalBoolean(label)
                    break;
                case Types.DECIMAL:
                case Types.REAL:
                case Types.NUMERIC:
                case Types.DOUBLE:
                    schemaAssembler.optionalDouble(label)
                    break;
                case Types.FLOAT:
                    schemaAssembler.optionalFloat(label)
                    break
                case Types.BIGINT:
                    schemaAssembler.optionalLong(label)
                    break
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    schemaAssembler.optionalInt(label)
                    break
                case Types.BLOB:
                    schemaAssembler.optionalBytes(label)
                    break
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                case Types.NCLOB:
                case Types.CLOB:
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                case Types.CHAR:
                default:
                    // This takes care of Types.BIT, Types.JAVA_OBJECT, and anything
                    // unknown.
                    schemaAssembler.optionalString(label)
            }
        }

        return schemaAssembler.endRecord()
    }

    /**
     * Helper function that turns a column value into the appropriate type for the AVRO record.
     * @param rs The result set
     * @param colType The Java Result Set Type for the given colIndex
     * @param colIndex The column index in the current row in the Result Set
     * @param dateFormatString The date format string for any Dates
     * @param timestampFormatString The timestamp format string for any Timestamps
     * @return A primitive object that can be written to an AVRO record
     */
    private Object getColumnValue(ResultSet rs, int colType, int colIndex, String dateFormatString, String timestampFormatString) {

        def value

        switch (colType) {
            case Types.BOOLEAN:
                value = rs.getBoolean(colIndex);
                break
            case Types.DECIMAL:
            case Types.REAL:
            case Types.NUMERIC:
                BigDecimal d = rs.getBigDecimal(colIndex)
                value = d.doubleValue()
                break
            case Types.DOUBLE:
                value = rs.getDouble(colIndex)
                break
            case Types.FLOAT:
                value = rs.getFloat(colIndex)
                break
            case Types.BIGINT:
                value = rs.getLong(colIndex)
                break
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                value = rs.getInt(colIndex)
                break
            case Types.BLOB:
                value = rs.getBlob(colIndex).binaryStream.bytes
                break
            case Types.DATE:
                value = handleDate(rs.getDate(colIndex), dateFormatString);
                break
            case Types.TIME:
                def time = rs.getTime(colIndex)
                value = time ? Objects.toString(time) : null
                break
            case Types.TIMESTAMP:
                value = handleTimestamp(rs.getTimestamp(colIndex), timestampFormatString);
                break
            case Types.NCLOB:
                value = rs.getNClob(colIndex)?.characterStream?.text
                break
            case Types.CLOB:
                value = rs.getClob(colIndex)?.characterStream?.text
                break
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
            case Types.CHAR:
                value = rs.getString(colIndex)
                break
            default:
                // This takes care of Types.BIT, Types.JAVA_OBJECT, and anything
                // unknown.
                // TODO Array types?
                def obj = rs.getObject(colIndex)
                value = obj ? Objects.toString(obj) : null
        }


        if (rs.wasNull() || value == null) {
            value = null
        }

        return value
    }

    private String handleDate(java.sql.Date date, String dateFormatString) throws SQLException {
        SimpleDateFormat df = new SimpleDateFormat(dateFormatString)
        return date == null ? null : df.format(date)
    }

    protected String handleTimestamp(Timestamp timestamp, String timestampFormatString) {
        SimpleDateFormat timeFormat = new SimpleDateFormat(timestampFormatString);
        return timestamp == null ? null : timeFormat.format(timestamp);
    }

    /**
     * Runs a SQL query and then runs the passed in closure with the ResultSet for the closure.
     *
     * @param sql The SQL query
     * @param params The parameters for the query
     * @param c The cloure to receive a single java.sql.ResultSet as a parameter
     */
    private def eachRowTo(String sql, List<Object> params,
                          @ClosureParams(value = SimpleType, options= 'java.sql.ResultSet' ) Closure c) {
        Connection conn = null
        PreparedStatement st = null
        ResultSet rs = null
        boolean savedAutoCommit = true
        try {
            conn = dataSource.getConnection()
            // Autocommit must be off for PG driver to use cursor
            savedAutoCommit = conn.autoCommit

            conn.autoCommit = false
            st = conn.prepareStatement(sql)
            // Fetch size must be non-0 for PG driver to use cursor
            st.fetchSize = 10000
            params.eachWithIndex { param, i ->
                st.setObject(i + 1, param)
            }
            rs = st.executeQuery()

            c(rs)
        } catch (SQLException e) {
            log.warn("Failed to execute: $sql because: ${e.message}")
            throw e
        } finally {
            try {
                conn?.autoCommit = savedAutoCommit
            }
            catch (SQLException e) {
                log.debug("Caught exception resetting auto commit: ${e.message} - continuing");
            }
            try {
                rs?.close()
            } catch (SQLException e) {
                log.debug("Caught exception closing resultSet: ${e.message} - continuing");
            }
            try {
                st?.close()
            } catch (SQLException e) {
                log.debug("Caught exception closing statement: ${e.message} - continuing");
            }
            try {
                conn?.close()
            } catch (SQLException e) {
                log.debug("Caught exception closing connection: ${e.message} - continuing");
            }
        }
    }


    /**
     * Export database entries to a file for elastic search to index.
     *
     * @return
     */
    File exportIndexToFile(){
        FileUtils.forceMkdir(new File(grailsApplication.config.getProperty('imageservice.exportDir')))
        def exportFile = grailsApplication.config.getProperty('imageservice.exportDir') + "/images-index.csv"
        def file = new File(exportFile)
        file.withWriter("UTF-8") { writer ->
            eachRowToCSV(writer, """SELECT * FROM export_index;""", [])
        }
        file
    }

    @Transactional
    void migrateImage(long imageId, long destinationStorageLocationId, String userId, boolean deleteSource) {
        log.debug("Migrating image id {} to storage location {}", imageId, destinationStorageLocationId)
        def image = Image.findById(imageId)
        def sl = StorageLocation.findById(destinationStorageLocationId)
        StorageLocation source = image.storageLocation
        String imageIdentifier = image.imageIdentifier
        if (!image.dateDeleted) {
            log.debug("Beginning migration for image {}...", imageId)
            if (source == sl) {
                log.warn("Attempt to migrate image {} to storage location {} aborted because it's already there.", imageId, sl)
                return
            }
            imageStoreService.migrateImage(image, sl)
            log.debug("Image {} migration ended", imageId)
            image.storageLocation = sl
            image.save()
            auditService.log(image.imageIdentifier, "Migrated to $sl", userId)
        }
        if (source && imageIdentifier && deleteSource) {
            source.deleteStored(imageIdentifier)
        }
    }

    private byte[] resizeImageIfNeeded(byte[] imageData, String contentType, int maxWidth) throws IOException {
        if (!shouldResize(contentType)) {
            return imageData
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(imageData)) {
            BufferedImage originalImage = ImageIO.read(bais);

            int originalWidth = originalImage.getWidth();
            int originalHeight = originalImage.getHeight();

            if (originalWidth > maxWidth) {
                int newWidth = maxWidth;
                int newHeight = (newWidth * originalHeight) / originalWidth;

                BufferedImage resizedImage = Scalr.resize(originalImage, Scalr.Method.QUALITY, newWidth, newHeight);

                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    ImageIO.write(resizedImage, contentType.split("/")[1], baos);
                    return baos.toByteArray();
                }
            }

            return imageData;
        } catch (Exception e) {
            log.error("Error resizing image", e);
            return imageData;
        }
    }

    private boolean shouldResize(String contentType) {
        return contentType.startsWith("image/")
    }

}
