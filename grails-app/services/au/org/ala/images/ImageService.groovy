package au.org.ala.images

import au.org.ala.images.metadata.MetadataExtractor
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.tiling.TileFormat
import com.opencsv.CSVParserBuilder
import com.opencsv.CSVWriterBuilder
import com.opencsv.RFC4180ParserBuilder
import grails.gorm.transactions.Transactional
import grails.orm.HibernateCriteriaBuilder
import groovy.transform.Synchronized
import groovyx.gpars.GParsPool
import okhttp3.HttpUrl
import org.apache.commons.codec.binary.Base64
import org.apache.commons.imaging.Imaging
import org.apache.commons.imaging.common.ImageMetadata
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata
import org.apache.commons.imaging.formats.tiff.TiffField
import org.apache.commons.imaging.formats.tiff.constants.TiffConstants
import org.apache.commons.imaging.formats.tiff.taginfos.TagInfo
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang.StringUtils
import org.apache.tika.mime.MimeType
import org.apache.tika.mime.MimeTypes
import org.hibernate.FlushMode
import org.hibernate.ScrollMode
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.multipart.MultipartFile

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.ReentrantLock

import static grails.web.http.HttpHeaders.USER_AGENT

class ImageService {

    def dataSource
    def imageStoreService
    def tagService
    def grailsApplication
    def logService
    def auditService
    def sessionFactory
    def imageService
    def elasticSearchService
    def settingService
    def collectoryService

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
    final EXPORT_DATASET_SQL = '''
SELECT
    i.image_identifier as "imageID",
    NULLIF(regexp_replace(i.original_filename, '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "identifier",
    NULLIF(regexp_replace(i.audience,          '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "audience",
    NULLIF(regexp_replace(i.contributor,       '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "contributor",
    NULLIF(regexp_replace(i.created,           '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "created",
    NULLIF(regexp_replace(i.creator,           '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "creator",
    NULLIF(regexp_replace(i.description,       '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "description",
    NULLIF(regexp_replace(i.mime_type,         '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "format",
    NULLIF(regexp_replace(i.license,           '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "license",
    NULLIF(regexp_replace(i.publisher,         '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "publisher",
    NULLIF(regexp_replace(i.dc_references,     '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "references",
    NULLIF(regexp_replace(i.rights_holder,     '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "rightsHolder",
    NULLIF(regexp_replace(i.source,            '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "source",
    NULLIF(regexp_replace(i.title,             '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "title",
    NULLIF(regexp_replace(i.type,              '[\\x00-\\x1F\\x7F-\\x9F]',  '', 'g'), '')  AS  "type"
FROM image i
WHERE data_resource_uid = ?
'''

    final EXPORT_DATASET_MAPPING_SQL = '''
SELECT
    image_identifier as "imageID",
    original_filename as "url"
    FROM image i
    WHERE data_resource_uid = ?
    '''

    private static Queue<BackgroundTask> _backgroundQueue = new ConcurrentLinkedQueue<BackgroundTask>()
    private static Queue<BackgroundTask> _tilingQueue = new ConcurrentLinkedQueue<BackgroundTask>()

    private static int BACKGROUND_TASKS_BATCH_SIZE = 100

    @Value('${http.default.readTimeoutMs:120000}')
    int readTimeoutMs = 120000 // 2 minutes

    @Value('${http.default.connectTimeoutMs:120000}')
    int connectTimeoutMs = 120000 // 2 minutes

    @Value('${http.default.user-agent:}')
    String userAgent

    @Value('${batch.purge.fetch.size:100}')
    int purgeFetchSize = 100

    @Value('${skin.orgNameShort:ALA}')
    String orgNameShort

    @Value('${info.app.name:image-service}')
    String appName

    @Value('${info.app.version:NaN}')
    String version

    Map imagePropertyMap = null

    ImageStoreResult storeImage(MultipartFile imageFile, String uploader, Map metadata = [:]) {

        if (imageFile) {
            // Store the image
            def originalFilename = imageFile.originalFilename
            def bytes = imageFile?.bytes
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

    private String userAgent() {
        def userAgent = this.userAgent
        if (!userAgent) {
            userAgent = "$orgNameShort-$appName/$version"
        }
        return userAgent
    }

    ImageStoreResult storeImageFromUrl(String imageUrl, String uploader, Map metadata = [:]) {
        if (imageUrl) {
            try {
                def image = Image.byOriginalFileOrAlternateFilename(imageUrl) // findByOriginalFilename(imageUrl)
                if (image && image.stored()) {
                    scheduleMetadataUpdate(image.imageIdentifier, metadata)
                    return new ImageStoreResult(image, true, image.alternateFilename?.contains(imageUrl) ?: false)
                }
                def url = new URL(imageUrl)
                def bytes = url.getBytes(connectTimeout: connectTimeoutMs, readTimeout: readTimeoutMs, requestProperties: [(USER_AGENT): userAgent()])

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
                        MimeType mimeType =  new MimeTypes().forName(metadata.format)
                        metadata.extension = mimeType.getExtension()
                        contentType = mimeType.toString()
                    } catch (Exception e){
                        log.debug("Un-parseable mime type supplied: " + metadata.format)
                    }
                }

                //detect from file
                if (contentType == null){
                    contentType = detectMimeTypeFromBytes(bytes, imageUrl)
                }

                def result = storeImageBytes(bytes, imageUrl, bytes.length, contentType, uploader, true, metadata)
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
            'thumbnail_square',
            'thumbnail_square_black',
            'thumbnail_square_white',
            'thumbnail_square_darkGrey',
            'thumbnail_square_darkGray'
    ] as Set

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
                        Image image = null

                        image = findImageInImageServiceUrl(imageUrl)

                        // Its not an image service URL, check DB
                        // For full URLs, we can treat these as unique identifiers
                        // For filenames (non URLs), use the filename and dataResourceUid to unique identify
                        if (!image){
                            if (imageUrl.startsWith("http")){
                                image = Image.byOriginalFileOrAlternateFilename(imageUrl) // findByOriginalFilename(imageUrl)
                            } else {
                                image = Image.findByOriginalFilenameAndDataResourceUid(imageUrl, imageSource.dataResourceUid)
                            }
                        }

                        if (!image) {
                            def result = [success: false, alreadyStored: false]
                            try {
                                def url = new URL(imageUrl)
                                def bytes = url.getBytes(connectTimeout: connectTimeoutMs, readTimeout: readTimeoutMs, requestProperties: [(USER_AGENT): userAgent()])
                                def contentType = detectMimeTypeFromBytes(bytes, imageUrl)
                                ImageStoreResult storeResult = storeImageBytes(bytes, imageUrl, bytes.length,
                                        contentType, uploader, true, imageSource)
                                result.imageId = storeResult.image.imageIdentifier
                                result.image = storeResult.image
                                result.success = true
                                result.alreadyStored = storeResult.alreadyStored
                                result.metadataUpdated = false
                            } catch (Exception ex) {
                                //log to batch update error file
                                log.error("Problem storing image - " + ex.getMessage(), ex)
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

    def logBadUrl(String url){
        new FailedUpload(url: url).save()
    }

    boolean isBadUrl(String url){
        FailedUpload.findByUrl(url) != null
    }

    @Transactional
    Map uploadImage(Map imageSource, String uploader){

        def imageUrl = getImageUrl(imageSource) as String

        if (imageUrl) {

            Image image = null

            image = findImageInImageServiceUrl(imageUrl)

            // Its not an image service URL, check DB
            // For full URLs, we can treat these as unique identifiers
            // For filenames (non URLs), use the filename and dataResourceUid to unique identify
            if (!image){
                if (imageUrl.startsWith("http")){
                    image = Image.byOriginalFileOrAlternateFilename(imageUrl) // findByOriginalFilename(imageUrl)
                } else {
                    image = Image.findByOriginalFilenameAndDataResourceUid(imageUrl, imageSource.dataResourceUid)
                }
            }

            if (!image && isBadUrl(imageUrl)){
                if (log.isDebugEnabled()) {
                    log.debug("We have already attempted to load {} without success. Skipping.", imageUrl)
                }
                return [success: false, alreadyStored: false]
            }

            if (!image) {
                def result = [success: false, alreadyStored: false]
                def bytes
                try {
                    def url = new URL(imageUrl)
                    bytes = url.getBytes(connectTimeout: connectTimeoutMs, readTimeout: readTimeoutMs, requestProperties: [(USER_AGENT): userAgent()])
                } catch (Exception e){
                    log.error("Unable to load image from URL: {}. Logging as failed URL", imageUrl)
                    logBadUrl(imageUrl)
                    return result
                }
                try {
                    def contentType = detectMimeTypeFromBytes(bytes, imageUrl)
                    ImageStoreResult storeResult = storeImageBytes(bytes, imageUrl, bytes.length, contentType, uploader, true, imageSource)
                    result.imageId = storeResult.image.imageIdentifier
                    result.image = storeResult.image
                    result.success = true
                    result.alreadyStored = storeResult.alreadyStored
                    result.isDuplicate = storeResult.isDuplicate
                    result.metadataUpdated = false
                } catch (Exception ex) {
                    //log to batch update error file
                    log.error("Problem storing image - " + ex.getMessage(), ex)
                    result.message = ex.message
                    result.success = false
                }
                result
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
                 [success: true,
                     imageId: image.imageIdentifier,
                     image: image,
                     alreadyStored: true,
                     metadataUpdated: metadataUpdated,
                     isDuplicate: image.alternateFilename?.contains(imageUrl) ?: false
                ]
            }
        }  else {
            [success: false]
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

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Store the bytes for an image.
     *
     * @param bytes
     * @param originalFilename
     * @param filesize
     * @param contentType
     * @param uploaderId
     * @param createDuplicates
     * @param metadata
     * @return
     */
    ImageStoreResult storeImageBytes(byte[] bytes, String originalFilename, long filesize, String contentType,
                          String uploaderId, boolean createDuplicates, Map metadata = [:]) {

        try {
            lock.lock()

            def md5Hash = bytes.encodeAsMD5()

            //check for existing image using MD5 hash
            def image = Image.findByContentMD5Hash(md5Hash)
            def preExisting = false
            def isDuplicate = false
            if (!image) {
                def sha1Hash = bytes.encodeAsSHA1()

                Long defaultStorageLocationID = settingService.getStorageLocationDefault()

                StorageLocation sl = StorageLocation.get(defaultStorageLocationID)

                def imgDesc = imageStoreService.storeImage(bytes, sl, contentType)

                // Create the image record, and set the various attributes
                image = new Image(
                        imageIdentifier: imgDesc.imageIdentifier,
                        contentMD5Hash: md5Hash,
                        contentSHA1Hash: sha1Hash,
                        uploader: uploaderId,
                        storageLocation: sl
                )

                if (metadata.extension) {
                    image.extension = metadata.extension
                } else {
                    // this is known to be problematic
                    def extension =  FilenameUtils.getExtension(originalFilename) ?: 'jpg'
                    if (extension && extension.contains("?")){
                        def cleanedExtension = extension.substring(0, extension.indexOf("?"))
                        if (cleanedExtension && cleanedExtension.length() > 0){
                            extension  = cleanedExtension
                        }
                    }
                    image.extension = extension
                }

                image.height = imgDesc.height
                image.width = imgDesc.width
                image.fileSize = filesize
                image.mimeType = contentType
                image.dateUploaded = new Date()
                image.originalFilename = originalFilename
                image.dateTaken = getImageTakenDate(bytes) ?: image.dateUploaded
            } else if (image.dateDeleted) {
                log.warn("Deleted Image has been re-uploaded.  Will undelete.")
                image.dateDeleted = null //reset date deleted if image resubmitted...
                preExisting = true
            } else if (createDuplicates && image.originalFilename != originalFilename) {
                log.warn("Existing image found at different URL ${image.originalFilename} to ${originalFilename}. Will add duplicate.")

                // we have seen this image before, but the URL has changed at source
                // so lets update it so that subsequent loads dont need
                // to re-download this image
                if (image.alternateFilename == null) {
                    image.alternateFilename = []
                }
                if (!image.alternateFilename.contains(originalFilename)) {
                    image.alternateFilename += originalFilename
                }
                preExisting = true
                isDuplicate = true
            } else {
                log.warn("Got a pre-existing image to store {} but it already exists at {}", originalFilename, image.imageIdentifier)
                preExisting = true
            }

            if (!preExisting) {
                //update metadata stored in the `image` table
                setMetadataOnImage(metadata, image)
                //try to match licence
                updateLicence(image)
            }

            try {
                image.save(flush: true, failOnError: true)
            } catch (Exception ex){
                log.error("Problem ${preExisting ? 'updating' : 'saving'} image ${originalFilename}  - " + ex.getMessage(), ex)
            }

            new ImageStoreResult(image, preExisting, isDuplicate)
        } finally {
            lock.unlock()
        }
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

    def hasImageCaseFriendlyProperty(Image image, String propertyName){
        if (!imagePropertyMap) {
            def properties = image.getProperties().keySet()
            imagePropertyMap = [:]
            properties.each { imagePropertyMap.put(it.toLowerCase(), it) }
        }
        imagePropertyMap.get(propertyName.toLowerCase())
    }

    def schedulePostIngestTasks(Long imageId, String identifier, String fileName, String uploaderId){
        scheduleArtifactGeneration(imageId, uploaderId)
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

    String getImageSquareThumbUrl(String imageIdentifier, String backgroundColor = null) {
        return imageStoreService.getImageSquareThumbUrl(imageIdentifier, backgroundColor)
    }

    List<String> getAllThumbnailUrls(String imageIdentifier) {
        def results = []
        def image = Image.findByImageIdentifier(imageIdentifier, [ cache: true])
        if (image) {
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

    private static Date getImageTakenDate(byte[] bytes) {
        try {
            ImageMetadata metadata = Imaging.getMetadata(bytes)
            if (metadata && metadata instanceof JpegImageMetadata) {
                JpegImageMetadata jpegMetadata = metadata

                def date = getImageTagValue(jpegMetadata,TiffConstants.EXIF_TAG_DATE_TIME_ORIGINAL)
                if (date) {
                    def sdf = new SimpleDateFormat("yyyy:MM:dd hh:mm:ss")
                    return sdf.parse(date.toString())
                }
            }
        } catch (Exception ex) {
            return null
        }
    }

    private static Object getImageTagValue(JpegImageMetadata jpegMetadata, TagInfo tagInfo) {
        TiffField field = jpegMetadata.findEXIFValue(tagInfo);
        if (field) {
            return field.value
        }
    }

    static Map<String, Object> getImageMetadataFromBytes(byte[] bytes, String filename) {
        def extractor = new MetadataExtractor()
        return extractor.readMetadata(bytes, filename)
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
            GParsPool.withPool(batchThreads, uncaughtExceptionHandler) {
                GParsPool.executeAsyncAndWait(theseTasks)
            }
        } catch (e) {
            log.error("Exception executing background tasks in batch", e)
        }
    }

    private static Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
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
            if (!image.deleteStored()) {
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
            def bytes = file.getBytes()
            def mimeType = detectMimeTypeFromBytes(bytes, file.name)
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
            generateImageThumbnails(image)

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
            scheduleTileGeneration(image.id, userId)
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
            logService.debug("Not Setting metadata item! Image ${image?.id} key: ${key} value: ${value}")
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
                logService.debug("Not Setting metadata item! Image ${image?.id} key: ${key} value: ${value}")
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

    static String detectMimeTypeFromBytes(byte[] bytes, String filename) {
        return new MetadataExtractor().detectContentType(bytes, filename);
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
            def subimage = storeImageBytes(results.bytes,filename, results.bytes.length, results.contentType, userId, false, metadata).image

            def subimageRect = new Subimage(parentImage: parentImage, subimage: subimage, x: x, y: y, height: height, width: width)
            subimage.parent = parentImage
            subimageRect.save(flush:true)

            auditService.log(parentImage, "Subimage created ${subimage.imageIdentifier}", userId)
            auditService.log(subimage, "Subimage created from parent image ${parentImage.imageIdentifier}", userId)

            scheduleArtifactGeneration(subimage.id, userId)
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
        def image = Image.findById(params.int("id"))
        if (!image) {
            String guid = params.id // maybe the id is a guid?
            if (!guid) {
                guid = params.imageId
            }

            image = Image.findByImageIdentifier(guid, [ cache: true])
        }
        return image
    }


    def addImageInfoToMap(Image image, Map results, Boolean includeTags, Boolean includeMetadata) {

        results.imageIdentifier = image.imageIdentifier
        results.mimeType = image.mimeType
        results.originalFileName = image.originalFilename
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

    def getImageGUIDFromParams(params) {

        if(params.id){
            //if it a GUID, avoid database trip if possible....
            if (UUID_PATTERN.matcher(params.id).matches()){
                return params.id
            }
            if(params.id ){
                def image = Image.findById(params.int("id"))
                if(image) {
                    return image.imageIdentifier
                }
            }
        } else if(params.imageId){
            //if it a GUID, avoid database trip if possible....
            if (UUID_PATTERN.matcher(params.imageId).matches()){
                return params.imageId
            }
            if(params.id ){
                def image = Image.findById(params.int("imageId"))
                if (image) {
                    return image.imageIdentifier
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
        eachRowToCSV(outputStream.newWriter('UTF-8'), """SELECT * FROM export_images;""")
    }

    /**
     * Export Mapping CSV.
     *
     * @param outputStream
     * @return
     */
    def exportMappingCSV(OutputStream outputStream) {
        eachRowToCSV(outputStream.newWriter('UTF-8'), """SELECT * FROM export_mapping;""")
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

    def exportDatasetCSV(String datasetID, OutputStream outputStream) {
        eachRowToCSV(outputStream.newWriter('UTF-8'), EXPORT_DATASET_SQL, [datasetID])
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

            csvWriter.writeAll(rs, true)
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
        writer.flush()
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
}
