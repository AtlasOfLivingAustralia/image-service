package au.org.ala.images

import au.org.ala.web.AlaSecured
import au.org.ala.web.CASRoles
import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import grails.converters.JSON
import grails.converters.XML
import groovy.json.JsonSlurper
import org.springframework.web.multipart.MultipartFile
import org.springframework.web.multipart.MultipartRequest

import java.util.regex.Pattern


@AlaSecured(value = [CASRoles.ROLE_ADMIN, "ROLE_IMAGE_ADMIN"], redirectUri = "/", anyRole = true)
class AdminController {

    def imageService
    def settingService
    def tagService
    def elasticSearchService
    def collectoryService
    def batchService
    def analyticsService
    def imageStoreService
    def authService
    def sessionFactory

    def index() {
        redirect(action:'dashboard')
    }

    def debugBackgroundQueue(){
        imageService.dumpQueueToFile()
        flash.message = "Background queue dumped to file."
        redirect(action:'dashboard')
    }

    def image() {
        def image = imageService.getImageFromParams(params)
        if (!image) {
            flash.errorMessage = "Could not find image with id ${params.int("id") ?: params.imageId }!"
            redirect(action:'list')
        } else {
            def subimages = Subimage.findAllByParentImage(image)*.subimage

            def sizeOnDisk = image.consumedSpace()

            //accessible from cookie
            def userId = authService.userId

            def isAdmin = request.isUserInRole('ROLE_ADMIN')

            def thumbUrls = imageService.getAllThumbnailUrls(image.imageIdentifier)

            boolean isImage = imageService.isImageType(image)

            //add additional metadata
            def resourceLevel = collectoryService.getResourceLevelMetadata(image.dataResourceUid)

            render( view:"../image/details", model: [imageInstance: image, subimages: subimages, sizeOnDisk: sizeOnDisk,
             squareThumbs: thumbUrls, isImage: isImage, resourceLevel: resourceLevel, isAdmin:isAdmin, userId:userId, isAdminView:true])
        }
    }

    def upload() { }

    def analytics() {
        render(view: 'analytics', model:[results:analyticsService.byAll()])
    }

    def storeImage() {

        MultipartFile file = request.getFile('image')

        if (!file || file.size == 0) {
            flash.errorMessage = "You need to select a file to upload!"
            redirect(action:'upload')
            return
        }

        def pattern = Pattern.compile('^image/(.*)$|^audio/(.*)|^application/pdf$')

        def m = pattern.matcher(file.contentType)
        if (!m.matches()) {
            flash.errorMessage = "Invalid file type for upload. Must be an image, audio  or PDF file (content is ${file.contentType})"
            redirect(action:'upload')
            return
        }

        def userId = authService.getUserId() ?: "<anonymous>"

        //retrieve metadata
        def metadata = [
           title: params.title,
           creator: params.creator,
           description: params.description,
           license: params.license,
           rights: params.rights,
           rightsHolder: params.rightsHolder,
           dataResourceUid: params.dataResourceUid,
        ]

        ImageStoreResult storeResult = imageService.storeImage(file, userId, metadata)
        if (storeResult.alreadyStored) {
            //reindex if already stored
            imageService.scheduleImageIndex(storeResult.image.id)
        } else {
            if (storeResult.image) {
                imageService.schedulePostIngestTasks(storeResult.image.id, storeResult.image.imageIdentifier, storeResult.image.originalFilename, userId)
            } else {
                imageService.scheduleNonImagePostIngestTasks(storeResult.image.id)
            }
        }
        flash.message = "Image uploaded with identifier: ${storeResult.image?.imageIdentifier}"
        redirect(action:'upload', params:[newImageId:storeResult.image?.imageIdentifier])
    }

    def uploadImagesFromCSVFile() {
        // it should contain a file parameter
        MultipartRequest req = request as MultipartRequest
        if (req) {
            MultipartFile file = req.getFile('csvfile')
            if (!file || file.size == 0) {
                renderResults([success: false, message: 'File not supplied or is empty. Please supply a filename.'])
                return
            }

            // need to convert the csv file into a list of maps...
            int lineCount = 0
            def headers = []
            def batch = []

            try {
                file.inputStream.eachCsvLine { tokens ->
                    if (lineCount == 0) {
                        headers = tokens
                    } else {
                        def m = [:]
                        for (int i = 0; i < headers.size(); ++i) {
                            m[headers[i]] = tokens[i]
                        }
                        batch << m
                    }
                    lineCount++
                }
                def batchId = scheduleImagesUpload(batch, authService.getUserId())
                renderResults([success: true, batchId: batchId, message:'Image upload started'])
            } catch (Exception e){
                log.error(e.getMessage(), e)
                renderResults([success: false, message: "Problem reading CSV file. Please check contents."])
            }
        } else {
            renderResults([success: false, message: "Problem reading CSV file from upload."])
        }
    }

    def scheduleImagesUpload(imageList, userId){

        def batchId = batchService.createNewBatch()
        int imageCount = 0
        imageList.each { srcImage ->
            if (!srcImage.containsKey("importBatchId")) {
                srcImage["importBatchId"] = batchId
            }
            batchService.addTaskToBatch(batchId, new UploadFromUrlTask(srcImage, imageService, userId))
            imageCount++
        }
        return batchId
    }

    def getBatchProgress() {
        def batchId = params.batchId
        def batchStatus = batchService.getBatchStatus(batchId)
        if (batchStatus) {
            render(batchStatus as JSON, contentType: 'application/json')
        } else {
            render([error: 'batch not found'] as JSON, contentType: 'application/json', status: 404)
        }
    }

    def scheduleDeletedImagesPurge(){
        imageService.scheduleBackgroundTask(new DeletedImagesPurgeBackgroundTask(imageService))
        flash.message = "Deleted images purge started. Refresh dashboard for progress."
        redirect(action:'dashboard')
    }

    def licences(){
        //get current licence content
        def licenceCSV = new StringWriter()
        def csvWriter = new CSVWriter(licenceCSV)
        License.findAll().each {
            String[] licence = [it.acronym, it.name, it.url,  it.imageUrl]
            csvWriter.writeNext(licence)
        }

        def licenceCSVMappings = new StringWriter()
        def csvWriter2 = new CSVWriter(licenceCSVMappings)
        LicenseMapping.findAll().each {
            String[] licenceCSVMapping = [it.license.acronym, it.value]
            csvWriter2.writeNext(licenceCSVMapping)
        }
        [licenceCSV:licenceCSV.toString(), licenceCSVMapping:licenceCSVMappings.toString()]
    }

    def batchUploads(){
        [results: batchService.getUploads(),
         active: batchService.getActiveFiles(),
         queued: batchService.getQueuedFiles(),
         batchServiceProcessingEnabled: settingService.getBatchServiceProcessingEnabled()]
    }

    def batchUpload(){
        BatchFileUpload batchFileUpload = batchService.getBatchFileUpload(params.id)
        [batchFileUpload: batchFileUpload, files: batchService.getFilesForUpload(params.id),
         batchServiceProcessingEnabled: settingService.getBatchServiceProcessingEnabled()]
    }

    def batchReloadFile(){
        batchService.reloadFile(params.fileId)
        flash.message = "Reload initiated for ${params.fileId}"
        redirect(action:'batchUploads', message: "Reload initiated for ${params.fileId}")
    }

    def batchFileDeleteFromQueue(){
        batchService.deleteFileFromQueue(params.fileId)
        flash.message = "File ${params.fileId} deleted"
        redirect(action:'batchUploads', message: "File removed from queue for ${params.fileId}")
    }

    def updateStoredLicences(){

        def licensesCSV = params.licenses
        def licenceMappingCSV = params.licenseMapping

        //load licences
        if (licensesCSV) {
            def csv = new CSVReader(new StringReader(licensesCSV))
            def iter = csv.iterator()
            while (iter.hasNext()) {
                def line = iter.next()
                if (line && line.length >= 4 && line[0] && line[1] && line[2]){
                    License license = License.findByAcronym(line[0])
                    if (license){
                        license.name = line[1]
                        license.url = line[2]
                        license.imageUrl = line[3]
                        license.save(flush: true, failOnError: true)
                    } else {
                        license = new License(acronym: line[0], name: line[1], url: line[2], imageUrl: line[3])
                        license.save(flush: true, failOnError: true)
                    }
                }
            }
        }

        //load license mappings
        if (licenceMappingCSV){
            def csv2 = new CSVReader(new StringReader(licenceMappingCSV))
            def iter2 = csv2.iterator()
            while (iter2.hasNext()){
                def line = iter2.next()
                if (line && line.length >= 2 && line[0] && line[1]) {
                    def license = License.findByAcronym(line[0])
                    if (license) {
                        LicenseMapping licenseMapping = LicenseMapping.findByValue(line[1])
                        if (licenseMapping) {
                            licenseMapping.license = license
                            licenseMapping.save(flush: true, failOnError: true)
                        } else {
                            licenseMapping = new LicenseMapping(license: license, value: line[1])
                            licenseMapping.save(flush: true, failOnError: true)
                        }
                    } else {
                        log.warn("Unable to find mapping for acronym: " + line[0])
                    }
                }
            }
        }

        redirect(action:'dashboard', message: "Licences updated")
    }

    private renderResults(Object results, int responseCode = 200) {

        withFormat {
            json {
                def jsonStr = results as JSON
                if (params.callback) {
                    render("${params.callback}(${jsonStr})")
                } else {
                    render(jsonStr)
                }
            }
            xml {
                render(results as XML)
            }
        }
        response.status = responseCode
    }

    def searchCriteria() {
        def searchCriteriaDefinitions = SearchCriteriaDefinition.list()
        [criteriaDefinitions: searchCriteriaDefinitions]
    }

    def newSearchCriteriaDefinition() {
        SearchCriteriaDefinition criteriaDefinition = null
        render(view: 'editSearchCriteriaDefinition', model: [criteriaDefinition:  criteriaDefinition])
    }

    def editSearchCriteriaDefinition() {
        SearchCriteriaDefinition criteriaDefinition = SearchCriteriaDefinition.get(params.int("searchCriteriaDefinitionId"))
        render(view: 'editSearchCriteriaDefinition', model: [criteriaDefinition:  criteriaDefinition])
    }

    def saveSearchCriteriaDefinition() {
        def criteria = SearchCriteriaDefinition.get(params.int("searchCriteriaDefinitionId"))
        if (criteria == null) {
            criteria = new SearchCriteriaDefinition(params)
        } else {
            criteria.properties = params
        }

        criteria.save()

        redirect(action:"searchCriteria")
    }

    def deleteSearchCriteriaDefinition() {
        def criteria = SearchCriteriaDefinition.get(params.int("searchCriteriaDefinitionId"))
        if (criteria) {
            try {
                criteria.delete(flush: true)
            } catch (Exception ex) {
                flash.errorMessage = "Delete failed. This is probably because there exists search critera that use this definition<br/>" + ex.message
            }
        }
        redirect(action:"searchCriteria")
    }

    def dashboard() {}

    def tools() {
        [batchProcessingEnabled: settingService.getBatchServiceProcessingEnabled()]
    }

    def localIngest() {}

    def reinitialiseImageIndex() {
        imageService.deleteIndex()
        flash.message = "Initialised. Image index now empty. Reindex images to repopulate."
        redirect(action:'tools')
    }

    def reindexImages() {
        flash.message = "Reindexing scheduled. Monitor progress using the search interface."
        imageService.scheduleBackgroundTask(new ScheduleReindexAllImagesTask(imageService, elasticSearchService,
                grailsApplication.config.getProperty('elasticsearch.batchIndexSize', Integer)))
        redirect(action:'tools')
    }

    def fieldDefinitionsFragment() {
        def fieldDefinitions = ImportFieldDefinition.listOrderByFieldName()
        [fieldDefinitions: fieldDefinitions]
    }

    def inboxFileListFragment() {
        def fileList = imageService.listStagedImages()
        [fileList: fileList, fieldDefinitions: ImportFieldDefinition.listOrderByFieldName()]
    }

    def addFieldFragment() {
        render(view:'editFieldFragment')
    }

    def editFieldFragment() {
        def field = ImportFieldDefinition.get(params.int("id"))
        [fieldDefinition: field]
    }

    def saveFieldDefinition() {
        def name = params.name
        def type = params.type as ImportFieldType
        def value = params.value

        if (name && type && value) {

            if (type == ImportFieldType.FilenameRegex) {
                // try and compile the pattern

                try {
                    def pattern = Pattern.compile(value)
                } catch (Exception ex) {
                    render(['success': false, message: "Invalid regular Expression: ${ex.message}"] as JSON)
                    return
                }
            }
            // check existing
            def existing = ImportFieldDefinition.findByFieldName(name)
            if (existing) {
                existing.value = value
                existing.fieldType = type
            } else {
                def field = new ImportFieldDefinition(fieldType: type, fieldName: name, value: value)
                field.save(flush: true, failOnError: true)
            }
            render(['success': true, message: ""] as JSON)
        }

        render(['success': false, message: "Missing or incorrect parameters!"] as JSON)

    }

    def deleteFieldDefinition() {
        def fieldDefinition = ImportFieldDefinition.findById(params.int("id"))
        if (fieldDefinition) {
            fieldDefinition.delete(flush: true)
            render([success:true] as JSON)
        } else {
            render([success:false] as JSON)
        }
    }

    def duplicates() {
        def queryParams = [:]
        queryParams.max = params.max ?: 10
        queryParams.offset = params.offset ?: 0

        def allCounts = Image.executeQuery("select count(contentMD5Hash) from Image group by contentMD5Hash having count(*) > 1")
        def c = Image.executeQuery("select contentMD5Hash, count(*) from Image group by contentMD5Hash having count(*) > 1 order by count(*) desc", queryParams)

        // find an exemplar image for each set of duplicates
        def results = []
        c.each {
            def hash = it[0]
            def image = Image.findByContentMD5Hash(hash)
            results << [image: image, hash: hash, count:it[1]]
        }

        [results: results, totalCount: allCounts.size()]
    }

    def settings() {
        def settings = Setting.list([sort:'name'])
        [settings: settings]
    }

    def setSettingValue() {
        def name = params.name
        def value = params.value
        if (name && value) {
            try {
                settingService.setSettingValue(name, value)
                flash.message = "${name} set to ${value}"
            } catch (Exception ex) {
                flash.errorMessage = ex.message
            }
        } else {
            flash.errorMessage = "Failed to set setting. Either name or value was not supplied"
        }
        redirect(action:'settings')
    }

    def tags() {}

    def storageLocations() {}

    def uploadTagsFragment() {}

    def clearQueues(){
        imageService.clearImageTaskQueue()
        imageService.clearTilingTaskQueueLength()
        flash.message = 'Queue cleared'
        redirect(action:'tools', message: 'Queue cleared')
    }

    def uploadTagsFile() {
        MultipartFile file = request.getFile('tagfile')

        if (!file || file.size == 0) {
            flash.errorMessage = "You need to select a file to upload!"
            redirect(action:'tags')
            return
        }

        def count = tagService.loadTagsFromFile(file)

        flash.message = "${count} tags loaded from file"

        redirect(action:'tags')
    }

    def rematchLicenses(){
        imageService.scheduleBackgroundTask(new ScheduleLicenseReMatchAllBackgroundTask(imageService))
        flash.message = "Rematching licenses scheduled. Monitor progress using the dashboard.";
        redirect(action:'tools', message: flash.message)
    }

    def disableBatchProcessing(){
        settingService.disableBatchProcessing()
        flash.message = "Batch processing disabled.";
        redirect(action:'batchUploads', message: flash.message)
    }

    def clearFileQueue(){
        batchService.clearFileQueue()
        flash.message = "File queue cleared.";
        redirect(action:'batchUploads', message: flash.message)
    }

    def clearUploads(){
        batchService.clearUploads()
        flash.message = "File queue cleared";
        redirect(action:'batchUploads', message: flash.message)
    }

    def enableBatchProcessing(){
        settingService.enableBatchProcessing()
        flash.message = "Batch processing enabled.";
        redirect(action:'batchUploads', message: flash.message)
    }

    def checkForMissingImages(){
        imageService.scheduleBackgroundTask(new ScheduleMissingImagesBackgroundTask(imageStoreService, grailsApplication.config.getProperty('imageservice.exportDir')))
        flash.message = "Check for missing images started......Output: " + grailsApplication.config.getProperty('imageservice.exportDir') + "/missing-images.csv";
        redirect(action:'tools', message: flash.message)
    }

    def indexSearch() {
        QueryResults<Image> results = null
        if (params.q) {
            Map map = null
            try {
                map = new JsonSlurper().parseText(params.q)
            } catch (Exception ex) {
                flash.message = "Invalid JSON! - " + ex.message
                return
            }

            params.max = params.max ?: 48
            params.offset = params.offset ?: 0

            results = elasticSearchService.search(map, params)
        }
        [results: results, query: params.q]
    }

    def clearCollectoryCache(){
        collectoryService.clearCache()
        flash.message = 'Collectory cache cleared'
        redirect(action:'tools', message: 'Cache is cleared')
    }

    def clearHibernateCache() {
        sessionFactory.cache?.evictAllRegions()
        flash.message = 'Hibernate cache cleared'
        redirect(action:'tools', message: 'Cache is cleared')
    }
}
