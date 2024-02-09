package au.org.ala.images

import grails.web.mapping.LinkGenerator
import org.apache.commons.io.ByteOrderMark
import org.apache.commons.io.FileUtils
import org.apache.commons.io.input.BOMInputStream
import grails.web.servlet.mvc.GrailsParameterMap
import grails.plugins.csv.CSVMapReader
import org.springframework.web.multipart.MultipartFile

import java.util.regex.Pattern

class ImageStagingService {

    def grailsApplication
    def settingService
    def logService
    def imageService
    def auditService
    LinkGenerator grailsLinkGenerator

    ResultsPageList<StagedFile> getFileList(String userId, GrailsParameterMap params) {

        if (!userId) {
            return new ResultsPageList<StagedFile>([], 0)
        }

        def queryParams = [:]

        queryParams.max = params.int('max') ?: -1
        queryParams.offset = params.int('offset') ?: 0
        queryParams.order = params.order ?: 'asc'
        queryParams.sort = params.sort ?: 'dateStaged'

        def page = StagedFile.findAllByUserId(userId, queryParams)
        int totalCount = StagedFile.countByUserId(userId)

        return new ResultsPageList<StagedFile>(page, totalCount)
    }

    public StagedFile stageFile(String userId, MultipartFile file) {
        if (!userId || !file) {
            return null
        }

        def userdir = getStagingDirectory(userId)
        def originalFilename = file.originalFilename

        def newFile = new File(combine(userdir, originalFilename));
        file.transferTo(newFile);
        // If we get here the transfer was successful, so we can create a record for it in the database
        def stagedFile = new StagedFile(userId: userId, filename: originalFilename, dateStaged: new Date())
        stagedFile.save(failOnError: true, flush: true)
        return stagedFile
    }

    private String getStagingDirectory(String userId) {
        def basedir = grailsApplication.config.getProperty('imageservice.imagestore.staging') as String
        def userdir = new File(combine(basedir, userId))
        if (!userdir.exists()) {
            userdir.mkdirs()
        }
        return userdir.toPath()
    }

    private String getStagingDataFile(String userId) {
        combine(getStagingDirectory(userId), "/datafile/datafile.txt")
    }

    public static String combine(String path1, String path2) {
        new File(new File(path1), path2).getPath()
    }

    public String getStagedFileLocalPath(StagedFile stagedFile) {
        def userdir = getStagingDirectory(stagedFile.userId)
        return combine(userdir, stagedFile.filename)
    }

    public void purgeOldStagedFiles() {
        def lifetimeInHours = settingService.getStagedFileLifespanInHours() ?: 24

        // work out the cutoff date...
        def millis = lifetimeInHours * 60 * 60 * 1000 // minutes * seconds * millis
        def cutoffDate = new Date(new Date().getTime() - millis)
        def purgeList = StagedFile.findAllByDateStagedLessThan(cutoffDate)
        if (purgeList) {
            logService.debug("${purgeList.size()} staged files older than ${lifetimeInHours} hours.")
            purgeList.each { stagedFile ->
                deleteStagedFile(stagedFile)
            }
        } else {
            logService.debug("No eligible staged files found to purge.")
        }
    }

    public boolean deleteStagedFile(StagedFile stagedFile) {
        // first delete the file from the disk...
        def file = new File(getStagedFileLocalPath(stagedFile))
        if (file.exists()) {
            if (!file.delete()) {
                file.deleteOnExit()
            }
        }
        // then remove from the database
        stagedFile.delete()
    }

    def uploadDataFile(String userId, MultipartFile multipartFile) {
        def f = new File(getStagingDataFile(userId))
        if (!f.parentFile.exists()) {
            f.mkdirs()
        }
        if (f.exists()) {
            // delete any existing file first
            f.delete()
        }

        multipartFile.transferTo(f);
    }

    public boolean hasDataFileUploaded(String userId) {
        def f = new File(getStagingDataFile(userId))
        return f.exists()
    }

    public String getDataFileUrl(String userId) {
        grailsLinkGenerator.link(absolute: true, controller: 'staging', action: 'serve', params: [ path: "${userId}/datafile/datafile.txt"] )
    }

    public String getStagedFileUrl(StagedFile stagedFile) {
        grailsLinkGenerator.link(absolute: true, controller: 'staging', action: 'serve', params: [ path: "${stagedFile.userId}/${stagedFile.filename}"] )
    }

    def deleteDataFile(String userId) {
        def f = new File(getStagingDataFile(userId))
        if (f.exists()) {
            f.delete()
        }
    }

    public String[] getDataFileColumns(String userId) {
        def f = new File(getStagingDataFile(userId))
        if (f.exists()) {
            def lines = FileUtils.readLines(f)
            if (lines && lines.size() > 0) {
                return lines[0].split(",")
            }
        }
        return []
    }

    /**
     * Build a cache of regex patterns already compiled, to avoid having to recompile each expression for every row
     * @param fieldDefinitions
     * @return
     */
    private static Map<StagingColumnDefinition, Pattern> createPatternMap(List<StagingColumnDefinition> fieldDefinitions) {
        def patternMap = [:]
        fieldDefinitions.each { field ->
            Pattern pattern = null
            switch (field.fieldDefinitionType) {
                case StagingColumnType.NameRegex:
                    try {
                        pattern = Pattern.compile(field.format)
                    } catch (Exception ex) {
                        log.error(ex.message, ex)
                    }
                    break
            }
            if (pattern) {
                patternMap[field] = pattern
            }
        }
        return patternMap
    }

    private Map<String, Map<String, String>> getDataFileValues(String userId) {
        def dataFile = new File(getStagingDataFile(userId))
        def dataFileMap = [:]
        def dataFileColumns = []
        if (dataFile.exists()) {
            FileInputStream fis = new FileInputStream(dataFile)
            BOMInputStream bomInputStream = new BOMInputStream(fis, ByteOrderMark.UTF_8) // Ignore any UTF-8 Byte Order Marks, as they will stuff up the mapping!
            try {
                new CSVMapReader(new InputStreamReader(bomInputStream)).each { Map map ->
                    if (map.filename) {
                        if (!dataFileColumns) {
                            map.each {
                                if (it.key != 'filename') {
                                    dataFileColumns << it.key
                                }
                            }
                        }
                        def filename = map.get('filename')
                        if (filename) {
                            dataFileMap[filename] = map
                        }
                    }
                }
            } finally {
                if (bomInputStream) {
                    bomInputStream.close()
                }

                if (fis) {
                    fis.close()
                }
            }
        }
        return dataFileMap
    }

    def buildStagedImageData(String userId, Map params) {

        if (!params) {
            params = [sort:'id', order:'asc']
        }

        def stagedFiles = StagedFile.findAllByUserId(userId, params)
        def fieldDefinitions = StagingColumnDefinition.findAllByUserId(userId, [sort:'id', order:'asc'])
        def dataFileMap = getDataFileValues(userId)
        def patternMap = createPatternMap(fieldDefinitions)
        def images = []

        stagedFiles.each { stagedFile ->

            def localFile = new File(getStagedFileLocalPath(stagedFile))
            if (!localFile.exists()) {
                logService.log("Staged File ${stagedFile.filename} for user ${stagedFile.userId} does not exist on disk (${localFile.getAbsolutePath()}")
            } else {
                def imageMap = [id: stagedFile.id, filename: stagedFile.filename, stagedFileUrl: getStagedFileUrl(stagedFile), dateStaged: stagedFile.dateStaged]
                images << imageMap
                fieldDefinitions.each { field ->
                    def value = ""
                    switch (field.fieldDefinitionType) {
                        case StagingColumnType.NameRegex:
                            Pattern pattern = patternMap[field] as Pattern
                            if (field.format && pattern) {
                                def matcher = pattern.matcher(stagedFile.filename)
                                if (matcher.matches()) {
                                    if (matcher.groupCount() >= 1) {
                                        value = matcher.group(1)
                                    }
                                }
                            } else {
                                value = stagedFile.filename
                            }
                            break;
                        case StagingColumnType.Literal:
                            value = field.format
                            break;
                        case StagingColumnType.DataFileColumn:
                            def values = dataFileMap[stagedFile.filename]
                            if (values) {
                                value = values[field.format ?: field.fieldName]
                            }
                            break;
                        default:
                            value = "err"
                            break;
                    } // switch
                    imageMap[field.fieldName] = value
                }
            } // if file exists
        } // for each file

        return images
    }

    public Image importFileFromStagedFile(StagedFile stagedFile, String batchId, Map<String, String> metadata, Boolean harvestable) {

        def file = new File(getStagedFileLocalPath(stagedFile))

        if (!file.exists()) {
            logService.log("Local file ${file.getAbsoluteFile()} could not be found! Import aborted")
            return null
        }

        Image image = null

        Image.withNewTransaction {

            // Create the image domain object
            def bytes = file.getBytes()
            def mimeType = imageService.detectMimeTypeFromBytes(bytes, file.name)
            image = imageService.storeImageBytes(bytes, file.name, file.length(),mimeType, stagedFile.userId, false).image

            auditService.log(image, "Imported from ${file.absolutePath}", stagedFile.userId)

            if (image && batchId) {
                imageService.setMetaDataItem(image, MetaDataSourceType.SystemDefined,  "importBatchId", batchId)
            }

            // Is there any extra data to be applied to this image?
            def filterList = ['id', 'filename', 'stagedFileUrl']
            if (metadata) {
                metadata.each { md ->
                    if (!filterList.contains(md.key)) {
                        imageService.setMetaDataItem(image, MetaDataSourceType.SystemDefined, md.key, md?.value?.toString())
                    }
                }
            }
            imageService.generateImageThumbnails(image)

            image.harvestable = harvestable

            image.save(flush: true, failOnError: true)
            imageService.scheduleImageIndex(image.id)
        }

        // If we get here, and the image is not null, it means it has been committed to the database and we can remove the file from the inbox
        if (image) {
            if (!FileUtils.deleteQuietly(file)) {
                file.deleteOnExit()
            }
            // also we should do the thumb generation (we'll defer tiles until after the load, as it will slow everything down)
            imageService.scheduleTileGeneration(image.id, stagedFile.userId)

            // and delete the staged file...
            stagedFile.delete()
        }

        return image
    }

}
