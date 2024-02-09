package au.org.ala.images


import groovyx.gpars.GParsPool
import net.lingala.zip4j.ZipFile
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord

import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime

class BatchService {

    private final static Map<String, BatchStatus> _Batches = [:]
    public static final String LOADING = "LOADING"
    public static final String STOPPED = "STOPPED"
    public static final String COMPLETE = "COMPLETE"
    public static final String PARTIALLY__COMPLETE = "PARTIALLY_COMPLETE"
    public static final String QUEUED = "QUEUED"
    public static final String WAITING__PROCESSING = "WAITING_PROCESSING"
    public static final String CORRUPT__AVRO__FILES = "CORRUPT_AVRO_FILES"
    public static final String INVALID = "INVALID"
    public static final String UNZIPPED = "UNZIPPED"
    public static final String MULTIMEDIA_ITEMS = "multimediaItems"

    def imageService
    def settingService
    def grailsApplication

    String createNewBatch() {
        BatchStatus status = new BatchStatus()

        synchronized (_Batches) {
            _Batches[status.batchId] = status
        }

        return status.batchId
    }

    void addTaskToBatch(String batchId, BackgroundTask task) {

        synchronized (_Batches) {
            if (!_Batches.containsKey(batchId)) {
                throw new RuntimeException("Unknown or invalid batch id!")
            }
            def status = _Batches[batchId]
            def batchTask = new BatchBackgroundTask(batchId, status.taskCount, task, this)
            status.taskCount = status.taskCount + 1

            imageService.scheduleBackgroundTask(batchTask)
        }
    }

    void notifyBatchTaskComplete(String batchId, int taskSequenceNumber, Object result) {

        if (!_Batches.containsKey(batchId)) {
            return
        }

        def status = _Batches[batchId]
        status.tasksCompleted = status.tasksCompleted + 1
        status.results[taskSequenceNumber] = result

        if (status.tasksCompleted == status.taskCount) {
            status.timeFinished = new Date()
        }
    }

    BatchStatus getBatchStatus(String batchId) {

        if (!_Batches.containsKey(batchId)) {
            return null
        }

        return _Batches[batchId]
    }

    void finaliseBatch(String batchId) {
        synchronized (_Batches) {
            if (!_Batches.containsKey(batchId)) {
                return
            }

            _Batches.remove(batchId)
        }
    }

    def getBatchFileUploadsFor(String dataResourceUid){
        BatchFileUpload.findAllByDataResourceUid(dataResourceUid)
    }

    def getBatchFileUpload(String uploadId){
        BatchFileUpload.findById(uploadId)
    }

    def getActiveBatchUploadCount(){
        BatchFile.countByStatusNotEqual(COMPLETE)
    }

    /**
     * Unpack the zip file.
     *
     * @param dataResourceUid
     * @param uploadedFile
     * @return
     */
    BatchFileUpload createBatchFileUploadsFromZip(String dataResourceUid, File uploadedFile){

        String md5Hash = ImageUtils.generateMD5(uploadedFile)
        BatchFileUpload upload = BatchFileUpload.findByMd5Hash(md5Hash)
        if (upload){
            // prevent multiple uploads of the same file
            return upload
        }

        upload = new BatchFileUpload(
                filePath: uploadedFile.getAbsolutePath(),
                md5Hash: md5Hash,
                dataResourceUid: dataResourceUid,
                status: "UNPACKING",
                message: "Unarchive zip file"

        )
        upload.save(flush:true)

        try {
            new ZipFile(uploadedFile).extractAll(uploadedFile.parentFile.absolutePath)

            File newDir = new File(grailsApplication.config.imageservice.batchUpload + "/" + upload.getId() + "/")
            if (!newDir.deleteDir()) {
                log.warn("Couldn't delete existing directory {} for batch upload {}", newDir)
            }
            uploadedFile.getParentFile().renameTo(newDir)
            upload.filePath = newDir.getAbsolutePath() + "/" +  uploadedFile.getName();
            upload.status = UNZIPPED
            upload.message = "Successfully unzipped"
            upload.save(flush:true)

            def batchFiles = []

            String message = ""
            //create BatchFileUpload jobs for each AVRO
            newDir.listFiles().each { File avroFile ->
                //read the file
                if (avroFile.getName().toLowerCase().endsWith(".avro")){
                    String md5HashBatchFile = ImageUtils.generateMD5(avroFile)
                    // have we seen this file before
                    BatchFile batchFile = BatchFile.findByMd5Hash(md5HashBatchFile)
                    if (!batchFile) {
                        batchFile = new BatchFile()
                        def result = validateAvro(avroFile)
                        batchFile.filePath = avroFile.getAbsolutePath()
                        batchFile.recordCount = result.recordCount
                        batchFile.status = batchFile.recordCount > 0 ? QUEUED : INVALID
                        batchFile.batchFileUpload = upload
                        batchFile.md5Hash = md5HashBatchFile
                        batchFiles << batchFile
                    } else {
                        message = "Ignoring previously uploaded files"
                    }
                }
            }

            upload.batchFiles = batchFiles as Set
            if (upload.batchFiles) {
                upload.message = "Awaiting processing"
                upload.status = WAITING__PROCESSING
            } else {
                upload.message = message
                upload.status = COMPLETE
            }

        } catch (Exception e){
            log.error("Problem unpacking zip " + e.getMessage(), e)
            upload.message = "Problem reading files: " + e.getMessage()
            upload.status = CORRUPT__AVRO__FILES
        }

        upload.save(flush:true)

        upload
    }

    Map validateAvro(File avroFile) {

        def inStream = new FileInputStream(avroFile)
        DataFileStream<GenericRecord> reader = new DataFileStream<>(inStream, new GenericDatumReader<GenericRecord>())
        long recordCount = 0

        // process record by record
        while (reader.hasNext() ) {
            GenericRecord currRecord = reader.next()
            // Here we can add in data manipulation like anonymization etc
            boolean hasField =  currRecord.hasField(MULTIMEDIA_ITEMS)
            if (hasField) {
                def multimediaRecords = currRecord.get(MULTIMEDIA_ITEMS)
                multimediaRecords.each { GenericRecord record ->
                    def identifier = record.get("identifier")
                    if (identifier) {
                        recordCount++;
                    }
                }
            } else {
                def identifier = currRecord.get("identifier")
                if (identifier) {
                    recordCount++;
                }                
            }
        }
        [recordCount: recordCount]
    }

    boolean batchEnabled(){
        def setting = Setting.findByName("batch.service.processing.enabled")
        setting.refresh()
        Boolean.parseBoolean(setting ? setting.value : "true")
    }

    private boolean loadBatchFile(BatchFile batchFile) {

        log.info("Loading batch file ${batchFile.id}: ${batchFile.filePath}")
        def start = Instant.now()

        def inStream = new FileInputStream(new File(batchFile.filePath))
        DataFileStream<GenericRecord> reader = new DataFileStream<>(inStream, new GenericDatumReader<GenericRecord>())

        def dataResourceUid = batchFile.batchFileUpload.dataResourceUid

        int processedCount = 0
        long newImageCount = 0
        long errorCount = 0
        long metadataUpdateCount = 0

        final int batchThreads = settingService.getBatchServiceThreads().intValue()
        final Long batchThrottleInMillis = settingService.getBatchServiceThrottleInMillis()
        final int batchReadSize = settingService.getBatchServiceReadSize().intValue()

        boolean completed = false

        while (reader.hasNext() && batchEnabled()){

            int batchSize = 0
            def batch = []
            def identifiers = []

            // read a batch of records
            while (reader.hasNext() && batchSize < batchReadSize) {
                GenericRecord currRecord = reader.next()

                boolean hasField =  currRecord.hasField(MULTIMEDIA_ITEMS)
                if (hasField) {
                    def multimediaRecords = currRecord.get(MULTIMEDIA_ITEMS);
                    // Here we can add in data manipulation like anonymization etc
                    multimediaRecords.each { GenericRecord record ->
                        // check URL
                        if (record.get("identifier")) {

                            if (!identifiers.contains(record.get("identifier"))) {

                                def recordMap = [
                                        dataResourceUid: dataResourceUid,
                                        identifier     : record.get("identifier")
                                ]

                                ImageService.SUPPORTED_UPDATE_FIELDS.each { updateField ->
                                    recordMap[updateField] = record.hasField(updateField) ? record.get(updateField) : null
                                }

                                batch << recordMap
                                identifiers << record.get("identifier")

                                processedCount++;
                            }
                        }
                    }
                } else {

                    def identifier = currRecord.get("identifier")

                    if (identifier && !identifiers.contains(identifier)) {

                        def recordMap = [
                                dataResourceUid: dataResourceUid,
                                identifier     : identifier
                        ]

                        ImageService.SUPPORTED_UPDATE_FIELDS.each { updateField ->
                            recordMap[updateField] = currRecord.hasField(updateField) ? currRecord.get(updateField) : null
                        }

                        batch << recordMap
                        identifiers << identifier

                        processedCount++;
                    }                    
                }
                batchSize ++
            }
            List<Map<String, Object>> results = Collections.synchronizedList(new ArrayList<Map<String, Object>>());

            GParsPool.withPool(batchThreads) {
                batch.eachParallel { image ->
                    results << execute(image, "batch-update")
                    if (batchThrottleInMillis > 0){
                        Thread.sleep(batchThrottleInMillis)
                    }
                }
            }

            //get counts
            results.each { result ->
                if (result) {
                    if (!result.success) {
                        errorCount ++
                    } else {
                        if (!result.alreadyStored) {
                            newImageCount++
                        }
                        if (result.metadataUpdated) {
                            metadataUpdateCount++
                        }
                    }
                }
            }

            batchFile.metadataUpdates = metadataUpdateCount
            batchFile.newImages = newImageCount
            batchFile.errorCount = errorCount
            batchFile.processedCount = processedCount
            batchFile.timeTakenToLoad = Duration.between(start, Instant.now()).toMillis() / 1000
            batchFile.save(flush:true)

            completed = !reader.hasNext()
        }

        if (completed) {
            log.info("Completed loading of batch file ${batchFile.id}: ${batchFile.filePath}")
            // TODO Delete .avro / .zip files?
        } else {
            log.info("Exiting the loading of batch file ${batchFile.id}:  ${batchFile.filePath}, complete: ${completed}")
        }
        completed
    }

    Map execute(Map imageSource,  String _userId) {

        try {
            def imageUpdateResult = imageService.uploadImage(imageSource, _userId)
            if (imageUpdateResult && imageUpdateResult.success && imageUpdateResult.image && !imageUpdateResult.isDuplicate) {
                if (imageUpdateResult.metadataUpdated) {
                    imageService.scheduleImageIndex(imageUpdateResult.image.id)
                }

                if (!imageUpdateResult.alreadyStored) {
                    imageService.scheduleArtifactGeneration(imageUpdateResult.image.id, _userId)
                    imageService.scheduleImageIndex(imageUpdateResult.image.id)
                    //Only run for new images......
                    imageService.scheduleImageMetadataPersist(
                            imageUpdateResult.image.id,
                            imageUpdateResult.image.imageIdentifier,
                            imageUpdateResult.image.originalFilename,
                            MetaDataSourceType.SystemDefined,
                            _userId)
                }
            }
            imageUpdateResult
        } catch (Exception e){
            log.error("Problem saving image: " + imageSource + " - Problem:" + e.getMessage())
            if (log.isDebugEnabled()){
                log.debug("Problem saving image: " + imageSource + " - Problem:" + e.getMessage(), e)
            }
            [success: false]
        }
    }

    def initialize(){
        BatchFile.findAllByStatus(LOADING).each {
            it.setStatus(STOPPED)
            it.save(flush:true)
        }
    }

    def processNextInQueue(){

        if (!settingService.getBatchServiceProcessingEnabled()){
            log.debug("Batch service is currently disabled")
            return
        }

        //is there an executing job ??
        def loading = BatchFile.findAllByStatus(LOADING)
        if (loading){
            return
        }

        log.debug("Checking batch file load job...")

        // Read a job from BatchFile - oldest file that is set to "READY_TO_LOAD"
        BatchFile batchFile = BatchFile.findByStatus(QUEUED, [sort: 'dateCreated', order: 'asc'])

        if (batchFile){

            // Read AVRO, download for new, enqueue for metadata updates....
            batchFile.status = LOADING
            batchFile.lastUpdated = new Date()
            batchFile.processedCount = 0
            batchFile.newImages = 0
            batchFile.metadataUpdates = 0
            batchFile.dateCompleted = null
            batchFile.save(flush:true)

            batchFile.batchFileUpload.status = LOADING
            batchFile.batchFileUpload.save(flush:true)

            //load batch file
            def start = Instant.now()
            def complete = loadBatchFile(batchFile)
            def millisTaken = Duration.between(start, Instant.now()).toMillis()

            Date now = new Date()
            if (complete) {
                batchFile.status = COMPLETE
                batchFile.lastUpdated = now
                batchFile.dateCompleted = now
                if (millisTaken)
                    batchFile.timeTakenToLoad = millisTaken / 1000
                else
                    batchFile.timeTakenToLoad = 0

                batchFile.save(flush: true)
            } else {
                batchFile.status = STOPPED
                batchFile.lastUpdated = now
                batchFile.save(flush: true)
            }

            // if all loaded, mark as complete
            boolean allComplete = batchFile.batchFileUpload.batchFiles.every { it.status == COMPLETE }
            if (allComplete){
                batchFile.batchFileUpload.message = "All files processed"
                batchFile.batchFileUpload.status =  COMPLETE
                batchFile.batchFileUpload.dateCompleted = now
            } else {
                batchFile.batchFileUpload.message = "Some files processed"
                batchFile.batchFileUpload.status = PARTIALLY__COMPLETE
            }
        } else {
            log.debug("No jobs to run.")
        }
    }

    def reloadFile(fileId){
        BatchFile batchFile = BatchFile.findById(fileId)
        if (batchFile){
            batchFile.status = QUEUED
            batchFile.save(flush:true)
        }
    }

    def deleteFileFromQueue(fileId){
        BatchFile batchFile = BatchFile.findById(fileId)
        if (batchFile){
            batchFile.delete(flush:true)
        }
    }

    def clearFileQueue(){
        BatchFile.findAllByStatusNotEqual(LOADING).each {
            it.delete(flush:true)
        }
    }

    def clearUploads(){
        BatchFileUpload.findAllByStatusNotEqual(LOADING).each {
            it.delete(flush:true)
        }
    }

    def getUploads(){
        BatchFileUpload.findAll([sort:'id', order: 'asc'])
    }

    def getNonCompleteFiles(){
        def list = BatchFile.findAllByStatus(COMPLETE, [sort:'id', order: 'asc'])
        list.sort(new Comparator<BatchFile>() {
            @Override
            int compare(BatchFile o1, BatchFile o2) {
                if(o1.status == LOADING && o2.status != LOADING) {
                    return 1
                } else if (o1.status != LOADING && o2.status == LOADING){
                    return -1
                } else {
                    return 0
                }
            }
        })
        list
    }

    def getQueuedFiles(){
        BatchFile.findAllByStatusInList([QUEUED, STOPPED], [sort:'dateCreated', order: 'asc'])
    }

    def getActiveFiles(){
        BatchFile.findAllByStatus(LOADING, [sort:'dateCreated', order: 'asc'])
    }

    def getFilesForUpload(uploadId){
        BatchFileUpload upload = BatchFileUpload.findById(uploadId)
        if (upload) {
            BatchFile.findAllByBatchFileUpload(upload, [sort: 'id', order: 'asc'])
        } else {
            []
        }
    }

    def getFiles(){
        BatchFile.findAll([sort:'id', order: 'asc'])
    }

    def purgeCompletedJobs(){
        ZonedDateTime now = ZonedDateTime.now()
        ZonedDateTime threeDaysAgo = now.minusDays(grailsApplication.config.purgeCompletedAgeInDays.toInteger())

        //remove batch files
        BatchFile.findAllByStatus(COMPLETE).each {
            if (it.dateCompleted.toInstant().isBefore(threeDaysAgo.toInstant())) {
                it.delete(flush: true)
            }
        }

        //remove batch file uploads
        BatchFileUpload.findAllByStatus(COMPLETE).each {
            if (it.dateCompleted.toInstant().isBefore(threeDaysAgo.toInstant())) {
                it.delete(flush: true)
            }
        }

        //remove batch file uploads with zero files
        BatchFileUpload.findAll().each {
            if (!it.batchFiles ) {
                it.delete(flush: true)
            }
        }
    }
}
