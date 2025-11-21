package au.org.ala.images

import grails.gorm.transactions.NotTransactional
import groovyx.gpars.GParsPool
import jsr166y.ForkJoinPool
import net.lingala.zip4j.ZipFile
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord

import javax.annotation.PreDestroy
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
    public static final String UNPACKING = "UNPACKING"
    public static final String MULTIMEDIA_ITEMS = "multimediaItems"
    public static final String BATCH_UPDATE_USERNAME = "batch-update"

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
                status: UNPACKING,
                message: "Unarchive zip file"

        )
        upload.save(flush:true)

        try {
            new ZipFile(uploadedFile).extractAll(uploadedFile.parentFile.absolutePath)

            File newDir = new File(grailsApplication.config.getProperty('imageservice.batchUpload') + "/" + upload.getId() + "/")
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
                if (upload.batchFiles.every { it.recordCount == 0 }) {
                    upload.status = COMPLETE
                    upload.dateCompleted = new Date()
                    upload.message = "No valid records found"
                } else {
                    upload.message = "Awaiting processing"
                    upload.status = WAITING__PROCESSING
                }
            } else {
                upload.message = message
                upload.status = COMPLETE
                upload.dateCompleted = new Date()
            }

        } catch (Exception e) {
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

    private volatile ForkJoinPool pool
    private final def $lock = new Object[0]

    private def getBackgroundTasksPool(int batchThreads) {
        // TODO update pool if batchThreads changes
        if (!pool) {
            synchronized ($lock) {
                if (!pool) {
                    pool = new ForkJoinPool(batchThreads)
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

    private boolean loadBatchFile(long id, String filePath, String dataResourceUid) {

        log.info("Loading batch file ${id}: ${filePath}")
        def start = Instant.now()

        def inStream = new FileInputStream(new File(filePath))
        DataFileStream<GenericRecord> reader = new DataFileStream<>(inStream, new GenericDatumReader<GenericRecord>())

//        def dataResourceUid = dataResourceUid

        int processedCount = 0
        long newImageCount = 0
        long errorCount = 0
        long metadataUpdateCount = 0

        final int batchThreads = settingService.getBatchServiceThreads().intValue()
        final Long batchThrottleInMillis = settingService.getBatchServiceThrottleInMillis()
        final int batchReadSize = settingService.getBatchServiceReadSize().intValue()

        boolean completed = false

        while (reader.hasNext() && batchEnabled()) {

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
//            List<Map<String, Object>> results //= Collections.synchronizedList(new ArrayList<Map<String, Object>>());

            // TODO: Use virtual threads
            //def pool = Executors.newVirtualThreadPerTaskExecutor()
            List<Map<String, Object>> results = GParsPool.withExistingPool(getBackgroundTasksPool(batchThreads)) {
                batch.collectParallel { image ->
                    def result = execute(image, BATCH_UPDATE_USERNAME)
                    if (batchThrottleInMillis > 0){
                        Thread.sleep(batchThrottleInMillis)
                    }
                    result
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

            BatchFile.withNewTransaction {
                BatchFile batchFile = BatchFile.get(id)
                batchFile.metadataUpdates = metadataUpdateCount
                batchFile.newImages = newImageCount
                batchFile.errorCount = errorCount
                batchFile.processedCount = processedCount
                batchFile.timeTakenToLoad = Duration.between(start, Instant.now()).toMillis() / 1000
                batchFile.save()
            }

            completed = !reader.hasNext()
        }

        if (completed) {
            log.info("Completed loading of batch file ${id}: ${filePath}")
            // TODO Delete .avro / .zip files?
        } else {
            log.info("Exiting the loading of batch file ${id}:  ${filePath}, complete: ${completed}")
        }
        return completed
    }

    @NotTransactional // transactions managed in method
    Map execute(Map imageSource,  String _userId) {

        try {
            def imageUpdateResult = imageService.uploadImage(imageSource, _userId)
            if (imageUpdateResult && imageUpdateResult.success && imageUpdateResult.image && !imageUpdateResult.isDuplicate) {
                if (imageUpdateResult.metadataUpdated) {
                    imageService.scheduleImageIndex(imageUpdateResult.image.id)
                }

                if (!imageUpdateResult.alreadyStored) {
//                    imageService.scheduleArtifactGeneration(imageUpdateResult.image.id, _userId)
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
            // not sure where this exception comes from
            if (e.message.startsWith('query did not return a unique result'))  {
                log.error("Problem saving image: " + imageSource + " - Problem:" + e.getMessage(), e)
            } else {
                log.error("Problem saving image: " + imageSource + " - Problem: " + e.class.name + " " + e.message)
                if (log.isDebugEnabled()){
                    log.debug("Problem saving image: " + imageSource + " - Problem: " + e.class.name + " " + e.message, e)
                }
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

    @NotTransactional
    def processNextInQueue() {

        def continueProcessing = true
        long id
        String filePath
        String dataResourceUid
        BatchFile.withNewTransaction {

            if (!settingService.getBatchServiceProcessingEnabled()) {
                log.debug("Batch service is currently disabled")
                continueProcessing = false
                return
            }

            //is there an executing job ??
            def loading = BatchFile.findAllByStatus(LOADING)
            if (loading) {
                log.debug("Batch service is currently processing a job")
                continueProcessing = false
                return
            }

            // Read a job from BatchFile - oldest file that is set to "READY_TO_LOAD"
            BatchFile batchFile = BatchFile.findByStatus(QUEUED, [sort: 'dateCreated', order: 'asc'])

            log.debug("Checking batch file load job...")
            if (batchFile) {
                id = batchFile.id
                filePath = batchFile.filePath
                dataResourceUid = batchFile.batchFileUpload.dataResourceUid

                // Read AVRO, download for new, enqueue for metadata updates....
                batchFile.status = LOADING
                batchFile.lastUpdated = new Date()
                batchFile.processedCount = 0
                batchFile.newImages = 0
                batchFile.metadataUpdates = 0
                batchFile.dateCompleted = null
                batchFile.save()

                BatchFileUpload.executeUpdate("update BatchFileUpload set status = :status where id = :id and status != :status", [status: LOADING, id: batchFile.batchFileUpload.id])
//                batchFile.batchFileUpload.status = LOADING
//                batchFile.batchFileUpload.save()

                return
            } else {
                log.debug("No jobs to run.")
                continueProcessing = false
                return
            }
        }

        if (!continueProcessing) {
            return
        }

        //load batch file
        def start = Instant.now()
        def complete = loadBatchFile(id, filePath, dataResourceUid)
        def millisTaken = Duration.between(start, Instant.now()).toMillis()

        Date now = new Date()
        BatchFile.withNewTransaction {

            BatchFile batchFile = BatchFile.get(id)

            if (complete) {
                batchFile.status = COMPLETE
                batchFile.lastUpdated = now
                batchFile.dateCompleted = now

                batchFile.timeTakenToLoad = millisTaken ? millisTaken / 1000 : 0
            } else {
                batchFile.status = STOPPED
                batchFile.lastUpdated = now
            }
            batchFile.save()

            // if all loaded, mark as complete

            boolean allComplete = batchFile.batchFileUpload.batchFiles.every { it.status == COMPLETE }

            BatchFileUpload batchFileUpload = batchFile.batchFileUpload
            if (allComplete) {
                batchFileUpload.message = "All files processed"
                batchFileUpload.status =  COMPLETE
                batchFileUpload.dateCompleted = now
            } else {
                batchFileUpload.message = "Some files processed"
                batchFileUpload.status = PARTIALLY__COMPLETE
            }
            batchFileUpload.save()
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

    def getUploads(boolean hideEmptyBatchUploads = false) {
        List<Long> ids
        if (hideEmptyBatchUploads) {
            ids = BatchFileUpload.executeQuery("select bfu.id from BatchFileUpload bfu left join bfu.batchFiles bf where bf.status != 'INVALID' group by bfu.id")
            if (ids) {
                return BatchFileUpload.findAllByIdInList(ids, [sort: 'id', order: 'asc'])
            }
            else {
                return []
            }
        } else {
            return BatchFileUpload.findAll([sort: 'id', order: 'asc'])
        }
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
        ZonedDateTime threeDaysAgo = now.minusDays(grailsApplication.config.getProperty('purgeCompletedAgeInDays', Long))

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
