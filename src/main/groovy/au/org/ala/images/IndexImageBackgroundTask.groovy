package au.org.ala.images

import com.opencsv.CSVReaderBuilder
import com.opencsv.RFC4180ParserBuilder
import grails.gorm.transactions.Transactional
import groovy.util.logging.Slf4j

//import org.apache.log4j.Logger

class IndexImageBackgroundTask extends BackgroundTask {

    private long _imageId
    private ElasticSearchService _elasticSearchService

    IndexImageBackgroundTask(long imageId, ElasticSearchService elasticSearchService) {
        _imageId = imageId
        _elasticSearchService = elasticSearchService
    }

    @Override
    void execute() {
        def imageInstance
        Image.withTransaction {
            imageInstance = Image.findById(_imageId, [fetch: [recognisedLicense: 'eager']])
        }
        if (imageInstance) {
            _elasticSearchService.indexImage(imageInstance)
        }
    }

    @Override
    String toString() {
        return "IndexImageBackgroundTask," + _imageId
    }
}

@Slf4j
class ScheduleReindexAllImagesTask extends BackgroundTask {

    private ImageService _imageService
    private ElasticSearchService _elasticSearchService
    private int _batchIndexSize = 1000
//    private Logger log = Logger.getLogger(ScheduleReindexAllImagesTask.class)

    ScheduleReindexAllImagesTask(ImageService imageService, ElasticSearchService elasticSearchService, int batchIndexSize) {
        _imageService = imageService
        _elasticSearchService = elasticSearchService
        _batchIndexSize = batchIndexSize
    }

    @Override
    void execute() {

        try {
            log.info("Starting CSV export for full index generation")
            def file = _imageService.exportIndexToFile()
            log.info("CSV export complete. Deleting existing index")
            // where we interrupted during CSV export?
            if (Thread.currentThread().isInterrupted()) {
                log.info("Interrupted during index CSV export, not continuing")
                return
            }
            _imageService.deleteIndex()
            log.info("Deleting existing index. Done.")

            def csvReader = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(file)))
                    .withCSVParser(new RFC4180ParserBuilder().build())
                    .build()
            def headers = csvReader.readNext()
            def line = csvReader.readNext()
            def i = 1
            def start = System.currentTimeMillis()
            def startOfProcess = start
            def batch = []
            log.info("Starting file read: ${file.getAbsolutePath()}")
            while (line) {
                try {
                    def record = [:]
                    if (line.length == headers.length) {
                        line.eachWithIndex { field, idx ->
                            if (field) {
                                record[headers[idx]] = field
                            }
                        }
                        batch << record
                        if (i % _batchIndexSize == 0) {
                            def lastBatch = System.currentTimeMillis() - start
                            _elasticSearchService.bulkIndexImageInES(batch)
                            batch.clear()
                            log.info("Indexing images:  ${i}. Last ${_batchIndexSize} in time: ${lastBatch} ms")
                            start = System.currentTimeMillis()
                        }
                    } else {
                        log.error("Problem with line: ${i}, incorrect number of fields, expected ${headers.length}, actual ${line.length}")
                    }
                } catch (Exception e) {
                    log.error("Problem indexing batch at count: ${i}, - " + e.getMessage(), e)
                    log.error("Retrying batch....")
                    try {
                        _elasticSearchService.bulkIndexImageInES(batch)
                        batch.clear()
                        log.error("Retry successful....")
                    } catch (Exception e2) {
                        log.error("Retry failed. Dumping batch to log file")
                        log.error("####################### Problem batch start #############################")
                        def batchDebug = ""
                        batch.each {
                            batchDebug = batchDebug + it + "\n"
                        }
                        log.error(batchDebug)
                        log.error("####################### Problem batch end #############################")
                    }
                }
                i += 1
                line = csvReader.readNext()
            }

            def lastBatch = System.currentTimeMillis() - startOfProcess
            _elasticSearchService.bulkIndexImageInES(batch)
            batch.clear()
            log.info("Indexing images complete. Total indexed: " + i + " total time: " + lastBatch)

        } catch (e) {
            log.error("Exception rebuilding search index", e)
        }
    }
}
