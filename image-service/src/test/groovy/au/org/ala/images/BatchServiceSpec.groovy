package au.org.ala.images

import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import spock.lang.Specification

class BatchServiceSpec extends Specification implements ServiceUnitTest<BatchService>, DataTest {

    def testFile = null

    def setup() {
        mockDomains BatchFile, BatchFileUpload
        testFile = AvroUtils.generateTestArchive()
    }

    def "test avro upload"() {
        setup:

        when:
        BatchFileUpload batchFileUpload = service.createBatchFileUploadsFromZip("dr1", testFile)

        then:
        batchFileUpload != null
        !batchFileUpload.batchFiles.isEmpty()
        batchFileUpload.batchFiles.size() == 1
        batchFileUpload.batchFiles[0].recordCount == 1
    }
}
