package au.org.ala.images

import grails.testing.web.controllers.ControllerUnitTest
import org.grails.plugins.testing.GrailsMockMultipartFile
import spock.lang.Specification

class BatchControllerSpec extends Specification implements ControllerUnitTest<BatchController> {

    def uploadFile = null

    def setup() {
        uploadFile = AvroUtils.generateTestArchive()
    }

    def "test avro zip file upload - missing zip file"() {
        setup:
        controller.batchService = Mock(BatchService)

        when:
        controller.upload()

        then:
        response.status == 400
    }

    def "test avro zip file upload - missing data resource uid"() {
        setup:
        controller.batchService = Mock(BatchService)

        def multipartFile = new GrailsMockMultipartFile(
                'archive',
                'data.avro.zip',
                'application/zip', uploadFile.bytes)
        request.addFile(multipartFile)

        when:
        controller.upload()

        then:
        response.status == 400
    }

    def "test avro zip file upload - 200"() {
        setup:
        controller.batchService = Mock(BatchService)
        controller.batchService.createBatchFileUploadsFromZip( _ as String, _ as File) >>  { String dataResourceUid, File uploadFile ->
            return new BatchFileUpload(id:"1")
        }

        def multipartFile = new GrailsMockMultipartFile(
                'archive',
                'data.avro.zip',
                'application/zip',
                uploadFile.bytes)
        request.addFile(multipartFile)
        params.dataResourceUid = 'dr1'

        when:
        controller.upload()

        then:
        response.status == 200
    }
}
