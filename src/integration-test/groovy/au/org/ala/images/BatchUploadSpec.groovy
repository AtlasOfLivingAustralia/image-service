package au.org.ala.images

import grails.core.GrailsApplication
import grails.testing.mixin.integration.Integration
import grails.gorm.transactions.Rollback
import groovy.json.JsonSlurper
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.BlockingHttpClient
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.multipart.MultipartBody
import spock.lang.Specification

import static au.org.ala.images.AvroUtils.AUDIENCE
import static au.org.ala.images.AvroUtils.CREATED
import static au.org.ala.images.AvroUtils.CREATOR
import static au.org.ala.images.AvroUtils.IDENTIFIER

@Integration(applicationClass = Application.class)
@Rollback
class BatchUploadSpec extends Specification {

    static final int TIMEOUT_SECONDS = 60
    static final String TEST_DR_UID = 'test-123'


    GrailsApplication grailsApplication

    private BlockingHttpClient getRest() {
        HttpClient.create(baseUrl).toBlocking()
    }

    private URL getBaseUrl() {
        def serverContextPath = grailsApplication.config.getProperty('server.servlet.contextPath', String, '')
        def url = "http://localhost:${serverPort}${serverContextPath}"
        return url.toURL()
    }

    /**
     * Test an AVRO upload with only a single record record in the AVRO file
     */
    def uploadSingleFileSingleImageAvro() {
        given:
        def imageUrl = 'https://www.ala.org.au/app/uploads/2019/05/palm-cockatoo-by-Alan-Pettigrew-1920-1200-CCBY-28072018-640x480.jpg'
        def imageUrls = [[[(IDENTIFIER): imageUrl, (CREATOR): 'creator', (CREATED): '2021-01-01 00:00:00']]]
        def avro = AvroUtils.generateTestArchiveWithMetadata(imageUrls, true)

        when:

        def request = HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build())
        HttpResponse uploadResponse = rest.exchange(request, String)

        def response = new JsonSlurper().parseText(uploadResponse.body())

        // wait for batch files and images to be created
        int start = System.currentTimeSeconds()
        // Poll until the image tiler has run on the last image in the batch upload
        def upload = findBatchFileUpload(response.batchID, start)
        def image = findImage(imageUrl, true, start)

        then:
        checkCommonResponse(uploadResponse, response, imageUrls, TEST_DR_UID)

        checkBatchFileUpload(upload, TEST_DR_UID, 1)

        // Check that the image was created
        image.originalFilename == imageUrl
        image.mimeType == 'image/jpeg'
        image.dateDeleted == null
        image.creator == 'creator'
        image.created == '2021-01-01 00:00:00'
        image.zoomLevels > 0 // Indicates that the tiler ran

    }

    /**
     * Test an AVRO upload with multiple images in multiple AVRO files
     */
    def uploadMultiFileMultiImageAvro() {
        given:
        def imageUrls = [
                [
                        'https://www.ala.org.au/app/uploads/2020/07/jun202.1-1920x910.png',
                        'https://www.ala.org.au/app/uploads/2020/07/jan2032.1-1920x749.png'
                ],
                [
                        'https://www.ala.org.au/app/uploads/2020/07/feb2013.1.png',
                        'https://profiles.ala.org.au/opus/0ded7a77-9efb-4684-8df0-48cbb1933684/image/sqlnq9itgaxtqv7psyvi.png'
                ]
        ]
        def imageUrlsSet = imageUrls.flatten() as Set
        def avro = AvroUtils.generateTestArchive(imageUrls)

        when:

        HttpResponse uploadResponse = rest.exchange(HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build()), String)

        def response = new JsonSlurper().parseText(uploadResponse.body())

        // wait for batch files and images to be created
        int start = System.currentTimeSeconds()
        def origFilename = 'https://www.ala.org.au/app/uploads/2019/05/palm-cockatoo-by-Alan-Pettigrew-1920-1200-CCBY-28072018-640x480.jpg'
        // Poll until the image tiler has run on the last image in the batch upload
        def upload = findBatchFileUpload(response.batchID, start)
        def images = imageUrls.flatten().collect {

            findImage(it, true, start)
        }

        then:
        checkCommonResponse(uploadResponse, response, imageUrls, TEST_DR_UID)

        checkBatchFileUpload(upload, TEST_DR_UID,2)

        images.size() == 4

        // Check that the image was created
        for (int i = 0; i < images.size(); ++i) {
            images[i] != null
            imageUrlsSet.contains(images[i].originalFilename)
            images[i].mimeType == 'image/png'
            images[i].dateDeleted == null
            images[i].zoomLevels > 0 // Indicates that the tiler ran

        }

    }

    /**
     * Test an upload with repeated image URLs
     */
    def uploadRepeatedImagesTest() {
        given:
        def imageUrls = [
                [
                        'https://www.ala.org.au/app/uploads/2020/07/jun202.1-1920x910.png',
                        'https://www.ala.org.au/app/uploads/2020/07/jan2032.1-1920x749.png'
                ],
                [
                        'https://www.ala.org.au/app/uploads/2020/07/jun202.1-1920x910.png',
                        'https://www.ala.org.au/app/uploads/2020/07/jan2032.1-1920x749.png'
                ]
        ]
        def imageUrlsSet = imageUrls.flatten() as Set
        def avro = AvroUtils.generateTestArchive(imageUrls)

        when:

        HttpResponse uploadResponse = rest.exchange(HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build()), String)

        def response = new JsonSlurper().parseText(uploadResponse.body())

        // wait for batch files and images to be created
        int start = System.currentTimeSeconds()
        // Poll until the image tiler has run on the last image in the batch upload
        def upload = findBatchFileUpload(response.batchID, start)
        def images = imageUrls.flatten().toSet().collect {
            findImage(it, true, start)
        }

        then:
        checkCommonResponse(uploadResponse, response, imageUrls, TEST_DR_UID)

        checkBatchFileUpload(upload, TEST_DR_UID,2)

        images.size() == 2

        // Check that the image was created
        for (int i = 0; i < images.size(); ++i) {
            images[i] != null
            imageUrlsSet.contains(images[i].originalFilename)
            images[i].mimeType == 'image/png'
            images[i].dateDeleted == null
            images[i].zoomLevels > 0 // Indicates that the tiler ran

        }
    }

    /**
     * Test an upload with repeated images at different URLs
     */
    def uploadAlternateUrlDuplicateImagesTest() {
        given:
        def imageUrls = [
                [
                        [(IDENTIFIER): 'https://www.ala.org.au/app/uploads/2020/07/jun202.1-1920x910.png', (AUDIENCE): 'audience']
                ],
                [
                        [(IDENTIFIER): 'https://www.ala.org.au/app/uploads/2020/07/jun202.1-1920x910.png?test', (AUDIENCE): 'audience2']
                ],
                [
                        [(IDENTIFIER): 'https://www.ala.org.au/app/uploads/2020/07/jun202.1-1920x910.png?test2', (AUDIENCE): 'audience3']
                ]
        ]
        def avro = AvroUtils.generateTestArchiveWithMetadata(imageUrls, false)

        when:

        HttpResponse uploadResponse = rest.exchange(HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build()), String)

        def response = new JsonSlurper().parseText(uploadResponse.body())

        // wait for batch files and images to be created
        int start = System.currentTimeSeconds()
        // Poll until the image tiler has run on the last image in the batch upload
        def upload = findBatchFileUpload(response.batchID, start)

        def originalImage = findImage(imageUrls[0][0][(IDENTIFIER)], true, start)

        then:
        checkCommonResponse(uploadResponse, response, imageUrls, TEST_DR_UID)

        checkBatchFileUpload(upload, TEST_DR_UID, 3)

        // One duplicate created per additional image
        originalImage != null
        originalImage.mimeType == 'image/png'
        originalImage.dateDeleted == null
        originalImage.audience == 'audience'
        originalImage.zoomLevels > 0 // Indicates that the tiler ran
        originalImage.alternateFilename != null
        originalImage.alternateFilename.size() == 2
        originalImage.alternateFilename as Set == [imageUrls[1][0][(IDENTIFIER)], imageUrls[2][0][(IDENTIFIER)]] as Set

        when: "uploading a duplicate image again, the duplicate is detected before it is downloaded again"

        uploadResponse = rest.exchange(HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build()), String)

        response = new JsonSlurper().parseText(uploadResponse.body())

        // wait for batch files and images to be created
        start = System.currentTimeSeconds()
        // Poll until the image tiler has run on the last image in the batch upload
        upload = findBatchFileUpload(response.batchID, start)

        originalImage = findImage(imageUrls[0][0][(IDENTIFIER)], true, start)

        then:
        checkCommonResponse(uploadResponse, response, imageUrls, TEST_DR_UID)

        checkBatchFileUpload(upload, TEST_DR_UID, 3)

        // One duplicate created per additional image
        originalImage != null
        originalImage.mimeType == 'image/png'
        originalImage.dateDeleted == null
        originalImage.audience == 'audience'
        originalImage.zoomLevels > 0 // Indicates that the tiler ran
        originalImage.alternateFilename != null
        originalImage.alternateFilename.size() == 2 // Check the number of alternate filenames has not increased.
        originalImage.alternateFilename as Set == [imageUrls[1][0][(IDENTIFIER)], imageUrls[2][0][(IDENTIFIER)]] as Set

    }

    /**
     * Test an AVRO upload with a broken URL
     */
    def uploadMissingImageAvro() {
        given:
        def imageUrl = 'https://www.ala.org.au/app/uploads/404/404/notfound.jpg'
        def imageUrls = [[imageUrl]]
        def avro = AvroUtils.generateTestArchive(imageUrls)

        when:

        HttpResponse uploadResponse = rest.exchange(HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build()), String)

        def response = new JsonSlurper().parseText(uploadResponse.body())

        // wait for batch files and images to be created
        int start = System.currentTimeSeconds()
        // Poll until the image tiler has run on the last image in the batch upload
        def upload = findBatchFileUpload(response.batchID, start)
//        def image = findImage(imageUrl, true, start)
        def badUrl = findFailedUpload(imageUrl, start)

        then:
        checkCommonResponse(uploadResponse, response, imageUrls, TEST_DR_UID)

        checkBatchFileUpload(upload, TEST_DR_UID, 1)

        // Check that the failed upload was created
        badUrl != null

    }

    /**
     * Test an AVRO upload with codec in AVRO file
     */
    def uploadFileWithCodecAvro() {
        given:
        def imageUrl = 'https://www.ala.org.au/app/uploads/2019/05/palm-cockatoo-by-Alan-Pettigrew-1920-1200-CCBY-28072018-640x480.jpg'
        def imageUrls = [[[(IDENTIFIER): imageUrl, (CREATOR): 'creator', (CREATED): '2021-01-01 00:00:00']]]
        def avro = AvroUtils.generateTestArchiveWithMetadata(imageUrls, true, true)

        when:

        def request = HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build())
        HttpResponse uploadResponse = rest.exchange(request, String)

        def response = new JsonSlurper().parseText(uploadResponse.body())

        then:
        response.batchID != null

    }

    /**
     * Test an AVRO upload without rights field in the AVRO file
     */
    def uploadImageAvroWithoutRightsField() {
        given:
        def imageUrl = 'https://www.ala.org.au/app/uploads/2019/05/palm-cockatoo-by-Alan-Pettigrew-1920-1200-CCBY-28072018-640x480.jpg'
        def imageUrls = [[[(IDENTIFIER): imageUrl, (CREATOR): 'creator', (CREATED): '2021-01-01 00:00:00']]]
        def avro = AvroUtils.generateTestArchiveWithMetadata(imageUrls, true, false, true)

        when:

        def request = HttpRequest.create(HttpMethod.POST, "/batch/upload")
                .contentType("multipart/form-data")
                .body(MultipartBody.builder()
                        .addPart("dataResourceUid", TEST_DR_UID)
                        .addPart('archive', avro)
                        .build())
        HttpResponse uploadResponse = rest.exchange(request, String)

        def response = new JsonSlurper().parseText(uploadResponse.body())

        // wait for batch files and images to be created
        int start = System.currentTimeSeconds()
        // Poll until the image tiler has run on the last image in the batch upload
        def upload = findBatchFileUpload(response.batchID, start)
        def image = findImage(imageUrl, true, start)

        then:
        checkCommonResponse(uploadResponse, response, imageUrls, TEST_DR_UID)

        checkBatchFileUpload(upload, TEST_DR_UID, 1)

        // Check that the image was created
        image.originalFilename == imageUrl
        image.mimeType == 'image/jpeg'
        image.dateDeleted == null
        image.creator == 'creator'
        image.created == '2021-01-01 00:00:00'
        image.zoomLevels > 0 // Indicates that the tiler ran

    }

    // TODO Use a notification / event bus to signal that batch / images are complete?
    private BatchFileUpload findBatchFileUpload(int batchFileUploadId, int startTimeSeconds) {
        def upload = BatchFileUpload.get(batchFileUploadId)
        while ((upload == null || upload.status != BatchService.COMPLETE) && since(startTimeSeconds) <= TIMEOUT_SECONDS) {
            Thread.sleep(500)
            upload = upload == null ? BatchFileUpload.get(batchFileUploadId) : upload.refresh()
        }
        return upload
    }

    private Image findImage(String originalFilename, boolean checkZoomLevel, int startTimeSeconds) {
        def image = Image.findByOriginalFilename(originalFilename)
        while ((image == null || (checkZoomLevel && image?.zoomLevels == 0)) && since(startTimeSeconds) <= TIMEOUT_SECONDS) {
            Thread.sleep(500)
            image = image == null ? Image.findByOriginalFilename(originalFilename) : image.refresh()
        }
        return image
    }

    private FailedUpload findFailedUpload(String url, int startTimeSeconds) {
        def failedUpload = FailedUpload.findByUrl(url)
        while (failedUpload == null && since(startTimeSeconds) <= TIMEOUT_SECONDS) {
            Thread.sleep(500)
            failedUpload = failedUpload == null ? FailedUpload.findByUrl(url) : failedUpload.refresh()
        }
        return failedUpload
    }

    private void checkCommonResponse(HttpResponse uploadResponse, Map response, List<List<?>> imageUrls, String dataResourceUid) {
        uploadResponse.status == HttpStatus.OK

        // Check the response from the server
        response.batchID != null
        response.batchID > 0
        response.dataResourceUid == dataResourceUid // TODO?
        response.file != null
        response.dateCreated != null
        response.status == BatchService.WAITING__PROCESSING
        response.files != null

        response.files.size() == imageUrls.size()
        for (int i = 0; i < imageUrls.size(); ++i) {
            response.files[i].status == BatchService.QUEUED
            response.files[i].file != null
            response.files[i].recordCount == imageUrls[i].size()
        }
    }

    private void checkBatchFileUpload(BatchFileUpload upload, String dataResourceUid, int expectedRecords) {
        // Check that the upload has completed successfully
        upload.status == BatchService.COMPLETE
        upload.getDataResourceUid() == dataResourceUid
        upload.batchFiles.size() == expectedRecords
    }

    private int since(int start) {
        System.currentTimeSeconds() - start
    }
}
