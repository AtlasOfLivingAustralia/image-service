package au.org.ala.images

import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import grails.web.mapping.LinkGenerator
import org.springframework.web.multipart.MultipartFile
import spock.lang.Specification
import java.nio.file.Files

class ImageStagingServiceSpec extends Specification implements ServiceUnitTest<ImageStagingService>, DataTest {

    def setup() {
        mockDomain(StagedFile)
        mockDomain(StagingColumnDefinition)
        def tempDir = Files.createTempDirectory("image-service-staging").toFile()
        tempDir.deleteOnExit()
        config.imageservice.imagestore.staging = tempDir.absolutePath
        service.grailsLinkGenerator = Mock(LinkGenerator) {
            link(_ as Map) >> '/staging/file'
        }
    }

    def "test uploadDataFile creates parent directory and not a directory at the file path (Finding 4)"() {
        given:
        def userId = "user1"
        def multipartFile = Mock(MultipartFile)
        def stagingDir = new File(config.imageservice.imagestore.staging as String, userId)
        def datafileDir = new File(stagingDir, "datafile")
        def dataFile = new File(datafileDir, "datafile.txt")

        when:
        service.uploadDataFile(userId, multipartFile)

        then:
        1 * multipartFile.transferTo(_ as File) >> { File f ->
             assert f.absolutePath == dataFile.absolutePath
             assert f.parentFile.exists()
             assert f.parentFile.isDirectory()
             assert !f.exists() // It should not be a directory!
        }
        dataFile.parentFile.exists()
        dataFile.parentFile.isDirectory()
    }

    def "test stageFile handles path traversal in originalFilename (Finding 1)"() {
        given:
        def userId = "user1"
        def multipartFile = Mock(MultipartFile)
        multipartFile.originalFilename >> "../../../traversal.txt"

        when:
        def stagedFile = service.stageFile(userId, multipartFile)

        then:
        stagedFile != null
        stagedFile.filename != "../../../traversal.txt"
        !stagedFile.filename.contains("..")
        
        def stagingDir = new File(config.imageservice.imagestore.staging as String, userId)
        def expectedFile = new File(stagingDir, stagedFile.filename)
        expectedFile.absolutePath.startsWith(stagingDir.absolutePath)
    }

    def "test combine prevents path traversal (Finding 1)"() {
        given:
        def base = "/tmp/base"
        def child = "../../etc/passwd"

        when:
        ImageStagingService.combine(base, child)

        then:
        thrown(RuntimeException)
    }

    def "buildStagedImageData maps a BOM-prefixed CSV fixture through headers, blanks and EOF"() {
        given:
        String userId = 'csv-user'
        def stagedFile = new StagedFile(
                userId: userId,
                filename: 'fixture-one.jpg',
                dateStaged: new Date()
        ).save(flush: true, failOnError: true)
        new StagingColumnDefinition(
                userId: userId,
                fieldName: 'title',
                fieldDefinitionType: StagingColumnType.DataFileColumn,
                format: 'title'
        ).save(flush: true, failOnError: true)
        new StagingColumnDefinition(
                userId: userId,
                fieldName: 'description',
                fieldDefinitionType: StagingColumnType.DataFileColumn,
                format: 'description'
        ).save(flush: true, failOnError: true)

        File stagedImage = new File(service.getStagedFileLocalPath(stagedFile))
        stagedImage.parentFile.mkdirs()
        stagedImage.bytes = [1, 2, 3] as byte[]

        File dataFile = new File(stagedImage.parentFile, 'datafile/datafile.txt')
        dataFile.parentFile.mkdirs()
        dataFile.withOutputStream { output ->
            output.write([0xEF, 0xBB, 0xBF] as byte[])
            getClass().getResourceAsStream('/fixtures/staging-metadata.csv').withCloseable { fixture ->
                fixture.transferTo(output)
            }
        }

        when:
        def images = service.buildStagedImageData(userId, [:])

        then:
        images.size() == 1
        images[0].filename == 'fixture-one.jpg'
        images[0].title == 'Bird, red'
        images[0].description == ''
    }
}
