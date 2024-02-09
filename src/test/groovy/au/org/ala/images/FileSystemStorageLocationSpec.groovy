package au.org.ala.images

import grails.testing.gorm.DomainUnitTest
import groovy.util.logging.Slf4j
import spock.lang.Shared
import spock.lang.TempDir

@Slf4j
class FileSystemStorageLocationSpec extends StorageLocationSpec implements DomainUnitTest<FileSystemStorageLocation> {

    @TempDir @Shared File tempFolder
    @Shared File tempDir
    @Shared File tempDir2
    @Shared File altTempDir

    List<FileSystemStorageLocation> getStorageLocations() { [
                new FileSystemStorageLocation(basePath: tempDir.toString()).save()
                ,new FileSystemStorageLocation(basePath: tempDir2.toString()+"/").save()
    ]}
    FileSystemStorageLocation alternateStorageLocation

    def setupSpec() {
        tempDir = new File(tempFolder, 'storage').tap { mkdir() }
        tempDir2 = new File(tempFolder, 'storage2').tap { mkdir() }
        altTempDir = new File(tempFolder, 'altStorage').tap { mkdir() }
    }

    def setup() {
        alternateStorageLocation = new FileSystemStorageLocation(basePath: altTempDir.toString()).save()
    }

}
