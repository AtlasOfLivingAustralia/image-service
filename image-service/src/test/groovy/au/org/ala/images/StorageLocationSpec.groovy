package au.org.ala.images

import com.google.common.io.Resources
import grails.testing.gorm.DomainUnitTest
import net.lingala.zip4j.ZipFile
import org.apache.commons.io.FileUtils
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

abstract class StorageLocationSpec extends Specification implements DomainUnitTest<StorageLocation> {

    @TempDir
    File zipFolder

    URL resource
    URLConnection connection
    long resourceLength
    String uuid
    String fakeUuid

    def setup() {
        resource = Resources.getResource('test.jpg')
        connection = resource.openConnection()
        resourceLength = connection.contentLengthLong
        uuid = UUID.randomUUID().toString()
        fakeUuid = UUID.randomUUID().toString()
    }

    abstract List<StorageLocation> getStorageLocations()

    abstract StorageLocation getAlternateStorageLocation()


    @Unroll
    def "test store and retrieve #storageLocation"(StorageLocation storageLocation) {
        when:
        storageLocation.store(uuid, connection.inputStream, 'image/jpeg', null, resourceLength)

        then:
        def bytes = storageLocation.retrieve(uuid)

        bytes == resource.bytes

        storageLocation.originalStoredLength(uuid) == resource.bytes.length

        when:
        storageLocation.originalStoredLength(fakeUuid)

        then:
        thrown FileNotFoundException

        when:
        storageLocation.thumbnailInputStream(fakeUuid, Range.emptyRange(resourceLength)).bytes

        then:
        thrown FileNotFoundException

        where:
        storageLocation << getStorageLocations()
    }

    @Unroll
    def "test thumbnails #storageLocation"(StorageLocation storageLocation) {
        setup:
        def bsf = storageLocation.thumbnailByteSinkFactory(uuid)

        when:
        bsf.getByteSinkForNames('thumbnail').write(resource.bytes)

        then:
        storageLocation.thumbnailStoredLength(uuid) == resourceLength
        storageLocation.thumbnailInputStream(uuid, Range.emptyRange(resourceLength)).bytes == resource.bytes

        when:
        bsf.getByteSinkForNames('thumbnail_large').write(resource.bytes)

        then:
        storageLocation.thumbnailTypeStoredLength(uuid, 'large') == resourceLength
        storageLocation.thumbnailTypeInputStream(uuid, 'large', Range.emptyRange(resourceLength)).bytes == resource.bytes

        when:
        storageLocation.thumbnailStoredLength(fakeUuid)

        then:
        thrown FileNotFoundException

        when:
        storageLocation.thumbnailInputStream(fakeUuid, Range.emptyRange(resourceLength)).bytes

        then:
        thrown FileNotFoundException

        when:
        storageLocation.thumbnailTypeStoredLength(uuid, 'fake')

        then:
        thrown FileNotFoundException

        when:
        storageLocation.thumbnailTypeInputStream(uuid, 'fake', Range.emptyRange(resourceLength)).bytes

        then:
        thrown FileNotFoundException

        where:
        storageLocation << getStorageLocations()
    }

    @Unroll
    def "test tiling #storageLocation"(StorageLocation storageLocation) {
        setup:
        def bsf = storageLocation.tilerByteSinkFactory(uuid)
        def range = Range.emptyRange(resourceLength)

        when:
        bsf.getByteSinkForNames('1','1','1.png').write(resource.bytes)

        then:
        storageLocation.tileInputStream(uuid,1,1,1, range).bytes == resource.bytes

        when:
        bsf.getByteSinkForNames('-3','-2','0.png').write(resource.bytes)

        then:
        storageLocation.tileInputStream(uuid,-2,0,-3, range).bytes == resource.bytes

        when:
        storageLocation.tileStoredLength(uuid, 7,7,7)

        then:
        thrown FileNotFoundException

        when:
        storageLocation.tileInputStream(uuid, 7,7,7, range).bytes

        then:
        thrown FileNotFoundException

        where:
        storageLocation << getStorageLocations()
    }

    @Unroll
    def "test delete #storageLocation"(StorageLocation storageLocation) {
        when:
        storageLocation.store(uuid, connection.inputStream, 'image/jpeg', null, resourceLength)
        storageLocation.tilerByteSinkFactory(uuid).getByteSinkForNames('1','1','1.png').write(resource.bytes)
        storageLocation.thumbnailByteSinkFactory(uuid).getByteSinkForNames('thumbnail').write(resource.bytes)
        storageLocation.thumbnailByteSinkFactory(uuid).getByteSinkForNames('thumbnail_large').write(resource.bytes)

        then:
        storageLocation.consumedSpace(uuid) == resourceLength * 4

        when:
        storageLocation.deleteStored(uuid)

        then:
        storageLocation.consumedSpace('uuid') == 0

        where:
        storageLocation << getStorageLocations()
    }

    @Unroll
    def "test range #storageLocation"(StorageLocation storageLocation) {
        setup:
        def range = new Range(start: 10, end: 20, suffixLength: null, totalLength: resourceLength)
        def endRange = new Range(start: null, end: null, suffixLength: 10, totalLength: resourceLength)

        when:
        storageLocation.store(uuid, connection.inputStream, 'image/jpeg', null, resourceLength)

        then:
        storageLocation.originalInputStream(uuid, range).bytes.toList() == resource.bytes[10..20]
        storageLocation.originalInputStream(uuid, endRange).bytes.toList() == resource.bytes[(resourceLength - 10)..(resourceLength-1)]

        when:
        storageLocation.tilerByteSinkFactory(uuid).getByteSinkForNames('1','1','1.png').write(resource.bytes)

        then:
        storageLocation.tileInputStream(uuid, 1, 1, 1, range).bytes.toList() == resource.bytes[10..20]
        storageLocation.tileInputStream(uuid, 1, 1, 1, endRange).bytes.toList() == resource.bytes[(resourceLength - 10)..(resourceLength-1)]

        when:
        storageLocation.thumbnailByteSinkFactory(uuid).getByteSinkForNames('thumbnail').write(resource.bytes)

        then:
        storageLocation.thumbnailInputStream(uuid, range).bytes.toList() == resource.bytes[10..20]
        storageLocation.thumbnailInputStream(uuid, endRange).bytes.toList() == resource.bytes[(resourceLength - 10)..(resourceLength-1)]

        when:
        storageLocation.thumbnailByteSinkFactory(uuid).getByteSinkForNames('thumbnail_large').write(resource.bytes)

        then:
        storageLocation.thumbnailTypeInputStream(uuid, 'large', range).bytes.toList() == resource.bytes[10..20]
        storageLocation.thumbnailTypeInputStream(uuid, 'large', endRange).bytes.toList() == resource.bytes[(resourceLength - 10)..(resourceLength-1)]

        where:
        storageLocation << getStorageLocations()
    }

    @Unroll
    def "test migrate #storageLocation"(StorageLocation storageLocation) {
        setup:
        storageLocation.store(uuid, connection.inputStream, 'image/jpeg', null, resourceLength)
        storageLocation.tilerByteSinkFactory(uuid).getByteSinkForNames('1','1','1.png').write(resource.bytes)
        storageLocation.thumbnailByteSinkFactory(uuid).getByteSinkForNames('thumbnail').write(resource.bytes)
        storageLocation.thumbnailByteSinkFactory(uuid).getByteSinkForNames('thumbnail_large').write(resource.bytes)
        def size = storageLocation.consumedSpace(uuid)

        when:
        storageLocation.migrateTo(uuid, 'image/jpeg', alternateStorageLocation)

        then:
        alternateStorageLocation.retrieve(uuid) == resource.bytes
        alternateStorageLocation.thumbnailInputStream(uuid, Range.emptyRange(resourceLength)).bytes == resource.bytes
        alternateStorageLocation.thumbnailTypeInputStream(uuid, 'large', Range.emptyRange(resourceLength)).bytes == resource.bytes
        alternateStorageLocation.tileInputStream(uuid, 1, 1, 1, Range.emptyRange(resourceLength)).bytes == resource.bytes
        alternateStorageLocation.consumedSpace(uuid) == size

        where:
        storageLocation << getStorageLocations()
    }

    @Unroll
    def "test unzip #storageLocation"(StorageLocation storageLocation) {
        setup:
        def zipUrl = Resources.getResource('test.zip')
        def zipFile = new File(zipFolder, 'test.zip')
        FileUtils.copyURLToFile(zipUrl, zipFile)
        def zip = new ZipFile(zipFile)
        def tileHeader = zip.getFileHeader('0/0/0.png')
        def tile2Header = zip.getFileHeader('7/8/14.png')

        when:
        storageLocation.storeTileZipInputStream(uuid, tileHeader.fileName, 'image/png', tileHeader.uncompressedSize, zip.getInputStream(tileHeader))

        then:
        storageLocation.tileInputStream(uuid, 0,0,0, Range.emptyRange(tileHeader.uncompressedSize)).bytes == zip.getInputStream(tileHeader).bytes

        when:
        storageLocation.storeTileZipInputStream(uuid, tile2Header.fileName, 'image/png', tile2Header.uncompressedSize, zip.getInputStream(tile2Header))

        then:
        storageLocation.tileInputStream(uuid, 8,14,7, Range.emptyRange(tile2Header.uncompressedSize)).bytes == zip.getInputStream(tile2Header).bytes

        where:
        storageLocation << getStorageLocations()
    }
}
