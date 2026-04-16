package au.org.ala.images

import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import com.google.common.io.Resources
import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import org.grails.plugins.testing.GrailsMockMultipartFile
import spock.lang.Specification

class ImageStoreServiceSpec extends Specification implements ServiceUnitTest<ImageStoreService>, DataTest {

    IImageThumbnailer imageThumbnailer = Mock(IImageThumbnailer)
    IImageTiler imageTiler = Mock(IImageTiler)
    IOnDemandImageTiler onDemandImageTiler = Mock(IOnDemandImageTiler)

    def setup() {
        defineBeans {
            imageThumbnailer(imageThumbnailer)
            imageTiler(imageTiler)
            onDemandImageTiler(onDemandImageTiler)
            imageTilerConfig(Mock(ImageTilerConfig))
            commandExecutor(Mock(CommandExecutor))
        }
        service.auditService = Mock(AuditService)
        service.storageLocationService = Mock(StorageLocationService)
        service.imageThumbnailer = imageThumbnailer
        service.imageTiler = imageTiler
        service.onDemandImageTiler = onDemandImageTiler
    }

    def "test store tiles zip"() {
        setup:
        def image = Mock(Image)
        def storageLocation = Mock(StorageLocation)
        service.storageLocationService.getStorageOperationsForImage(image) >> storageLocation
        def uuid = UUID.randomUUID().toString()
        image.getImageIdentifier() >> uuid
        image.getStorageLocation() >> storageLocation
        def mpf = new GrailsMockMultipartFile('upload.zip', 'upload.zip', 'application/zip', Resources.getResource('test.zip').newInputStream())

        when:
        service.storeTilesArchiveForImage(image, mpf)

        then:

        1 * storageLocation.stored(uuid) >> true
        // no directories entries call storeTileZipInputStream
        0 * storageLocation.storeTileZipInputStream(uuid, '0/0/', _, 0, _)
        // file entries do call storeTileZipInputStream
        1 * storageLocation.storeTileZipInputStream(uuid, '0/0/0.png', 'image/jpeg', 1994, _)
        1 *  service.auditService.log(uuid, 'Image tiles stored from zip file (outsourced job?)', 'N/A')
    }

}
