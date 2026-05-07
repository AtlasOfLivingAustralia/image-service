package au.org.ala.images

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.util.logging.Slf4j

@Slf4j
@EqualsAndHashCode(excludes = ['imageService'])
@ToString(excludes = ['imageService'])
class MigrateStorageLocationTask extends BackgroundTask {

    long imageId
    long destinationStorageLocationId
    boolean deleteSource
    String userId

    ImageService imageService

    @Override
    void execute() {
        imageService.migrateImage(imageId, destinationStorageLocationId, userId, deleteSource)
    }
}
