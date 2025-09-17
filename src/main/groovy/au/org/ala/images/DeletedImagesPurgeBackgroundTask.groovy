package au.org.ala.images

import groovy.util.logging.Slf4j

@Slf4j
class DeletedImagesPurgeBackgroundTask extends BackgroundTask {

    ImageService imageService
    boolean requiresSession = true

    DeletedImagesPurgeBackgroundTask(ImageService imageService) {
        this.imageService = imageService
    }

    @Override
    void execute() {
        imageService.purgeAllDeletedImages()
    }

    @Override
    boolean isRequiresSession() {
        return true
    }
}
