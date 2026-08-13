package au.org.ala.images

class ImportFileBackgroundTask extends BackgroundTask {

    File imageFile
    ImageService imageService
    String batchId
    String userId

    ImportFileBackgroundTask(File imageFile, ImageService imageService, String batchId, String userId) {
        this.imageFile = imageFile
        this.imageService = imageService
        this.batchId = batchId
        this.userId = userId
    }

    @Override
    void execute() {
        imageService.importFileFromInbox(imageFile, batchId, userId);
    }
}
