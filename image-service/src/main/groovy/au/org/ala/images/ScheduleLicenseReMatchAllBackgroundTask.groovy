package au.org.ala.images

class ScheduleLicenseReMatchAllBackgroundTask extends BackgroundTask {

    private ImageService _imageService

    ScheduleLicenseReMatchAllBackgroundTask(ImageService imageService) {
        _imageService = imageService
    }

    @Override
    void execute() {
        _imageService.updateLicences()
    }

    @Override
    boolean isRequiresSession() {
        return true
    }
}
