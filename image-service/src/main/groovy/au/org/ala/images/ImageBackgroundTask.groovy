package au.org.ala.images

class ImageBackgroundTask extends BackgroundTask {

    long imageId
    ImageTaskType operation
    ImageService imageService
    String userId
    boolean requiresSession = true

    ImageBackgroundTask(long imageId, ImageService imageService, ImageTaskType operation, String userId) {
        this.imageId = imageId
        this.imageService = imageService
        this.operation = operation
        this.userId = userId ?: "<unknown>"
    }

    @Override
    void execute() {
        Image.withNewTransaction {
            Image.lock(imageId)
            def imageInstance = Image.get(imageId)
            switch (operation) {
                case ImageTaskType.Thumbnail:
                    imageService.generateImageThumbnails(imageInstance)
                    break
                case ImageTaskType.TMSTile:
                    if (imageService.isImageType(imageInstance)) {
                        imageService.generateTMSTiles(imageInstance.imageIdentifier)
                    }
                    break
                case ImageTaskType.KeywordRebuild:
                    imageService.tagService.rebuildKeywords(imageInstance)
                    break
                case ImageTaskType.Delete:
                    imageService.deleteImage(imageInstance, userId)
                    break
                default:
                    throw new Exception("Unhandled image operation type: ${operation}")
            }
            if (operation != ImageTaskType.Delete) {
                imageInstance.save(flush: true, failOnError: true)
            }
        }
    }
}
