package au.org.ala.images

class UploadFromUrlTask extends BackgroundTask {

    private Map<String, String> _imageSource
    private ImageService _imageService
    private String _userId

    UploadFromUrlTask(Map<String, String> imageSource, ImageService imageService, String userId) {
        _imageSource = imageSource
        _imageService = imageService
        _userId = userId
    }

    @Override
    void execute() {
        def results = _imageService.batchUpdate([_imageSource], _userId)
        def newImage = results[_imageSource.sourceUrl ?: _imageSource.imageUrl]
        if (newImage && newImage.success) {
            Image.withNewTransaction {
                _imageService.setMetadataItemsByImageId(newImage.image.id, _imageSource, MetaDataSourceType.SystemDefined, _userId)
            }
//            _imageService.scheduleArtifactGeneration(newImage.image.id, _userId)
            _imageService.scheduleImageIndex(newImage.image.id)
            _imageService.scheduleImageMetadataPersist(newImage.image.id, newImage.image.imageIdentifier,  newImage.image.originalFileName, MetaDataSourceType.Embedded, _userId)
            this.yieldResult(newImage.image)
        }
    }
}
