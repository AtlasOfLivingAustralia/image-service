package au.org.ala.images;

class ImageStoreResult {

    Image image = null
    boolean alreadyStored = false
    boolean isDuplicate = false
    boolean metadataAlreadyPersisted = false
    Map<String, Object> extractedMetadata = null // Metadata extracted before optimisation

    ImageStoreResult(Image image, boolean alreadyStored, boolean isDuplicate){
        this.alreadyStored = alreadyStored
        this.image = image
        this.isDuplicate = isDuplicate;
    }

    ImageStoreResult(Image image, boolean alreadyStored, boolean isDuplicate, boolean metadataAlreadyPersisted){
        this.alreadyStored = alreadyStored
        this.image = image
        this.isDuplicate = isDuplicate;
        this.metadataAlreadyPersisted = metadataAlreadyPersisted
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ImageStoreResult{")
        sb.append("image.id=").append(image.id)
        sb.append(", image.imageIdentifier=").append(image.imageIdentifier)
        sb.append(", alreadyStored=").append(alreadyStored)
        sb.append(", isDuplicate=").append(isDuplicate)
        sb.append(", metadataAlreadyPersisted=").append(metadataAlreadyPersisted)
        sb.append('}')
        return sb.toString()
    }
}