package au.org.ala.images;

class ImageStoreResult {

    Image image = null
    boolean alreadyStored = false
    boolean isDuplicate = false

    ImageStoreResult(Image image, boolean alreadyStored, boolean isDuplicate){
        this.alreadyStored = alreadyStored
        this.image = image
        this.isDuplicate = isDuplicate;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ImageStoreResult{")
        sb.append("image.id=").append(image.id)
        sb.append(", image.imageIdentifier=").append(image.imageIdentifier)
        sb.append(", alreadyStored=").append(alreadyStored)
        sb.append(", isDuplicate=").append(isDuplicate)
        sb.append('}')
        return sb.toString()
    }
}