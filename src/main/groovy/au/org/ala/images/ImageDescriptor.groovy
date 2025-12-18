package au.org.ala.images

class ImageDescriptor {
    String imageIdentifier
    int height
    int width
    int zoomLevels
    Map<String, Object> extractedMetadata // Metadata extracted before optimisation
}