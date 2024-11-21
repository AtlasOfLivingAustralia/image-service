package au.org.ala.images

class ImageInfo {

    boolean exists
    String imageIdentifier
    String dataResourceUid
    long length
    String etag
    Date lastModified
    String contentType
    String extension
    URI redirectUri
    // takes an optional single Range parameter
    Closure<InputStream> inputStreamSupplier

}
