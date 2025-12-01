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
    boolean shouldExist // whether the requested image should actually exist (ie the image record exists but the bytes don't)

    static ImageInfo notFound() {
        return new ImageInfo(
                exists: false,
                shouldExist: false,
                imageIdentifier: null,
                dataResourceUid: null,
                length: 0,
                etag: null,
                lastModified: null,
                contentType: null,
                extension: null,
                redirectUri: null,
                inputStreamSupplier: null
        )
    }
}
