package au.org.ala.images

import net.kaleidos.hibernate.usertype.ArrayType

class Image {

    Image parent
    @SearchableProperty(description = "The unique identifier of an image")
    String imageIdentifier
    @SearchableProperty(description = "The MD5 hash of an image")
    String contentMD5Hash
    @SearchableProperty(description = "The SHA1 hash of an image")
    String contentSHA1Hash
    @SearchableProperty(description = "The content type of the image")
    String mimeType
    @SearchableProperty(description= "The original name of the image file when it was uploaded")
    String originalFilename
    @SearchableProperty(description = "The extension of the image file when uploaded")
    String extension
    @SearchableProperty(valueType = CriteriaValueType.DateRange, description = "The date the image was uploaded")
    Date dateUploaded
    String uploader
    @SearchableProperty(valueType = CriteriaValueType.DateRange, description = "The date the image was captured or authored")
    Date dateTaken
    @SearchableProperty(valueType = CriteriaValueType.NumberRangeLong, units = "bytes", description = "The size of the image file in bytes")
    Long fileSize = 0
    @SearchableProperty(valueType = CriteriaValueType.NumberRangeInteger, units = "pixels", description = "The height of the image in pixels")
    Integer height
    @SearchableProperty(valueType = CriteriaValueType.NumberRangeInteger, units = "pixels", description = "The width of the image in pixels")
    Integer width
    @SearchableProperty(valueType = CriteriaValueType.NumberRangeInteger, units = "", description = "The number of zoom levels available in the TMS tiles")
    Integer zoomLevels = 0
    @SearchableProperty(description="The UID for the data resource associated with this image.")
    String dataResourceUid
    @SearchableProperty(description="An entity primarily responsible for making the resource.")
    String creator
    @SearchableProperty(description="A title for this image")
    String title
    @SearchableProperty(description="A general description of the image")
    String description
    @SearchableProperty(description="Rights information includes a statement about various property rights associated with the resource, including intellectual property rights")
    String rights
    @SearchableProperty(description="A person or organization owning or managing rights over the resource.")
    String rightsHolder
    @SearchableProperty(description="A legal document giving official permission to do something with the resource.")
    String license
    @SearchableProperty(description="Associated occurrence ID.")
    @Deprecated // search biocache for this image's uuid instead
    String occurrenceId
    @SearchableProperty(description="Calibrated by user.")
    String calibratedByUser
    License recognisedLicense
    @SearchableProperty(valueType = CriteriaValueType.NumberRangeInteger, units = "pixels", description = "The height of the thumbnail in pixels")
    Integer thumbHeight = 0
    @SearchableProperty(valueType = CriteriaValueType.NumberRangeInteger, units = "pixels", description = "The width of the thumbnail in pixels")
    Integer thumbWidth = 0
    @SearchableProperty(valueType = CriteriaValueType.Boolean, description = "Should be harvested by the ALA")
    Boolean harvestable = false
    @SearchableProperty(description="publisher DC term")
    String publisher
    @SearchableProperty(description="created DC term")
    String created
    @SearchableProperty(description="contributor DC term")
    String contributor
    @SearchableProperty(description="type DC term")
    String type
    @SearchableProperty(description="references DC term")
    String references
    @SearchableProperty(description="source DC term")
    String source
    @SearchableProperty(description="audience DC term")
    String audience

    Date dateDeleted
    Double mmPerPixel
    Integer squareThumbSize
    @SearchableProperty(description="alternate filenames / URLs that this image has been found under")
    String[] alternateFilename = []

    static belongsTo = [ storageLocation: StorageLocation ]
    static hasMany = [keywords:ImageKeyword, metadata: ImageMetaDataItem, tags: ImageTag, outSourcedJobs: OutsourcedJob]

    static constraints = {
        parent nullable: true
        contentMD5Hash nullable: true
        contentSHA1Hash nullable: true
        mimeType nullable: true         // dc:format is mapped to mimeType
        originalFilename nullable: true
        extension nullable: true
        dateUploaded nullable: true
        uploader nullable: true
        dateTaken nullable: true
        fileSize nullable: true
        height nullable: true
        width nullable: true
        zoomLevels nullable: true
        recognisedLicense nullable:true
        dataResourceUid nullable: true

        //Dublin core fields
        creator nullable: true
        title nullable: true
        rightsHolder nullable: true     // formerly 'attribution'
        rights nullable: true           // formerly 'copyright'
        license nullable: true
        publisher nullable: true
        created nullable: true
        contributor nullable: true
        type nullable: true
        references nullable: true
        source nullable: true
        description nullable: true
        audience nullable: true

        thumbHeight nullable: true
        thumbWidth nullable: true
        squareThumbSize nullable: true
        mmPerPixel nullable: true
        calibratedByUser nullable:true
        harvestable nullable: true

        dateDeleted  nullable: true
        occurrenceId nullable: true
        alternateFilename nullable: true
    }

    static mapping = {
        imageIdentifier index: 'imageidentifier_idx'
        contentMD5Hash index: 'image_md5hash_idx'
        dateTaken index: 'image_datetaken_idx'
        dateUploaded index: 'image_dateuploaded'
        dataResourceUid index: 'image_dataResourceUid_Idx'
        originalFilename index: 'image_originalfilename_idx'
        alternateFilename type: ArrayType, params: [type: String], index: 'image_alternatefilename_idx'

        description length: 8096
        references column: "dc_references",  length: 1024
        publisher length: 1024
        contributor length: 1024
        source length: 1024
        audience length: 1024

        metadata cascade: 'all'
        cache true
        storageLocation cache: true
    }

    byte[] retrieve() {
        storageLocation.retrieve(this.imageIdentifier)
    }

    boolean stored() {
        storageLocation.stored(this.imageIdentifier)
    }

    long consumedSpace() {
        storageLocation.consumedSpace(this.imageIdentifier)
    }

    boolean deleteStored() {
        storageLocation.deleteStored(this.imageIdentifier)
    }

    InputStream originalInputStream() throws FileNotFoundException {
        storageLocation.originalInputStream(this.imageIdentifier, Range.emptyRange(this.fileSize))
    }

    InputStream originalInputStream(Range range) throws FileNotFoundException {
        storageLocation.originalInputStream(this.imageIdentifier, range)
    }

    InputStream thumbnailInputStream(Range range) throws FileNotFoundException {
        storageLocation.thumbnailInputStream(this.imageIdentifier, range)
    }

    InputStream thumbnailTypeInputStream(String type, Range range) throws FileNotFoundException {
        storageLocation.thumbnailTypeInputStream(this.imageIdentifier, type, range)
    }

    InputStream tileInputStream(Range range, int x, int y, int z) throws FileNotFoundException {
        storageLocation.tileInputStream(this.imageIdentifier, x, y, z, range)
    }

    long originalStoredLength() throws FileNotFoundException {
        storageLocation.originalStoredLength(this.imageIdentifier)
    }

    long thumbnailStoredLength() throws FileNotFoundException {
        storageLocation.thumbnailStoredLength(this.imageIdentifier)
    }

    long thumbnailTypeStoredLength(String type) throws FileNotFoundException {
        storageLocation.thumbnailTypeStoredLength(this.imageIdentifier, type)
    }

    long tileStoredLength(int x, int y, int z) throws FileNotFoundException {
        storageLocation.tileStoredLength(this.imageIdentifier, x, y, z)
    }

    void migrateTo(StorageLocation destination) {
        storageLocation.migrateTo(this.imageIdentifier, this.mimeType, destination)
    }

    static Image byOriginalFileOrAlternateFilename(String filename) {
        Image.withCriteria(uniqueResult: true) {
            or {
                eq 'originalFilename', filename
                pgArrayContains 'alternateFilename', filename
            }
        }
    }
}