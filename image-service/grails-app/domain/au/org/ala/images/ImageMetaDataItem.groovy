package au.org.ala.images

class ImageMetaDataItem {

    Image image
    String name
    String value
    MetaDataSourceType source

    static belongsTo = [image: Image]

    static constraints = {
        image nullable: false
        name nullable: false
        value nullable: false
        source nullable: true
    }

    static mapping = {
        id generator:'sequence', params:[sequence_name:'image_metadata_seq']
        name length: 1024
        value length: 8096
    }

}
