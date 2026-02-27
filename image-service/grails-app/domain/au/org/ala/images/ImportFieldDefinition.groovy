package au.org.ala.images

class ImportFieldDefinition {

    String fieldName
    String value
    ImportFieldType fieldType

    static constraints = {
        fieldName nullable: false
        value nullable: false
        fieldType nullable: false
    }

    static mapping = {
        value length: 1024
    }

}
