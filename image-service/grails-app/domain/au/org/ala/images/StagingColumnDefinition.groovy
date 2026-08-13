package au.org.ala.images

class StagingColumnDefinition {

    String userId
    String fieldName
    StagingColumnType fieldDefinitionType
    String format

    static constraints = {
        userId nullable: false, blank: false
        fieldName nullable: false, blank: false
        fieldDefinitionType nullable: true
        format nullable: true
    }

}
