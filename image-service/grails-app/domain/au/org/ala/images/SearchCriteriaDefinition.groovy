package au.org.ala.images

class SearchCriteriaDefinition implements Serializable {

    private static final long serialVersionUID = -68799122222905033L;

    CriteriaType type
    CriteriaValueType valueType

    String name
    String description
    String fieldName
    String units

    static constraints = {
        valueType nullable: false
        name nullable: false
        description nullable: true
        units nullable: true
    }

    static mapping = {
        description length: 1024
    }

}
