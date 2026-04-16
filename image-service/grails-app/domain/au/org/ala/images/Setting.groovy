package au.org.ala.images

class Setting {

    String name
    String description
    String value
    SettingType type

    static constraints = {
        name nullable: false
        value nullable: false
        description nullable: true
        type nullable: false
    }

}
