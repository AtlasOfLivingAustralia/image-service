package au.org.ala.images

class OutsourcedJob {

    String ticket
    Date dateCreated
    ImageTaskType taskType
    int expectedDurationInMinutes

    static belongsTo = [image: Image]

    static constraints = {
        ticket nullable: false
        image nullable: false
        taskType nullable: false
        expectedDurationInMinutes nullable: false
        dateCreated nullable: true
    }

}
