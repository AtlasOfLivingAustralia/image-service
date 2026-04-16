package au.org.ala.images

class StagedFile {

    String userId
    String filename
    Date dateStaged

    static constraints = {
        userId nullable: false
        filename nullable: false, unique: true
        dateStaged nullable: false
    }

}
