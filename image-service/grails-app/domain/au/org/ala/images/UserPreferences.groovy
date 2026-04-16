package au.org.ala.images

class UserPreferences {

    String userId
    String exportColumns

    static constraints = {
        userId nullable: false
        exportColumns nullable: true
    }

    static mapping = {
        userId index: 'userprefs_userId_idx'
        exportColumns  length: 4096
    }

}
