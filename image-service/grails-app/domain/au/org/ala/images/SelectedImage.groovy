package au.org.ala.images

class SelectedImage {

    String userId
    Image image

    static constraints = {
        userId nullable: false
        image nullable: false
    }
}
