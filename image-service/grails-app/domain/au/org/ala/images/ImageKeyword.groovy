package au.org.ala.images

class ImageKeyword {

    Image image
    String keyword

    static constraints = {
        image nullable: false
        keyword nullable: false
    }
}
