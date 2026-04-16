package au.org.ala.images

class ImageTag {

    static belongsTo = [ image: Image, tag: Tag]

    static constraints = {
        tag nullable: false
        image nullable: false
    }

}
