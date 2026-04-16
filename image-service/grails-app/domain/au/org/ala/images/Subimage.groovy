package au.org.ala.images

class Subimage {

    Image parentImage
    Image subimage
    int x
    int y
    int height
    int width

    static constraints = {
        parentImage nullable: false
        subimage nullable: false
    }
}
