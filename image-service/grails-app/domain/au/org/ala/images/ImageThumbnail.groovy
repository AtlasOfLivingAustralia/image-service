package au.org.ala.images

import grails.gorm.async.AsyncEntity

class ImageThumbnail implements AsyncEntity<ImageThumbnail> {

    Image image
    int width
    int height
    boolean isSquare
    String name

    static constraints = {
        image nullable: false
        width nullable: false
        height nullable: false
        isSquare nullable: false
        name nullable: false
    }

    static mapping = {
        image index: "image_thumbnail_image_idx"
        name index: "image_thumbnail_image_idx,image_thumbnail_name_idx"
    }

}
