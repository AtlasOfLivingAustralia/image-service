package au.org.ala.images

class License {

    String acronym // e.g. 'CC BY'
    String name // 'Creative commons by Attribution'
    String url
    String imageUrl // 'URL to image to display'

    static constraints = {
        imageUrl nullable: true
    }
}
