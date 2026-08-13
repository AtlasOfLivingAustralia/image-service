package au.org.ala.images

class FailedUpload {

    String url
    Date dateCreated
    String errorMessage
    Integer httpStatusCode

    static constraints = {
        url nullable: false
        errorMessage nullable: true, maxSize: 2000
        httpStatusCode nullable: true
    }

    static mapping = {
        version false
        id name: 'url', generator: 'assigned'
    }
}
