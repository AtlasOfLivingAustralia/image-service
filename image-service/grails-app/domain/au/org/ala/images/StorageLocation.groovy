package au.org.ala.images

import au.org.ala.images.storage.StorageOperations

abstract class StorageLocation implements StorageOperations {

    Long id

    Date dateCreated
    Date lastUpdated

    static hasMany = [images: Image]

    static transients = ['supportsRedirect']

    static constraints = {
    }

    static mapping = {
        id generator: 'identity'
        cache true
        images lazy: true
    }

    abstract StorageOperations asStandaloneStorageOperations()

}
