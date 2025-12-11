package au.org.ala.images

import au.org.ala.images.storage.FileSystemStorageOperations
import au.org.ala.images.storage.StorageOperations
import groovy.transform.EqualsAndHashCode

@EqualsAndHashCode(includes = ['basePath'])
class FileSystemStorageLocation extends StorageLocation {

    String basePath

    static transients = ['storageOperations', '$storageOperations', 'probeFilesForImageInfo']

    static constraints = {
    }

    static mapping = {
        cache true
    }

    @Delegate @Lazy FileSystemStorageOperations storageOperations = { new FileSystemStorageOperations(basePath: basePath) }()

    StorageOperations asStandaloneStorageOperations() {
        storageOperations
    }

    @Override
    String toString() {
        "Filesystem($id): $basePath"
    }
}
