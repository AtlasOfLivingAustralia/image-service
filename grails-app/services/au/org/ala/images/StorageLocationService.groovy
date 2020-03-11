package au.org.ala.images

import grails.gorm.transactions.Transactional
import org.javaswift.joss.client.factory.AuthenticationMethod

@Transactional
class StorageLocationService {

    def imageService

    StorageLocation createStorageLocation(json) {
        StorageLocation storageLocation
        switch (json.type?.toLowerCase()) {
            case 'fs':
                if (FileSystemStorageLocation.countByBasePath(json.basePath) > 0) {
                    throw new AlreadyExistsException("FS $json.basePath already exists")
                }
                storageLocation = new FileSystemStorageLocation(basePath: json.basePath)
                break
            case 's3':
                if (S3StorageLocation.countByRegionAndBucketAndPrefix(json.region, json.bucket, json.prefix ?: '') > 0) {
                    throw new AlreadyExistsException("S3 $json.region $json.bucket $json.prefix already exists")
                }
                storageLocation = new S3StorageLocation(region: json.region, bucket: json.bucket, prefix: json.prefix ?: '',
                        accessKey: json.accessKey, secretKey: json.secretKey, publicRead: [true, 'true', 'on'].contains(json.publicRead))
                break
            case 'swift':
                if (SwiftStorageLocation.countByAuthUrlAndContainerName(json.authUrl, json.containerName) > 0) {
                    throw new AlreadyExistsException("Swift Storage $json.authUrl $json.containerName already exists")
                }
                storageLocation = new SwiftStorageLocation(authUrl: json.authUrl, containerName: json.containerName,
                            username: json.username, password: json.password,
                            tenantId: json.tenantId ?: '', tenantName: json.tenantName ?: '',
                            authenticationMethod: AuthenticationMethod.valueOf(json.authenticationMethod),
                            publicContainer: [true, 'true', 'on'].contains(json.publicContainer))
                break
            default:
                throw new RuntimeException("Unknown storage location type ${json.type}")
        }

        if (storageLocation.hasErrors()) {
            throw new RuntimeException("Validation error")
        }

        def sl = storageLocation.save(failOnError: true, validate: true)

        return sl

    }

    def migrate(long sourceId, long destId, String userId) {
        log.info("migrating images from storage location {} to {}", sourceId, destId)
        StorageLocation source = StorageLocation.findById(sourceId)
        StorageLocation dest = StorageLocation.findById(destId)

        if (!source || !dest) {
            throw new RuntimeException("Storage Location doesn't exist")
        }
        log.debug("Searching for images")

        def c = Image.createCriteria()
        def results = c.scroll {
            eq('storageLocation', source)
            projections {
                property('id')
            }
        }

        while (results.next()) {
            def id = results.get(0)
            log.debug("Adding MigrationStorageLocationTask for image id {} to dest storage location {}", id, dest)
            imageService.scheduleBackgroundTask(new MigrateStorageLocationTask(imageId: id, destinationStorageLocationId: destId, userId: userId, imageService: imageService))
        }
    }

    static class AlreadyExistsException extends RuntimeException {
        AlreadyExistsException(String message) {
            super(message)
        }

        @Override
        synchronized Throwable fillInStackTrace() {
            return this
        }
    }
}
