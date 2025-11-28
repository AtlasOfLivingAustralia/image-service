package au.org.ala.images

import grails.gorm.transactions.Transactional
import org.javaswift.joss.client.factory.AuthenticationMethod
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value

import java.util.concurrent.Executor
import java.util.concurrent.Executors

@Transactional
class StorageLocationService {

    def imageService

    @Value('${images.storage.updateAcl.enabled:true}')
    Boolean updateAclEnabled = true

    @Value('${images.storage.updateAcl.objectThreshold:50000}')
    int updateAclObjectThreshold = 50000

    @Autowired
    Executor analyticsExecutor

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
                        accessKey: json.accessKey, secretKey: json.secretKey,
                        publicRead: [true, 'true', 'on'].contains(json.publicRead),
                        privateAcl: [true, 'true', 'on'].contains(json.privateAcl),
                        redirect: [true, 'true', 'on'].contains(json.redirect))
                break
            case 'swift':
                if (SwiftStorageLocation.countByAuthUrlAndContainerName(json.authUrl, json.containerName) > 0) {
                    throw new AlreadyExistsException("Swift Storage $json.authUrl $json.containerName already exists")
                }
                storageLocation = new SwiftStorageLocation(authUrl: json.authUrl, containerName: json.containerName,
                            username: json.username, password: json.password,
                            tenantId: json.tenantId ?: '', tenantName: json.tenantName ?: '',
                            authenticationMethod: AuthenticationMethod.valueOf(json.authenticationMethod),
                            publicContainer: [true, 'true', 'on'].contains(json.publicContainer),
                            redirect: [true, 'true', 'on'].contains(json.redirect))
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

    @Transactional(readOnly = true)
    def migrate(long sourceId, long destId, String userId, boolean deleteSource) {
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
            imageService.scheduleBackgroundTask(new MigrateStorageLocationTask(imageId: id, destinationStorageLocationId: destId, userId: userId, imageService: imageService, deleteSource: deleteSource))
        }
    }

    void updateStorageLocation(StorageLocation storageLocation) {
        storageLocation.save(failOnError: true, validate: true)
    }

    void updateAcl(StorageLocation storageLocation) {
        if (storageLocation instanceof S3StorageLocation) {
            if (updateAclEnabled) {
                // Default behavior: enable only if the total number of images is under the threshold
                long totalObjects = Image.countByStorageLocation(storageLocation)
                if (totalObjects < updateAclObjectThreshold) {
                    // Proceed with background ACL and cache-control update
                    analyticsExecutor.execute {
                        storageLocation.updateACL()
                    }
                } else {
                    log.info("updateAcl skipped for storageLocation id={}, class={}, total objects={} exceeds threshold={}", storageLocation.id, storageLocation.class.name, totalObjects, updateAclObjectThreshold)
                }
            } else {
                log.info("Skipping ACL update for storageLocation id={}, class={}, feature disabled by config", storageLocation.id, storageLocation.class.name)
            }
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
