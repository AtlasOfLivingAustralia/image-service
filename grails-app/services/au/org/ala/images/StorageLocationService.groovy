package au.org.ala.images

import au.org.ala.images.storage.StorageOperations
import grails.gorm.transactions.NotTransactional
import grails.gorm.transactions.Transactional
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import org.grails.orm.hibernate.cfg.GrailsHibernateUtil
import org.javaswift.joss.client.factory.AuthenticationMethod
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value

import java.util.concurrent.Executor

@Slf4j
@Transactional
class StorageLocationService {

    def imageService
    def settingService

    @Autowired
    StorageOperationsRegistry storageOperationsRegistry

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
                        containerCredentials: [true, 'true', 'on'].contains(json.containerCredentials),
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

    /**
     * Get storage operations for a specific image identifier.
     * This will use config-based storage if available, with multiple optimization tiers:
     *
     * Tier 1: Single config storage - 0 DB queries (fastest)
     * Tier 2: Multi config storage with string name - 1 simple indexed query (fast, no join)
     * Tier 3: Database FK lookup - 1 join query (backward compatibility)
     */
    @NotTransactional // avoid wrapping in a transaction in case we're in no db lookup mode
    StorageOperations getStorageOperationsForImage(String imageIdentifierArg) {
        // TIER 1: Try single-storage config first (zero DB hit!)
        StorageOperations ops = getStorageOperationsWithoutDbLookup()
        if (ops) {
            log.trace("Using single-storage config for image {}", imageIdentifierArg)
            return ops
        }

        return StorageLocation.withTransaction(readOnly: true) {
            // TIER 2: If we have config-based storage, lookup by string name
            // This requires minimal DB query (just the string field, no join!)
            if (storageOperationsRegistry.isUsingConfigBasedStorage()) {
                log.trace("Looking up storage location name for image {}", imageIdentifierArg)
                def storageLocationName = Image.where {
                    imageIdentifier == imageIdentifierArg
                }.projections {
                    property('storageLocationName')
                }.get()

                if (storageLocationName) {
                    ops = storageOperationsRegistry.getByName(storageLocationName as String)
                    if (ops) {
                        log.trace("Found storage operations for image {} using name: {}", imageIdentifierArg, storageLocationName)
                        return ops
                    } else {
                        log.warn("Image {} has storageLocationName '{}' but no matching config found", imageIdentifierArg, storageLocationName)
                    }
                }
            }

            // TIER 3: Fallback to FK join (for legacy/migration or when no config)
            log.trace("Falling back to FK lookup for image {}", imageIdentifierArg)
            def image = Image.findByImageIdentifier(imageIdentifierArg, [cache: true, fetch: [storageLocation: 'join']])
            if (image?.storageLocation) {
                return GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).asStandaloneStorageOperations()
            }

            log.warn("Image {} can't resolve a storage location, falling back to default!", imageIdentifierArg)
            return getDefaultStorageOperations()
        }
    }

    /**
     * Determines if database lookup can be skipped for storage operations.
     * This is true when using config-based storage with a single location.
     */
    boolean canSkipDatabaseLookup() {
        return storageOperationsRegistry.isUsingConfigBasedStorage() &&
               storageOperationsRegistry.hasSingleStorageLocation()
    }

    /**
     * Get storage operations without database lookup if possible.
     * Returns null if database lookup is required (multi-storage scenario).
     */
    StorageOperations getStorageOperationsWithoutDbLookup() {
        if (canSkipDatabaseLookup()) {
            return storageOperationsRegistry.getDefault()
        }
        return null
    }

    /**
     * Get storage operations for a specific image entity.
     * Even though we already have the Image entity, we will still try to avoid DB hits if possible.
     *
     * @param image
     * @return
     */
    StorageOperations getStorageOperationsForImage(Image image) {
        String imageIdentifierArg = image?.imageIdentifier

        StorageOperations ops = getStorageOperationsWithoutDbLookup()
        if (ops) {
            log.trace("Using single-storage config for image {}", imageIdentifierArg)
            return ops
        }

        if (storageOperationsRegistry.isUsingConfigBasedStorage()) {

            def storageLocationName = image.storageLocationName

            if (storageLocationName) {
                ops = storageOperationsRegistry.getByName(storageLocationName as String)
                if (ops) {
                    log.trace("Found storage operations for image {} using name: {}", imageIdentifierArg, storageLocationName)
                    return ops
                } else {
                    log.warn("Image {} has storageLocationName '{}' but no matching config found", imageIdentifierArg, storageLocationName)
                }
            }
        }

        log.trace("Falling back to DB lookup for image {}", imageIdentifierArg)
        if (image?.storageLocation) {
            return GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).asStandaloneStorageOperations()
        }

        log.warn("Image {} can't resolve a storage location, falling back to default!", imageIdentifierArg)
        return getDefaultStorageOperations()
    }

    @NotTransactional
    StorageOperations getDefaultStorageOperations() {
        StorageOperations ops
        if (storageOperationsRegistry.usingConfigBasedStorage) {
            ops = storageOperationsRegistry.getDefault()
            if (ops) {
                return ops
            }
        }
        return Image.withTransaction { // this should be readonly but the settingService may create the setting if it does not exist
            def defaultStorageLocationID = settingService.getStorageLocationDefault()
            return StorageLocation.get(defaultStorageLocationID)?.asStandaloneStorageOperations()
        }
    }

    /**
     * For backwards compatibility, get all possible default storage operations identifiers.
     * This includes both config-based default and database default.
     * The order is config-based first, then database-based.
     * @return The list of default storage operations identifiers.
     */
    @NotTransactional
    List<DefaultStorageOperationsId> getDefaultStorageOperationsId() {
        def result = []
        StorageOperations ops
        if (storageOperationsRegistry.usingConfigBasedStorage) {
            ops = storageOperationsRegistry.getDefault()
            if (ops) {
                result += new ConfigDefaultStorageOperationsId(name: storageOperationsRegistry.defaultName, operations: ops)
            }
        }
        Image.withTransaction { // this should be readonly but the settingService may create the setting if it does not exist
            def defaultStorageLocationID = settingService.getStorageLocationDefault()
            if (defaultStorageLocationID && defaultStorageLocationID > 0) { // if this is set to 0 or a negative value then there is no default in the DB
                def operations = StorageLocation.get(defaultStorageLocationID)?.asStandaloneStorageOperations()
                result += new DatabaseDefaultStorageOperationsId(id: defaultStorageLocationID, operations: operations)
            }
        }
        return result
    }

    @ToString
    static abstract class DefaultStorageOperationsId {
        StorageOperations operations
        abstract void applyToImage(Image image)
    }

    @ToString(includeSuper=true)
    static class ConfigDefaultStorageOperationsId extends DefaultStorageOperationsId {
        String name
        @Override
        void applyToImage(Image image) {
            image.storageLocationName = name
        }
    }
    @ToString(includeSuper=true)
    static class DatabaseDefaultStorageOperationsId extends DefaultStorageOperationsId {
        Long id
        @Override
        void applyToImage(Image image) {
            image.storageLocation = StorageLocation.get(id)
        }
    }

    /**
     * Check if an image exists without full database lookup if possible.
     * In single-storage scenarios, this just checks storage without DB hit.
     */
    boolean imageExists(String imageIdentifier) {
        StorageOperations ops = getStorageOperationsWithoutDbLookup()
        if (ops) {
            // Single storage location - check storage directly
            return ops.stored(imageIdentifier)
        }

        // Multi-storage - need DB lookup
        return Image.countByImageIdentifier(imageIdentifier) > 0
    }

    /**
     * Get the mime type for an image with minimal DB overhead.
     * Uses projection query to fetch only the mimeType field.
     * Returns null if image not found.
     */
    @Transactional(readOnly = true)
    String getMimeType(String imageIdentifier) {
        return Image.where {
            imageIdentifier == imageIdentifier
        }.projections {
            property('mimeType')
        }.get() as String
    }

    /**
     * Check if the given identifier represents actual image content (not audio/document).
     * This performs a minimal DB query for just the mimeType field.
     *
     * @return true if it's an image, false if it's audio/document/etc, null if not found
     */
    @Transactional(readOnly = true)
    Boolean isActuallyAnImage(String imageIdentifier) {
        def mimeType = getMimeType(imageIdentifier)
        if (mimeType == null) {
            return null // Not found
        }
        return mimeType.startsWith('image/')
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
