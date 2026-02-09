package au.org.ala.images

import au.org.ala.images.storage.FileSystemStorageOperations
import au.org.ala.images.storage.S3StorageOperations
import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.storage.SwiftStorageOperations
import com.google.common.annotations.VisibleForTesting
import grails.core.GrailsApplication
import groovy.util.logging.Slf4j
import org.javaswift.joss.client.factory.AuthenticationMethod
import org.springframework.beans.factory.annotation.Autowired

import javax.annotation.PostConstruct
import java.util.concurrent.ConcurrentHashMap

/**
 * Registry for StorageOperations instances, supporting both config-based and database-based storage locations.
 * This allows high-performance access to storage operations without database lookups in single-storage scenarios.
 */
@Slf4j
class StorageOperationsRegistry {

    private final Map<String, StorageOperations> operationsByName = new ConcurrentHashMap<>()
    private volatile StorageOperations defaultOperations = null
    private volatile String defaultOperationsName = null
    private volatile boolean useConfigBasedStorage = false

    private volatile boolean initialized = false
    @VisibleForTesting
    final GrailsApplication grailsApplication

    private boolean invalidDefaultAllowed = false

    StorageOperationsRegistry(GrailsApplication grailsApplication, boolean invalidDefaultAllowed) {
        this.grailsApplication = grailsApplication
        this.invalidDefaultAllowed = invalidDefaultAllowed
        init()

    }

    StorageOperationsRegistry(GrailsApplication grailsApplication) {
        this(grailsApplication, false)
    }

    void init() {
        log.info("Initializing StorageOperationsRegistry")
        
        // Check if config-based storage is configured
        def storageConfig = grailsApplication.config.getProperty('imageservice.storage.locations', Map, [:])
        
        if (storageConfig && !storageConfig.isEmpty()) {
            log.info("Found ${storageConfig.size()} storage location(s) in configuration")
            useConfigBasedStorage = true
            initializeFromConfig(storageConfig)
        } else {
            log.info("No config-based storage locations found, will use database-based storage")
            useConfigBasedStorage = false
        }
        
        initialized = true
    }

    private void initializeFromConfig(Map storageConfig) {
        boolean defaultValid = true
        defaultOperations = null
        defaultOperationsName = null
        storageConfig.each { String name, Map config ->
            try {
                StorageOperations ops = createStorageOperations(name, config)
                if (ops) {
                    boolean valid = false
                    try {
                        valid = ops.verifySettings()
                    } catch (Exception e) {
                        log.error("Storage operations '{}' of type {} failed verification: {}", name, ops.class.simpleName, e.message)
                    }
                    if (!valid) {
                        log.error("Storage operations '{}' of type {} failed verification", name, ops.class.simpleName)
                    }

                    operationsByName.put(name, ops)
                    log.info("Registered storage operation '{}' of type {}", name, ops.class.simpleName)

                    // First one or explicitly marked as default becomes the default
                    if (defaultOperations == null || config.default == true) {
                        defaultValid = valid
                        defaultOperations = ops
                        defaultOperationsName = name
                        log.info("Set '{}' as default storage operation", name)
                    }
                }
            } catch (Exception e) {
                log.error("Failed to initialize storage operation '{}': {}", name, e.message, e)
            }
        }

        if (operationsByName.isEmpty()) {
            log.warn("No storage operations were successfully initialized from configuration")
            useConfigBasedStorage = false
        }

        // final check, make sure the default is valid
        if (defaultOperations != null && !defaultValid && !invalidDefaultAllowed) {
            log.error("Default storage operation '{}' is not valid", defaultOperationsName)
            throw new IllegalStateException("Default storage operation '${defaultOperationsName}' failed validation")
        }
    }

    private StorageOperations createStorageOperations(String name, Map config) {
        String type = config.type?.toString()?.toLowerCase()
        
        switch (type) {
            case 'fs':
            case 'filesystem':
                return new FileSystemStorageOperations(basePath: config.basePath as String, probeFilesForImageInfo: true)
                
            case 's3':
                return new S3StorageOperations(
                    bucket: config.bucket as String,
                    region: config.region as String,
                    prefix: (config.prefix ?: '') as String,
                    accessKey: config.accessKey as String,
                    secretKey: config.secretKey as String,
                    containerCredentials: config.containerCredentials as Boolean ?: false,
                    publicRead: config.publicRead as Boolean ?: false,
                    privateAcl: config.privateAcl as Boolean ?: false,
                    redirect: config.redirect as Boolean ?: false,
                    pathStyleAccess: config.pathStyleAccess as Boolean ?: false,
                    hostname: config.hostname as String ?: '',
                    cloudfrontDomain: config.cloudfrontDomain as String ?: ''
                )
                
            case 'swift':
                return new SwiftStorageOperations(
                    authUrl: config.authUrl as String,
                    username: config.username as String,
                    password: config.password as String,
                    tenantId: (config.tenantId ?: '') as String,
                    tenantName: (config.tenantName ?: '') as String,
                    containerName: config.containerName as String,
                    authenticationMethod: config.authenticationMethod ? 
                        AuthenticationMethod.valueOf(config.authenticationMethod as String) : 
                        AuthenticationMethod.BASIC,
                    publicContainer: config.publicContainer as Boolean ?: false,
                    redirect: config.redirect as Boolean ?: false,
                    mock: config.mock as Boolean ?: false
                )
                
            default:
                log.error("Unknown storage type '{}' for location '{}'", type, name)
                return null
        }
    }

    /**
     * Get storage operations by name (for multi-tenant scenarios where images specify storage location name)
     */
    StorageOperations getByName(String name) {
        return operationsByName.get(name)
    }

    /**
     * Get the default storage operations (for single-storage scenarios)
     */
    StorageOperations getDefault() {
        return defaultOperations
    }

    /**
     * Get the config name of the default storage operations
     */
    String getDefaultName() {
        return defaultOperationsName
    }

    /**
     * Check if only config-based storage is being used (no database storage locations)
     */
    boolean isUsingConfigBasedStorage() {
        return useConfigBasedStorage
    }

    /**
     * Check if there's exactly one storage location configured
     */
    boolean hasSingleStorageLocation() {
        return operationsByName.size() == 1
    }

    /**
     * Get count of configured storage locations
     */
    int getStorageLocationCount() {
        return operationsByName.size()
    }

    /**
     * Get all storage operation names
     */
    Set<String> getStorageLocationNames() {
        return Collections.unmodifiableSet(operationsByName.keySet())
    }
    
    /**
     * Reverse lookup: Find the name for a storage operations instance.
     * Used when saving new images to populate the storageLocationName field.
     */
    String getNameForOperations(StorageOperations operations) {
        if (operations == null) {
            return null
        }
        return operationsByName.find { k, v -> v == operations || v.is(operations) }?.key
    }

    /**
     * Check if registry is initialized
     */
    boolean isInitialized() {
        return initialized
    }
}

