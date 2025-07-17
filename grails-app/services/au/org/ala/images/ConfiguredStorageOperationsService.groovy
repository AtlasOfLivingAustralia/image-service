package au.org.ala.images

import au.org.ala.images.storage.FileSystemStorageOperations
import au.org.ala.images.storage.S3StorageOperations
import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.storage.SwiftStorageOperations
import grails.core.GrailsApplication
import org.javaswift.joss.client.factory.AuthenticationMethod
import org.springframework.beans.factory.annotation.Autowired

import javax.annotation.PostConstruct

class ConfiguredStorageOperationsService {

    @Autowired
    GrailsApplication grailsApplication

    private StorageOperations configuredStorageOperations

    /**
     * Returns the configured StorageOperations instance if enabled, otherwise null.
     * @return The configured StorageOperations instance or null if not enabled.
     */
    StorageOperations getConfiguredStorageOperations() {
        return configuredStorageOperations
    }

    /**
     * Initializes the configured StorageOperations instance based on the configuration in application.yml.
     */
    @PostConstruct
    private void initializeConfiguredStorageOperations() {
        if (configuredStorageOperations != null) {
            return
        }

        def config = grailsApplication.config.imageservice.storageOperations
        if (!config || !config.enabled) {
            return
        }

        switch (config.type?.toLowerCase()) {
            case 'fs':
                configuredStorageOperations = new FileSystemStorageOperations(
                        basePath: config.basePath
                )
                break
            case 's3':
                configuredStorageOperations = new S3StorageOperations(
                        region: config.region,
                        bucket: config.bucket,
                        prefix: config.prefix ?: '',
                        accessKey: config.accessKey,
                        secretKey: config.secretKey,
                        containerCredentials: config.containerCredentials ?: false,
                        publicRead: config.publicRead ?: false,
                        redirect: config.redirect ?: false,
                        pathStyleAccess: config.pathStyleAccess ?: false,
                        hostname: config.hostname ?: '',
                        cloudfrontDomain: config.cloudfrontDomain ?: ''
                )
                break
            case 'swift':
                configuredStorageOperations = new SwiftStorageOperations(
                        authUrl: config.authUrl,
                        containerName: config.containerName,
                        username: config.username,
                        password: config.password,
                        tenantId: config.tenantId ?: '',
                        tenantName: config.tenantName ?: '',
                        authenticationMethod: AuthenticationMethod.valueOf(config.authenticationMethod),
                        publicContainer: config.publicContainer ?: false,
                        redirect: config.redirect ?: false
                )
                break
            default:
                throw new RuntimeException("Unknown storage operations type ${config.type}")
        }
    }
}