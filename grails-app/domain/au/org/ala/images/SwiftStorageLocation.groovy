package au.org.ala.images

import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.storage.SwiftStorageOperations
import groovy.transform.EqualsAndHashCode
import org.javaswift.joss.client.factory.AuthenticationMethod
@EqualsAndHashCode(includes=['authUrl', 'containerName', 'tenantId', 'tenantName', 'publicContainer'])
class SwiftStorageLocation extends StorageLocation {

    AuthenticationMethod authenticationMethod = AuthenticationMethod.BASIC
    String authUrl
    String username
    String password

    String tenantId
    String tenantName

    String containerName

    boolean publicContainer
    boolean redirect
    boolean mock = false

    static constraints = {
        tenantId nullable: false, blank: true
        tenantName nullable: false, blank: true
    }

    static mapping = {
        cache true
    }

    static transients = ['_account', '_container', 'mock', 'storageOperations', '$storageOperations']

    @Delegate @Lazy SwiftStorageOperations storageOperations = {
        new SwiftStorageOperations(
                authUrl: authUrl,
                username: username,
                password: password,
                tenantId: tenantId,
                tenantName: tenantName,
                containerName: containerName,
                publicContainer: publicContainer,
                redirect: redirect,
                mock: mock
        )
    }()

    StorageOperations asStandaloneStorageOperations() {
        storageOperations
    }

    @Override
    String toString() {
        return "SwiftStorageLocation $id $authUrl $tenantId $tenantName $containerName"
    }
}
