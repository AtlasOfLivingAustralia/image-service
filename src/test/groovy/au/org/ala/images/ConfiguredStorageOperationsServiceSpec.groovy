package au.org.ala.images

import au.org.ala.images.storage.FileSystemStorageOperations
import au.org.ala.images.storage.S3StorageOperations
import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.storage.SwiftStorageOperations
import grails.core.GrailsApplication
import grails.testing.services.ServiceUnitTest
import org.javaswift.joss.client.factory.AuthenticationMethod
import spock.lang.Specification

class ConfiguredStorageOperationsServiceSpec extends Specification implements ServiceUnitTest<ConfiguredStorageOperationsService> {

    def setup() {
    }

    def cleanup() {
    }

    void "test getConfiguredStorageOperations returns null when not enabled"() {
        given:
        def config = [imageservice: [storageOperations: [enabled: false]]]
        service.grailsApplication = [config: config] as GrailsApplication

        when:
        def result = service.getConfiguredStorageOperations()

        then:
        result == null
    }

    void "test getConfiguredStorageOperations returns FileSystemStorageOperations when configured"() {
        given:
        def config = [imageservice: [storageOperations: [
            enabled: true,
            type: 'fs',
            basePath: '/test/path'
        ]]]
        service.grailsApplication = [config: config] as GrailsApplication

        when:
        def result = service.getConfiguredStorageOperations()

        then:
        result instanceof FileSystemStorageOperations
        result.basePath == '/test/path'
    }

    void "test getConfiguredStorageOperations returns S3StorageOperations when configured"() {
        given:
        def config = [imageservice: [storageOperations: [
            enabled: true,
            type: 's3',
            region: 'test-region',
            bucket: 'test-bucket',
            prefix: 'test-prefix',
            accessKey: 'test-access-key',
            secretKey: 'test-secret-key',
            publicRead: true,
            redirect: true
        ]]]
        service.grailsApplication = [config: config] as GrailsApplication

        when:
        def result = service.getConfiguredStorageOperations()

        then:
        result instanceof S3StorageOperations
        result.region == 'test-region'
        result.bucket == 'test-bucket'
        result.prefix == 'test-prefix'
        result.accessKey == 'test-access-key'
        result.secretKey == 'test-secret-key'
        result.publicRead
        result.redirect
    }

    void "test getConfiguredStorageOperations returns SwiftStorageOperations when configured"() {
        given:
        def config = [imageservice: [storageOperations: [
            enabled: true,
            type: 'swift',
            authUrl: 'test-auth-url',
            containerName: 'test-container',
            username: 'test-username',
            password: 'test-password',
            tenantId: 'test-tenant-id',
            tenantName: 'test-tenant-name',
            authenticationMethod: 'BASIC',
            publicContainer: true,
            redirect: true
        ]]]
        service.grailsApplication = [config: config] as GrailsApplication

        when:
        def result = service.getConfiguredStorageOperations()

        then:
        result instanceof SwiftStorageOperations
        result.authUrl == 'test-auth-url'
        result.containerName == 'test-container'
        result.username == 'test-username'
        result.password == 'test-password'
        result.tenantId == 'test-tenant-id'
        result.tenantName == 'test-tenant-name'
        result.authenticationMethod == AuthenticationMethod.BASIC
        result.publicContainer
        result.redirect
    }

    void "test getConfiguredStorageOperations throws exception for unknown type"() {
        given:
        def config = [imageservice: [storageOperations: [
            enabled: true,
            type: 'unknown'
        ]]]
        service.grailsApplication = [config: config] as GrailsApplication

        when:
        service.getConfiguredStorageOperations()

        then:
        thrown(RuntimeException)
    }
}