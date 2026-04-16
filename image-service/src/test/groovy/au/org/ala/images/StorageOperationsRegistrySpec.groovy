package au.org.ala.images

import org.grails.testing.GrailsUnitTest
import spock.lang.Specification

/**
 * Unit test for StorageOperationsRegistry
 */
class StorageOperationsRegistrySpec extends Specification implements GrailsUnitTest {

    StorageOperationsRegistry service

    void setup() {
        service = new StorageOperationsRegistry(grailsApplication, true) // allow invalid default for testing
    }

    void "test initialization with no config"() {
        when:
        service.init()
        
        then:
        !service.isUsingConfigBasedStorage()
        service.getDefault() == null
        service.getStorageLocationCount() == 0
    }

    void "test initialization with single filesystem storage"() {
        given:
        grailsApplication.config.imageservice = [
            storage: [
                locations: [
                    primary: [
                        type: 'fs',
                        basePath: '/tmp/test-images'
                    ]
                ]
            ]
        ]
        
        when:
        service.init()
        
        then:
        service.isUsingConfigBasedStorage()
        service.hasSingleStorageLocation()
        service.getDefault() != null
        service.getStorageLocationCount() == 1
        service.getStorageLocationNames().contains('primary')
    }

    void "test initialization with multiple storage locations"() {
        given:
        grailsApplication.config.imageservice = [
            storage: [
                locations: [
                    primary: [
                        type: 'fs',
                        basePath: '/tmp/test-images',
                        default: true
                    ],
                    backup: [
                        type: 'fs',
                        basePath: '/tmp/backup-images'
                    ]
                ]
            ]
        ]
        
        when:
        service.init()
        
        then:
        service.isUsingConfigBasedStorage()
        !service.hasSingleStorageLocation()
        service.getDefault() != null
        service.getStorageLocationCount() == 2
        service.getByName('primary') != null
        service.getByName('backup') != null
    }

    void "test S3 storage initialization"() {
        given:
        grailsApplication.config.imageservice = [
            storage: [
                locations: [
                    s3storage: [
                        type: 's3',
                        region: 'us-east-1',
                        bucket: 'test-bucket',
                        prefix: 'images/',
                        publicRead: true
                    ]
                ]
            ]
        ]
        
        when:
        service.init()
        
        then:
        service.isUsingConfigBasedStorage()
        service.getDefault() != null
        service.getByName('s3storage') != null
    }

    void "test default selection"() {
        given:
        grailsApplication.config.imageservice = [
            storage: [
                locations: [
                    first: [
                        type: 'fs',
                        basePath: '/tmp/first'
                    ],
                    second: [
                        type: 'fs',
                        basePath: '/tmp/second',
                        default: true
                    ]
                ]
            ]
        ]
        
        when:
        service.init()
        
        then:
        // 'second' should be default because it's explicitly marked
        service.getDefault() == service.getByName('second')
    }
}

