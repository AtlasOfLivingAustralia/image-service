package au.org.ala.images

import grails.testing.gorm.DataTest
import grails.testing.services.ServiceUnitTest
import spock.lang.Specification

class SettingServiceSpec extends Specification implements ServiceUnitTest<SettingService>, DataTest {

    def setup() {
        mockDomains Setting
    }

    def 'test getTilingEnabled'() {
        expect:
        service.getTilingEnabled() == true
    }

    def 'test getBackgroundTasksEnabled'() {
        expect:
        service.getBackgroundTasksEnabled() == true
    }

    def 'test getBackgroundTasksThreads'() {
        expect:
        service.getBackgroundTasksThreads() == 3
    }

    def 'test getOutsourcedTaskCheckingEnabled'() {
        expect:
        service.getOutsourcedTaskCheckingEnabled() == true
    }

    def 'test getPurgeStagedFilesEnabled'() {
        expect:
        service.getPurgeStagedFilesEnabled() == true
    }

    def 'test getStagedFileLifespanInHours'() {
        expect:
        service.getStagedFileLifespanInHours() == 24
    }

    def 'test getStorageLocationDefault'() {
        expect:
        service.getStorageLocationDefault() == 1
    }

    def 'test getBatchServiceThreads'() {
        expect:
        service.getBatchServiceThreads() == 5
    }

    def 'test getBatchServiceThrottleInMillis'() {
        expect:
        service.getBatchServiceThrottleInMillis() == 0
    }

    def 'test getBatchServiceReadSize'() {
        expect:
        service.getBatchServiceReadSize() == 250
    }

    def 'test getBatchServiceProcessingEnabled'() {
        expect:
        service.getBatchServiceProcessingEnabled() == true
    }

    def 'test enableBatchProcessing'() {
        setup:
        service.batchServiceProcessingEnabled

        when:
        service.enableBatchProcessing()

        then:
        service.getBatchServiceProcessingEnabled() == true

        when:
        service.disableBatchProcessing()
        service.enableBatchProcessing()

        then:
        service.getBatchServiceProcessingEnabled() == true
    }

    def 'test disableBatchProcessing'() {
        setup:
        service.batchServiceProcessingEnabled

        when:
        service.disableBatchProcessing()

        then:
        service.getBatchServiceProcessingEnabled() == false

        when:
        service.enableBatchProcessing()
        service.disableBatchProcessing()

        then:
        service.getBatchServiceProcessingEnabled() == false
    }

    def 'test setStorageLocationDefault'() {
        setup:
        service.storageLocationDefault

        when:
        service.setStorageLocationDefault(3)

        then:
        service.getStorageLocationDefault() == 3

        when:
        service.setStorageLocationDefault(5)

        then:
        service.getStorageLocationDefault() == 5
    }

    def 'test setSettingValue'() {
        setup:
        service.getBackgroundTasksThreads()

        when:
        service.setSettingValue('background.tasks.threads', '5')

        then:
        service.getBackgroundTasksThreads() == 5

    }

}
