package au.org.ala.images

import groovy.util.logging.Slf4j

@Slf4j
class ProcessBackgroundTasksJob {

    def imageService
    def settingService

    static concurrent = false

    static triggers = {
        simple repeatInterval: 1000l
    }

    def execute() {
        try {
            if (settingService.backgroundTasksEnabled) {
                imageService.processBackgroundTasks()
            }
        } catch (Exception ex) {
            log.error("Exception thrown in job handler", ex)
            throw ex
        }
    }
}
