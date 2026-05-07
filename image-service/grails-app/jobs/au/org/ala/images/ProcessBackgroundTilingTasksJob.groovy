package au.org.ala.images

import groovy.util.logging.Slf4j

@Slf4j
class ProcessBackgroundTilingTasksJob {

    def imageService
    def settingService

    static concurrent = false

    static triggers = {
        simple repeatInterval: 10000l
    }

    def execute() {
        try {
            if (settingService.tilingEnabled) {
                imageService.processTileBackgroundTasks()
            }
        } catch (Exception ex) {
            log.error("Exception thrown in tiling job handler", ex)
            throw ex
        }
    }

}
