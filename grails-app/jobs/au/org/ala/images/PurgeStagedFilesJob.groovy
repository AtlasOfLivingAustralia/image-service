package au.org.ala.images

class PurgeStagedFilesJob {

    def logService
    def settingService
    def imageStagingService

    static concurrent = false
    static triggers = {
        simple repeatInterval: 15 * 60 * 1000; // 15 minutes
    }

    def execute() {
        if (!settingService.purgeStagedFilesEnabled) {
            return
        }
        imageStagingService.purgeOldStagedFiles()
    }
}
