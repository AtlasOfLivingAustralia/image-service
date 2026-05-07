package au.org.ala.images

class BatchFileLoadJob {

    def batchService

    static concurrent = false
    static triggers = {
        simple repeatInterval: 5000l
    }

    def execute() {
        log.debug("Running batch file load job with interval")
        batchService.processNextInQueue()
    }
}
