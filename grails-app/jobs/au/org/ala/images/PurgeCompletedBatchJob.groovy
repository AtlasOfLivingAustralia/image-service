package au.org.ala.images

class PurgeCompletedBatchJob {

    def batchService

    static concurrent = false
    static triggers = {
        simple repeatInterval: 24 * 60 * 60 * 1000; // once a day
    }

    def execute() {
        log.info("Running purge of completed jobs")
        batchService.purgeCompletedJobs()
    }
}
