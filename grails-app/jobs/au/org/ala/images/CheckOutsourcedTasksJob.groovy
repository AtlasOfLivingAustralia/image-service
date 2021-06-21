package au.org.ala.images

import groovy.time.TimeCategory

import java.text.SimpleDateFormat


class CheckOutsourcedTasksJob {

    def logService
    def imageService
    def settingService

    static concurrent = false
    static triggers = {
        simple repeatInterval: 30000l // execute job once every minute
    }

    def execute() {

        if (!settingService.outsourcedTaskCheckingEnabled) {
            return
        }

        def sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

        OutsourcedJob.withNewTransaction {
            def jobs = OutsourcedJob.list()
            jobs.each { job ->
                logService.debug("Checking if job ${job.id} as expired")
                // Check if expired
                use (TimeCategory) {
                    def date = job.expectedDurationInMinutes.minutes.ago
                    if (job.dateCreated.before(date)) {
                        logService.log("Outsourced job (ticket ${job.ticket} for image ${job.image.imageIdentifier}) has expired! Returning to queue.")
                        // new to rescheduled job
                        if (job.taskType == ImageTaskType.TMSTile) {
                            imageService.scheduleTileGeneration(job.image.id, "<unknown>")
                        }
                        job.delete()
                    }

                }

            }
        }

    }
}
