package au.org.ala.images

import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Value

import java.text.DateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Job to periodically check failed upload URLs and remove entries if they're now accessible.
 * Configurable to run weekly, monthly, or quarterly.
 */
@Slf4j
class CheckFailedUploadsJob {

    public static final String LAST_CHECK_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    def imageService
    def downloadService
    def settingService
    
    @Value('${failedUpload.check.enabled:true}')
    boolean enabled = true
    
    @Value('${failedUpload.check.intervalDays:7}')
    int intervalDays = 7  // Default: weekly
    
    @Value('${failedUpload.check.batchSize:100}')
    int batchSize = 100
    
    @Value('${failedUpload.check.maxAgeDays:90}')
    int maxAgeDays = 90  // Don't retry URLs older than this
    
    static concurrent = false
    
    static triggers = {
        // Run daily at 2 AM - the actual interval is controlled by the intervalDays config
        cron name: 'checkFailedUploadsTrigger', cronExpression: '0 0 2 * * ?'
    }

    def execute(context) {
        if (!enabled) {
            log.debug("Failed upload checking is disabled")
            return
        }
        
        // Check for manual override parameter
        def forceRun = context?.mergedJobDataMap?.get('forceRun') ?: false

        // Check if we should run based on the last run date
        def setting = settingService.getFailedUploadLastCheck()
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(LAST_CHECK_DATE_FORMAT)
        LocalDateTime now = LocalDateTime.now()

        if (setting && !forceRun) {
            try {
                LocalDateTime localDateTime = LocalDateTime.parse(setting, formatter)

                def daysSinceLastCheck = Duration.between(localDateTime, now).toDays()
                
                if (daysSinceLastCheck < intervalDays) {
                    log.debug("Skipping failed upload check - last run was ${daysSinceLastCheck} days ago, interval is ${intervalDays} days")
                    return
                }
            } catch (Exception e) {
                log.warn("Could not parse last check date: ${e.message}")
            }
        }
        
        if (forceRun) {
            log.info("Manual execution of failed upload check (forced run)")
        }

        log.info("Starting failed upload check (interval: ${intervalDays} days, batch size: ${batchSize}, max age: ${maxAgeDays} days)")

        def cutoffDate = now.minusDays(maxAgeDays).toDate()
        def checked = 0
        def removed = 0
        def errors = 0
        
        try {
            // Get failed uploads that aren't too old
            def failedUploads = FailedUpload.createCriteria().scroll {
                ge('dateCreated', cutoffDate)
                order('dateCreated', 'asc')  // Check oldest first
            }

            try {
                while (failedUploads.next()) {
                    def failedUpload = scrollableResults.get(0) as FailedUpload
                    checked++

                    try {
                        // Try to access the URL to see if it's now available
                        def isNowAccessible = downloadService.checkUrlAccessible(failedUpload.url)

                        if (isNowAccessible) {
                            log.info("URL is now accessible, removing from failed list: ${failedUpload.url}")
                            failedUpload.delete(flush: true)
                            removed++
                        } else {
                            log.debug("URL still inaccessible: ${failedUpload.url}")
                        }
                    } catch (Exception e) {
                        log.warn("Error checking failed upload ${failedUpload.url}: ${e.message}")
                        errors++
                    }

                    // Add a small delay to avoid overwhelming remote servers
                    if (checked % 10 == 0) {
                        Thread.sleep(1000)  // 1 second pause every 10 URLs
                    }
                }
            } finally {
                if (failedUploads) {
                    failedUploads.close()
                }
            }

            // Update last check time
            settingService.setFailedUploadLastCheck(LocalDateTime.now().format(formatter))
            log.info("Failed upload check completed: checked ${checked}, removed ${removed}, errors ${errors}")
            
        } catch (Exception e) {
            log.error("Error during failed upload check: ${e.message}", e)
        }
    }
}
