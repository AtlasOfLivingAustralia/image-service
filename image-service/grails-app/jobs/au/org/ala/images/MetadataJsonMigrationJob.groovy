package au.org.ala.images

import grails.core.GrailsApplication
import grails.gorm.transactions.Transactional
import groovy.util.logging.Slf4j

import java.lang.management.ManagementFactory
import java.time.LocalTime

/**
 * Quartz job to migrate metadata from EAV table (image_meta_data_item) to JSONB column (metadata).
 * Runs during low-usage hours (configurable) to avoid impacting production workload.
 * Uses batching and pagination to handle large datasets without overwhelming the database.
 */
@Slf4j
class MetadataJsonMigrationJob {

    def imageService
    GrailsApplication grailsApplication

    static concurrent = false

    static triggers = {
        // Run every minute to check if we should migrate
        cron name: 'metadataJsonMigrationTrigger', cronExpression: '0 0/1 * * * ?'
    }

    def execute() {
        try {
            // Check if migration is enabled
            boolean migrationEnabled = grailsApplication.config.getProperty('images.metadata.jsonb.migration.enabled', Boolean, false)
            if (!migrationEnabled) {
                log.trace("Metadata JSONB migration is disabled")
                return
            }

            // Check if we're in the allowed time window
            if (!isInMigrationWindow()) {
                log.trace("Not in migration time window")
                return
            }

            // Check system load if configured
            if (isSystemLoadTooHigh()) {
                log.debug("System load too high, skipping migration")
                return
            }

            int batchSize = grailsApplication.config.getProperty('images.metadata.jsonb.migration.batchSize', Integer, 100)
            int maxBatchesPerRun = grailsApplication.config.getProperty('images.metadata.jsonb.migration.maxBatchesPerRun', Integer, 10)

            log.info("Starting metadata JSONB migration run (batchSize={}, maxBatches={})", batchSize, maxBatchesPerRun)

            int totalMigrated = 0
            int batchesProcessed = 0

            while (batchesProcessed < maxBatchesPerRun) {
                int migrated = migrateBatch(batchSize)
                totalMigrated += migrated
                batchesProcessed++

                if (migrated == 0) {
                    log.info("No more images to migrate")
                    break
                }

                // Check if we should stop due to time window or load
                if (!isInMigrationWindow() || isSystemLoadTooHigh()) {
                    log.info("Stopping migration: window closed or load too high")
                    break
                }
            }

            if (totalMigrated > 0) {
                log.info("Metadata JSONB migration run completed: {} images migrated in {} batches", totalMigrated, batchesProcessed)
            }

        } catch (Exception ex) {
            log.error("Exception in metadata JSONB migration job", ex)
        }
    }

    /**
     * Migrate a batch of images from EAV to JSONB
     * @param batchSize Number of images to process
     * @return Number of images actually migrated
     */
    @Transactional
    int migrateBatch(int batchSize) {
        // Find images without metadata (null indicates not yet migrated)
        def images = Image.createCriteria().list(max: batchSize) {
            isNull('metadata')
//            isNull('dateDeleted')  // Skip deleted images
            order('id', 'asc')     // Process in ID order for predictability
        }

        if (!images) {
            return 0
        }

        log.debug("Migrating batch of {} images", images.size())

        int migrated = 0
        images.each { Image image ->
            try {
                migrateImageMetadata(image)
                migrated++
            } catch (Exception ex) {
                log.error("Failed to migrate metadata for image {}: {}", image.id, ex.message, ex)
                // Mark as migrated with empty map so we don't keep retrying
                image.metadata = [:]
                image.save(flush: false)
            }
        }

        return migrated
    }

    /**
     * Migrate metadata for a single image from EAV to JSONB
     * @param image The image to migrate
     */
    private void migrateImageMetadata(Image image) {
        def metadataItems = ImageMetaDataItem.findAllByImage(image)

        if (metadataItems) {
            // Build JSONB map with nested structure: { "key": { "value": "...", "source": "..." } }
            def jsonMap = [:]
            metadataItems.each { md ->
                jsonMap[md.name] = [
                    value: md.value,
                    source: md.source.toString()
                ]
            }
            image.metadata = jsonMap
            log.trace("Migrated {} metadata items for image {}", metadataItems.size(), image.id)
        } else {
            // No metadata - set to empty map to mark as processed
            image.metadata = [:]
            log.trace("No metadata to migrate for image {}, marking as processed", image.id)
        }

        image.save(flush: false)
    }

    /**
     * Check if current time is within the configured migration window
     */
    private boolean isInMigrationWindow() {
        // Default: 11 PM to 6 AM (low usage hours)
        String startTime = grailsApplication.config.getProperty('images.metadata.jsonb.migration.startTime', String, '23:00')
        String endTime = grailsApplication.config.getProperty('images.metadata.jsonb.migration.endTime', String, '06:00')

        try {
            LocalTime now = LocalTime.now()
            LocalTime start = LocalTime.parse(startTime)
            LocalTime end = LocalTime.parse(endTime)

            // Handle overnight window (e.g., 23:00 to 06:00)
            if (start.isAfter(end)) {
                return now.isAfter(start) || now.isBefore(end)
            } else {
                return now.isAfter(start) && now.isBefore(end)
            }
        } catch (Exception ex) {
            log.warn("Invalid time configuration for migration window: start={}, end={}", startTime, endTime, ex)
            return false
        }
    }

    /**
     * Check if system load is too high to run migration
     * Can be extended to check actual system metrics
     */
    private boolean isSystemLoadTooHigh() {
        // Simple implementation - can be enhanced to check actual load average
        double maxLoadAverage = grailsApplication.config.getProperty('images.metadata.jsonb.migration.maxLoadAverage', Double, 6.0d)

        // disable check if maxLoadAverage <= 0
        if (maxLoadAverage <= 0.0d) {
            return false
        }

        try {
            def os = ManagementFactory.getOperatingSystemMXBean()
            if (os.metaClass.respondsTo(os, 'getSystemLoadAverage')) {
                double load = os.systemLoadAverage
                if (load > 0 && load > maxLoadAverage) {
                    log.debug("System load {} exceeds max {}", load, maxLoadAverage)
                    return true
                }
            }
        } catch (Exception ex) {
            log.debug("Could not check system load: {}", ex.message)
        }

        return false
    }
}

