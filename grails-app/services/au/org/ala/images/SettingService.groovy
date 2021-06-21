package au.org.ala.images

import grails.gorm.transactions.Transactional
import org.codehaus.groovy.runtime.StackTraceUtils

import java.lang.reflect.Method
import java.util.concurrent.locks.ReentrantLock

/**
 * Settings service. Use reflection and annotations to ensure that settings are created at startup
 * To add a new setting simply define a getter with the preferred type of the setting (currently boolean or String)
 * and mark it with the @ImageServiceSetting annotation to describe it's name, description and default (initial) value
 */
class SettingService {

    def logService

    @ImageServiceSetting(name = 'background.tiling.enabled', description = "Should the service perform image tiling in the background", defaultValue = "true")
    boolean getTilingEnabled() {
        return getBoolSetting()
    }

    @ImageServiceSetting(name = 'background.tasks.enabled', description = "Should the service perform thumbnailing and ingesting", defaultValue = "true")
    boolean getBackgroundTasksEnabled() {
        return getBoolSetting()
    }

    @ImageServiceSetting(name = 'background.tasks.threads', description = "Number of threads the service perform thumbnailing and ingesting", defaultValue = "3")
    Integer getBackgroundTasksThreads() {
        return getIntSetting()
    }

    @ImageServiceSetting(name = 'outsourced.task.checking.enabled', description = "Should the service check the status of outsourced tiling jobs", defaultValue = "true")
    boolean getOutsourcedTaskCheckingEnabled() {
        return getBoolSetting()
    }

    @ImageServiceSetting(name = 'stagedimages.purge.stalefiles', description = "Should the service automatically purge old staged files", defaultValue = "true")
    boolean getPurgeStagedFilesEnabled() {
        return getBoolSetting()
    }

    @ImageServiceSetting(name = 'stagedimages.file.lifespan.hours', description = "How long should a user staged file hang around before being cleaned up (in hours)", defaultValue = "24")
    Integer getStagedFileLifespanInHours() {
        return getIntSetting()
    }

    @ImageServiceSetting(name = 'storage.location.default', description = "The ID of the default storage location", defaultValue = '1')
    Long getStorageLocationDefault() {
        return getLongSetting()
    }

    @ImageServiceSetting(name = 'batch.service.threads', description = "Number of download threads to use", defaultValue = '5')
    Long getBatchServiceThreads() {
        return getLongSetting()
    }

    @ImageServiceSetting(name = 'batch.service.throttle.milliseconds', description = "Time to sleep between download requests", defaultValue = '0')
    Long getBatchServiceThrottleInMillis() {
        return getLongSetting()
    }

    @ImageServiceSetting(name = 'batch.service.read.size', description = "Number of records to read in single batch", defaultValue = '250')
    Long getBatchServiceReadSize() {
        return getLongSetting()
    }

    @ImageServiceSetting(name = 'batch.service.processing.enabled', description = "Should the service process files on the queue", defaultValue = "true")
    boolean getBatchServiceProcessingEnabled() {
        getBoolSetting()
    }

    void enableBatchProcessing(){
        setSettingValue( 'batch.service.processing.enabled', "true")
    }

    void disableBatchProcessing(){
        setSettingValue( 'batch.service.processing.enabled', "false")
    }

    void setStorageLocationDefault(Long value) {
        setSettingValue('storage.location.default', value.toString())
    }

    @Transactional
    void setSettingValue(String name, String value) {
        def setting = Setting.findByName(name)
        if (!setting) {
            throw new RuntimeException("No such setting: ${name}")
        }

        if (setting.type == SettingType.Boolean) {
            // Ensure it can be parse as boolean
            Boolean.parseBoolean(value)
        }
        setting.value = value
        setting.save()
    }

    /**
     * The magic sauce that attempts to find a method in the current callstack that has the ServiceSetting annotation
     * It needs to trawl the stack because we have no idea what proxies or shims have been wrapped around this class by grails/spring
     *
     * @return A Setting, if one could be found
     */
    private Setting getSettingFromStack() {

        // Look for a method in the stack marked with the settings annotation
        ImageServiceSetting annotation = null
        Method method = null
        StackTraceUtils.sanitize(new Throwable()).stackTrace.find { stack ->
            def candidate = SettingService.class.declaredMethods.find { it.name == stack.methodName }
            if (candidate) {
                def ann = candidate.annotations?.find { ImageServiceSetting.class.isAssignableFrom(it.annotationType()) } as ImageServiceSetting
                if (ann) {
                    annotation = ann
                    method = candidate
                    return true
                }
            }
        }

        if (annotation && method) {
            def settingType = SettingType.String
            if (method.returnType.is(Boolean) || method.returnType.is(boolean)) {
                settingType = SettingType.Boolean
            }

            def setting = getOrCreateSetting(annotation.name(), settingType, annotation.defaultValue(), annotation.description())
            return setting
        }

        throw new RuntimeException("Failed to extract annotation from setting method")
    }

    private boolean getBoolSetting() {
        def setting = getSettingFromStack()
        return Boolean.parseBoolean(setting.value)
    }

    private String getStringSetting() {
        def setting = getSettingFromStack()
        return setting.value
    }

    private Integer getIntSetting() {
        def setting = getSettingFromStack()

        return Integer.parseInt(setting.value)
    }

    private Long getLongSetting() {
        def setting = getSettingFromStack()

        return Long.parseLong(setting.value)
    }

    private ReentrantLock getOrCreateSettingLock = new ReentrantLock()

    private Setting getOrCreateSetting(String key, SettingType type, String defaultValue, String description) {
        def setting = Setting.findByName(key)

        if (!setting) {
            getOrCreateSettingLock.lock()
            try {
                setting = Setting.findByName(key)
                if (!setting) {
                    setting = new Setting(name: key, type: type, value: defaultValue, description: description)
                    setting.save(flush: true, failOnError: true)
                }
            } finally {
                getOrCreateSettingLock.unlock()
            }
        }
        return setting
    }

    def ensureSettingsCreated() {
        def methods = SettingService.class.getDeclaredMethods()
        methods.each { method ->
            def annotation = method.declaredAnnotations?.find {
                ImageServiceSetting.class.isAssignableFrom(it.annotationType())
            } as ImageServiceSetting
            if (annotation) {
                try {
                    def settingType = SettingType.String
                    if (method.returnType.is(Boolean) || method.returnType.is(boolean)) {
                        settingType = SettingType.Boolean
                    }
                    def setting = getOrCreateSetting(annotation.name(),settingType,annotation.defaultValue(), annotation.description())
                    logService.log("${method.name} returns \"${setting?.value}\"")
                } catch (Exception ex) {
                    logService.error("", ex)
                }
            }
        }
    }
}
