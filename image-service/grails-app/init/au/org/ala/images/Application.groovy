package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ProcessCommandExecutor
import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import groovy.util.logging.Slf4j
import org.springframework.context.annotation.Bean
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

import javax.annotation.PostConstruct
import javax.imageio.ImageIO
import java.util.concurrent.RejectedExecutionHandler

@Slf4j
class Application extends GrailsAutoConfiguration {

    public static final int TILE_SIZE = 256

    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

    @Bean
    TaskExecutor analyticsExecutor() {
        return createThreadPoolTaskExecutor("analytics-", 1)
    }

    @Bean
    TaskExecutor storageLocationExecutor() {
        return createThreadPoolTaskExecutor("storage-", 1)
    }

    @Bean
    StorageOperationsRegistry storageOperationsRegistry() {
        return new StorageOperationsRegistry(this.grailsApplication)
    }

    // This is a workaround, instead of using @EnableConfigurationProperties above
    // we create the bean manually then load it using the ImageOptimisationConfigLoader
    // (registered via AutoConfiguration.imports)
    @Bean
    ImageOptimisationConfig imageOptimisationConfig() {
        return new ImageOptimisationConfig()
    }

    @Bean
    CommandExecutor commandExecutor() {
        return new ProcessCommandExecutor()
    }

    @PostConstruct
    void init() {
        log.info("Initialising ImageIO settings...")
        ImageIO.scanForPlugins()
        ImageIO.setUseCache(false)
    }

    static TaskExecutor createThreadPoolTaskExecutor(String namePrefix, int coreSize, int maxSize = coreSize, int queueCapacity = -1, RejectedExecutionHandler rejectionHandler = null) {
        log.info("executor[{}]: requested core={} max={} queue={} rejection={} -> applied core={} max={} queue={}",
                namePrefix, coreSize, maxSize, queueCapacity, rejectionHandler?.class?.simpleName,
                Math.max(1, coreSize), Math.max(1, maxSize), queueCapacity >= 0 ? queueCapacity : 'unbounded')
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor()
        executor.corePoolSize = Math.max(1, coreSize)
        executor.maxPoolSize = Math.max(1, maxSize)
        if (queueCapacity >= 0) {
            executor.queueCapacity = queueCapacity
        }
        executor.threadNamePrefix = namePrefix
        executor.waitForTasksToCompleteOnShutdown = true
        executor.awaitTerminationSeconds = 10
        if (rejectionHandler != null) {
            executor.rejectedExecutionHandler = rejectionHandler
        }
        return executor
    }
}
