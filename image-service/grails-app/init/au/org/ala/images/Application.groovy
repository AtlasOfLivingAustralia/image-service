package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ProcessCommandExecutor
import au.org.ala.images.thumb.DelegatingImageThumbnailer
import au.org.ala.images.tiling.DelegatingImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TileFormat
import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.core.task.TaskExecutor

import java.awt.Color
import java.util.concurrent.Executor

//@EnableConfigurationProperties(ImageOptimisationConfig)
@Slf4j
class Application extends GrailsAutoConfiguration {
    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

//    @Autowired
//    ImageOptimisationConfig imageOptimisationConfig

    @Value('${images.streamingTool:vips}')
    String streamingTool

    @Value('${images.preferJna:true}')
    boolean preferJna

    @Value('${imageservice.tiling.io.threads:${tiling.ioThreads:2}}')
    int tilingIoThreads

    @Value('${imageservice.tiling.level.threads:${tiling.levelThreads:2}}')
    int tilingLevelThreads

    @Value('${imageservice.tiling.io.virtualThreads:${tiling.ioVirtualThreads:true}}')
    boolean tilingIoVirtualThreads

    @Bean
    TaskExecutor analyticsExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor()
        executor.setCorePoolSize(1)
        executor.setMaxPoolSize(1)
        executor.setThreadNamePrefix("analytics-")
        executor.setWaitForTasksToCompleteOnShutdown(true)
        executor.setAwaitTerminationSeconds(10)
        return executor
    }

    @Bean
    StorageOperationsRegistry storageOperationsRegistry() {
        return new StorageOperationsRegistry(this.grailsApplication)
    }

    // This is a workaround, instead of using @EnableConfigurationProperties above
    // we create the bean manually then load it using the ImageOptimisationConfigLoader
    // (which is a @Component)
    @Bean
    ImageOptimisationConfig imageOptimisationConfig() {
        return new ImageOptimisationConfig()
    }

    @Bean
    CommandExecutor commandExecutor() {
        return new ProcessCommandExecutor()
    }

    @Bean
    DelegatingImageThumbnailer delegatingImageThumbnailer(CommandExecutor commandExecutor) {
        return new DelegatingImageThumbnailer(commandExecutor, streamingTool, preferJna)
    }

    @Bean
    TaskExecutor tilingIoPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor()
        if (tilingIoVirtualThreads) {
            try {
                executor.setThreadFactory(Thread.ofVirtual().name("tiling-io-pool-", 0).factory())
                executor.setCorePoolSize(0)
                executor.setMaxPoolSize(Integer.MAX_VALUE)
                executor.setQueueCapacity(0)
            } catch (NoSuchMethodError | Exception e) {
                log.warn("Unable to use virtual threads for tiling IO pool, falling back to regular thread pool. Reason: {}", e.toString())
                // Fallback if not on Java 21+ or other issues with virtual threads
                int poolSize = Math.max(1, tilingIoThreads)
                executor.setCorePoolSize(poolSize)
                executor.setMaxPoolSize(poolSize)
                executor.setThreadNamePrefix("tiling-io-pool-")
            }
        } else {
            int poolSize = Math.max(1, tilingIoThreads)
            executor.setCorePoolSize(poolSize)
            executor.setMaxPoolSize(poolSize)
            executor.setThreadNamePrefix("tiling-io-pool-")
        }
        executor.setWaitForTasksToCompleteOnShutdown(true)
        executor.setAwaitTerminationSeconds(10)
        return executor
    }

    @Bean
    TaskExecutor tilingWorkPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor()
        int poolSize = Math.max(1, tilingLevelThreads)
        executor.setCorePoolSize(poolSize)
        executor.setMaxPoolSize(poolSize)
        executor.setThreadNamePrefix("tiling-work-pool-")
        executor.setWaitForTasksToCompleteOnShutdown(true)
        executor.setAwaitTerminationSeconds(10)
        return executor
    }

    @Bean
    ImageTilerConfig imageTilerConfig(Executor tilingIoPool, Executor tilingWorkPool) {
        def config = new ImageTilerConfig(tilingIoPool, tilingWorkPool, 256, 6, TileFormat.JPEG)
        config.setTileBackgroundColor(new Color(221, 221, 221))
        return config
    }

    @Bean
    DelegatingImageTiler delegatingImageTiler(CommandExecutor commandExecutor, ImageTilerConfig imageTilerConfig) {
        return new DelegatingImageTiler(commandExecutor, imageTilerConfig, streamingTool, preferJna)
    }
}