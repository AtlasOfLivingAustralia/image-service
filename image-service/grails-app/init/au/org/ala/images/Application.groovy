package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import au.org.ala.images.iiif.DelegatingIiifImageProcessor
import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.iiif.JavaIiifImageProcessor
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ProcessCommandExecutor
import au.org.ala.images.thumb.DelegatingImageThumbnailer
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ImageThumbnailer
import au.org.ala.images.tiling.DelegatingImageTiler
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.ImageTiler3
import au.org.ala.images.tiling.ImageTiler4
import au.org.ala.images.tiling.ImageTiler5
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TilerVersion
import au.org.ala.images.tiling.TileFormat
import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import groovy.util.logging.Slf4j
import org.apache.commons.lang3.exception.ExceptionUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.core.task.TaskExecutor

import java.awt.Color
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.Executor

//@EnableConfigurationProperties(ImageOptimisationConfig)
@Slf4j
class Application extends GrailsAutoConfiguration {

    public static final int TILE_SIZE = 256

    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

//    @Autowired
//    ImageOptimisationConfig imageOptimisationConfig

    @Value('${images.streamingTool:vips}')
    String streamingTool

    @Value('${images.preferJna:true}')
    boolean preferJna

    @Value('${tiling.tiler.version:V4}')
    TilerVersion tilerVersion

    @Value('${tiling.tiler.class:}')
    String tilerClassName

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
    TaskExecutor storageLocationExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor()
        executor.setCorePoolSize(1)
        executor.setMaxPoolSize(2)
        executor.setThreadNamePrefix("storage-")
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
    IImageThumbnailer fallbackThumbnailer() {
        return new ImageThumbnailer()
    }

    @Bean("imageThumbnailer")
    @ConditionalOnProperty(name = "images.useStreamingThumbnailer", havingValue = "true")
    IImageThumbnailer streamingImageThumbnailer(CommandExecutor commandExecutor, @Qualifier("fallbackThumbnailer") IImageThumbnailer fallbackThumbnailer) {
        return new DelegatingImageThumbnailer(commandExecutor, fallbackThumbnailer, streamingTool, preferJna)
    }

    @Bean("imageThumbnailer")
    @ConditionalOnProperty(name = "images.useStreamingThumbnailer", havingValue = "false", matchIfMissing = true)
    IImageThumbnailer defaultImageThumbnailer(@Qualifier("fallbackThumbnailer") IImageThumbnailer fallbackThumbnailer) {
        return fallbackThumbnailer
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
    ImageTilerConfig imageTilerConfig(@Qualifier("tilingIoPool") Executor tilingIoPool, @Qualifier("tilingWorkPool") Executor tilingWorkPool) {
        def config = new ImageTilerConfig(tilingIoPool, tilingWorkPool, TILE_SIZE, 6, TileFormat.JPEG)
        config.setTileBackgroundColor(new Color(221, 221, 221))
        return config
    }

    @Bean
    IImageTiler fallbackTiler(ImageTilerConfig config) {
        switch (tilerVersion) {
            case TilerVersion.V1:
                log.trace("Tiler version V1 is deprecated, using V3 instead")
                return new ImageTiler3(config)
            case TilerVersion.V3:
                log.trace("Using Tiler version V3")
                return new ImageTiler3(config)
            case TilerVersion.CUSTOM:
                log.trace("Using custom Tiler class: ${tilerClassName}")
                return loadCustomTiler(config)
            case TilerVersion.V4:
                log.trace("Using Tiler version V4")
                return new ImageTiler4(config)
            case TilerVersion.V5:
            default:
                log.trace("Using Tiler version V5")
                return new ImageTiler5(config)
        }
    }

    private IImageTiler loadCustomTiler(ImageTilerConfig config) {
        if (!tilerClassName) {
            throw new IllegalStateException("Tiler version is set to CUSTOM but no tiler class name has been provided")
        }
        try {
            Class tilerClass = this.class.classLoader.loadClass(tilerClassName)
            Constructor constructor = tilerClass.getConstructor(ImageTilerConfig.class)
            return (IImageTiler) constructor.newInstance(config)
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            log.error("Error loading custom tiler class ${tilerClassName}: ${ExceptionUtils.getStackTrace(e)}")
            throw new IllegalStateException("Error loading custom tiler class ${tilerClassName}: ${e.message}", e)
        }
    }

    @Bean("imageTiler")
    @ConditionalOnProperty(name = "images.useStreamingTiler", havingValue = "true")
    IImageTiler streamingImageTiler(CommandExecutor commandExecutor, ImageTilerConfig imageTilerConfig, IImageTiler fallbackTiler) {
        return new DelegatingImageTiler(commandExecutor, imageTilerConfig, fallbackTiler, streamingTool, preferJna)
    }

    @Bean("imageTiler")
    @ConditionalOnProperty(name = "images.useStreamingTiler", havingValue = "false", matchIfMissing = true)
    IImageTiler defaultImageTiler(IImageTiler fallbackTiler) {
        return fallbackTiler
    }

    @Bean("iiifImageProcessor")
    @ConditionalOnProperty(name = "images.useStreamingIiifProcessor", havingValue = "true", matchIfMissing = true)
    IiifImageProcessor iiifImageProcessor() {
        return new DelegatingIiifImageProcessor()
    }

    @Bean("iiifImageProcessor")
    @ConditionalOnProperty(name = "images.useStreamingIiifProcessor", havingValue = "false")
    IiifImageProcessor fallbackIiifImageProcessor() {
        return new JavaIiifImageProcessor()
    }
}