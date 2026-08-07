package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.iiif.JavaIiifImageProcessor
import au.org.ala.images.jna.NativeDzTilerBridgeLoader
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ProcessCommandExecutor
import au.org.ala.images.spring.SimpleAsyncTaskExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.OnDemandImageTiler
import au.org.ala.images.tiling.ImageTiler
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

import javax.annotation.PostConstruct
import javax.imageio.ImageIO
import java.awt.Color
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.util.ServiceLoader
import java.util.concurrent.Executor
import java.util.concurrent.ThreadPoolExecutor

//@EnableConfigurationProperties(ImageOptimisationConfig)
@Slf4j
class Application extends GrailsAutoConfiguration {

    public static final int TILE_SIZE = 256

    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

//    @Autowired
//    ImageOptimisationConfig imageOptimisationConfig

    @Value('${images.vipsCommand:vips}')
    String vipsCommand

    @Value('${images.magickCommand:magick}')
    String magickCommand

    @Value('${images.preferPureJavaOperations:false}')
    boolean preferPureJavaOperations

    @Value('${images.nativeDzTiler.jna.enabled:false}')
    boolean nativeDzTilerJnaEnabled

    @Value('${images.nativeDzTiler.ffm.enabled:false}')
    boolean nativeDzTilerFfmEnabled

    @Value('${images.nativeDzTiler.bridgeLibraryPath:}')
    String nativeDzTilerBridgeLibraryPath

    @Value('${images.padTiles:true}')
    boolean padTiles

    @Value('${tiling.tiler.version:V5}')
    TilerVersion tilerVersion

    @Value('${tiling.tiler.class:}')
    String tilerClassName

    @Value('${imageservice.tiling.io.threads:${tiling.ioThreads:2}}')
    int tilingIoThreads

    @Value('${imageservice.tiling.level.threads:${tiling.levelThreads:2}}')
    int tilingLevelThreads

    @Value('${imageservice.tiling.io.virtualThreads:${tiling.ioVirtualThreads:true}}')
    boolean tilingIoVirtualThreads

    @Value('${imageservice.tiling.io.virtualTaskConcurrencyLimit:${tiling.ioVirtualTaskConcurrencyLimit:0}}')
    int tilingIoVirtualTaskConcurrencyLimit

    @Value('${imageservice.tiling.io.virtualTaskConcurrencyLimitCloud:${tiling.ioVirtualTaskConcurrencyLimitCloud:512}}')
    int tilingIoVirtualTaskConcurrencyLimitCloud

    @Value('${imageservice.tiling.io.virtualTaskConcurrencyLimitLocal:${tiling.ioVirtualTaskConcurrencyLimitLocal:32}}')
    int tilingIoVirtualTaskConcurrencyLimitLocal

    @Value('${derivative.loader.threads:4}')
    int derivativeLoaderThreads

    @Value('${derivative.loader.queueCapacity:100}')
    int derivativeLoaderQueueCapacity

    @Bean
    TaskExecutor analyticsExecutor() {
        return createThreadPoolTaskExecutor("analytics-", 1)
    }

    @Bean
    TaskExecutor storageLocationExecutor() {
        return createThreadPoolTaskExecutor("storage-", 1)
    }

    @Bean
    TaskExecutor derivativeLoaderExecutor() {
        ThreadPoolTaskExecutor executor = createThreadPoolTaskExecutor("derivative-loader-", derivativeLoaderThreads, derivativeLoaderThreads, Math.max(0, derivativeLoaderQueueCapacity)) as ThreadPoolTaskExecutor
        // Derivative requests are user-facing. When the bounded executor is full, apply
        // backpressure on the request thread instead of rejecting every subsequent image.
        executor.rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy()
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
    IImageThumbnailer streamingImageThumbnailer(CommandExecutor commandExecutor, @Qualifier("fallbackThumbnailer") IImageThumbnailer fallbackThumbnailer, List<ImageLibraryFactory> availableFactories) {
        IImageThumbnailer current = fallbackThumbnailer
        Map<String, String> commands = buildLibraryCommandMap()
        // Build up from lowest priority
        for (ImageLibraryFactory factory : availableFactories.reverse()) {
            IImageThumbnailer thumb = factory.createThumbnailer(commandExecutor, commands, current)
            if (thumb != null) {
                current = thumb
            }
        }
        return current
    }

    @Bean("imageThumbnailer")
    @ConditionalOnProperty(name = "images.useStreamingThumbnailer", havingValue = "false", matchIfMissing = true)
    IImageThumbnailer defaultImageThumbnailer(@Qualifier("fallbackThumbnailer") IImageThumbnailer fallbackThumbnailer) {
        return fallbackThumbnailer
    }

    @Bean
    TaskExecutor tilingIoPool() {
        if (tilingIoVirtualThreads) {
            try {
                SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("tiling-io-")
                executor.setVirtualThreads(true)

                int concurrencyLimit = tilingIoVirtualTaskConcurrencyLimit
                if (concurrencyLimit <= 0) {
                    // Detect cloud storage (S3 or Swift) in configuration
                    def storageConfig = grailsApplication.config.getProperty('imageservice.storage.locations', Map, [:])
                    boolean isLocal = storageConfig.any { k, v ->
                        String type = v?.type?.toString()?.toLowerCase()
                        return type == 'fs' || type == 'filesystem'
                    }
                    concurrencyLimit = isLocal ? tilingIoVirtualTaskConcurrencyLimitLocal : tilingIoVirtualTaskConcurrencyLimitCloud
                    log.info("Auto-detected tiling IO max virtual threads: {} (Cloud storage detected: {})", concurrencyLimit, !isLocal)
                }
                executor.setConcurrencyLimit(Math.max(1, concurrencyLimit))
                executor.setTaskTerminationTimeout(10_000)
                return executor
            } catch (NoSuchMethodError | Exception e) {
                log.warn("Unable to use virtual threads for tiling IO pool, falling back to regular thread pool. Reason: {}", e.toString())
            }
        }

        // Default and fallback handled here
        return createThreadPoolTaskExecutor("tiling-io-pool-", tilingIoThreads)
    }

    @Bean
    TaskExecutor tilingWorkPool() {
        return createThreadPoolTaskExecutor("tiling-work-pool-", tilingLevelThreads)
    }

    @Bean
    ImageTilerConfig imageTilerConfig(@Qualifier("tilingIoPool") Executor tilingIoPool, @Qualifier("tilingWorkPool") Executor tilingWorkPool) {
        return new ImageTilerConfig(tilingIoPool, tilingWorkPool, TILE_SIZE, 6, TileFormat.JPEG, new Color(221, 221, 221), 0, padTiles)
    }

    @PostConstruct
    void init() {
        log.info("Initialising ImageIO settings...")
        ImageIO.scanForPlugins()
        ImageIO.setUseCache(false)

        if (nativeDzTilerBridgeLibraryPath) {
            System.setProperty(NativeDzTilerBridgeLoader.BRIDGE_LIB_PATH_PROPERTY, nativeDzTilerBridgeLibraryPath)
            log.info("Configured native dz tiler bridge library path: {}", nativeDzTilerBridgeLibraryPath)
        }
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
    IImageTiler streamingImageTiler(CommandExecutor commandExecutor, ImageTilerConfig imageTilerConfig, IImageTiler fallbackTiler, List<ImageLibraryFactory> availableFactories) {
        IImageTiler current = fallbackTiler
        Map<String, String> commands = buildLibraryCommandMap()
        for (ImageLibraryFactory factory : availableFactories.reverse()) {
            IImageTiler tiler = factory.createTiler(commandExecutor, imageTilerConfig, commands, current)
            if (tiler != null) {
                current = tiler
            }
        }
        return current
    }

    @Bean("imageTiler")
    @ConditionalOnProperty(name = "images.useStreamingTiler", havingValue = "false", matchIfMissing = true)
    IImageTiler defaultImageTiler(IImageTiler fallbackTiler) {
        return fallbackTiler
    }

    @Bean("onDemandImageTiler")
    IOnDemandImageTiler onDemandImageTiler(CommandExecutor commandExecutor, ImageTilerConfig config, List<ImageLibraryFactory> availableFactories) {
        IOnDemandImageTiler current = new OnDemandImageTiler(config)
        Map<String, String> commands = buildLibraryCommandMap()
        for (ImageLibraryFactory factory : availableFactories.reverse()) {
            IOnDemandImageTiler tiler = factory.createOnDemandTiler(commandExecutor, config, commands, current)
            if (tiler != null) {
                current = tiler
            }
        }
        return current
    }

    @Bean("iiifImageProcessor")
    @ConditionalOnProperty(name = "images.useStreamingIiifProcessor", havingValue = "true", matchIfMissing = true)
    IiifImageProcessor iiifImageProcessor(CommandExecutor commandExecutor, List<ImageLibraryFactory> availableFactories) {
        IiifImageProcessor current = new JavaIiifImageProcessor()
        Map<String, String> commands = buildLibraryCommandMap()
        for (ImageLibraryFactory factory : availableFactories.reverse()) {
            IiifImageProcessor proc = factory.createIiifProcessor(commandExecutor, commands, current)
            if (proc != null) {
                current = proc
            }
        }
        return current
    }

    @Bean("iiifImageProcessor")
    @ConditionalOnProperty(name = "images.useStreamingIiifProcessor", havingValue = "false")
    IiifImageProcessor fallbackIiifImageProcessor() {
        return new JavaIiifImageProcessor()
    }

    @Bean
    List<ImageLibraryFactory> availableFactories() {
        ServiceLoader<ImageLibraryFactory> loader = ServiceLoader.load(ImageLibraryFactory)
        Map<String, String> commands = buildLibraryCommandMap()
        List<ImageLibraryFactory> factories = loader.toList().findAll { it.isAvailable(commands) }.sort { -it.priority }
        if (preferPureJavaOperations) {
            factories = factories.findAll { it.priority == 0 }
        }
        return factories
    }

    @Bean
    ImageLibraryFactory imageLibraryFactory(List<ImageLibraryFactory> availableFactories) {
        if (!availableFactories) {
            log.warn("No suitable ImageLibraryFactory found!")
            throw new IllegalStateException("No suitable ImageLibraryFactory found")
        }
        ImageLibraryFactory selected = availableFactories.head()
        log.info("Primary ImageLibraryFactory: {} (priority: {})", selected.implementationName, selected.priority)
    }

    private static TaskExecutor createThreadPoolTaskExecutor(String namePrefix, int coreSize, int maxSize = coreSize, int queueCapacity = -1) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor()
        executor.corePoolSize = Math.max(1, coreSize)
        executor.maxPoolSize = Math.max(1, maxSize)
        if (queueCapacity >= 0) {
            executor.queueCapacity = queueCapacity
        }
        executor.threadNamePrefix = namePrefix
        executor.waitForTasksToCompleteOnShutdown = true
        executor.awaitTerminationSeconds = 10
        return executor
    }

    private Map<String, String> buildLibraryCommandMap() {
        return [
            vips                  : vipsCommand,
            magick                : magickCommand,
            convert               : magickCommand,
            nativeDzTilerJnaEnabled: Boolean.toString(nativeDzTilerJnaEnabled),
            nativeDzTilerFfmEnabled: Boolean.toString(nativeDzTilerFfmEnabled)
        ]
    }
}
