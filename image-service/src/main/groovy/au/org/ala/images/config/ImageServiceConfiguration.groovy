package au.org.ala.images.config

import au.org.ala.images.Application
import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.iiif.JavaIiifImageProcessor
import au.org.ala.images.jna.NativeDzTilerBridgeLoader
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.spring.SimpleAsyncTaskExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTiler
import au.org.ala.images.tiling.ImageTiler3
import au.org.ala.images.tiling.ImageTiler4
import au.org.ala.images.tiling.ImageTiler5
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.OnDemandImageTiler
import au.org.ala.images.tiling.TileFormat
import au.org.ala.images.tiling.TilerVersion
import grails.core.GrailsApplication
import groovy.util.logging.Slf4j
import org.apache.commons.lang3.exception.ExceptionUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.core.task.TaskExecutor

import java.awt.Color
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.util.ServiceLoader
import java.util.concurrent.Executor

/**
 * Holds all {@code @Bean} factory methods that consume {@code @Value}-injected configuration.
 *
 * These beans were moved out of {@link Application} because {@code GrailsAutoConfiguration}
 * invokes {@code @Bean} factory methods before {@code AutowiredAnnotationBeanPostProcessor}
 * populates {@code @Value} fields on the bootstrap instance, causing every {@code @Value} field
 * to silently read its Groovy default (0 / false / ""). Field and method-parameter injection on a regular
 * configuration class is processed by the full Spring annotation pipeline and therefore
 * resolves property placeholders correctly. Grails does not component-scan, so this class is
 * registered via {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}.
 */
@Slf4j
@AutoConfiguration
class ImageServiceConfiguration {

    @Autowired
    GrailsApplication grailsApplication

    @Value('${images.vipsCommand:vips}')
    String vipsCommand

    @Value('${images.magickCommand:magick}')
    String magickCommand

    @Value('${images.nativeDzTiler.jna.enabled:false}')
    boolean nativeDzTilerJnaEnabled

    @Value('${images.nativeDzTiler.ffm.enabled:false}')
    boolean nativeDzTilerFfmEnabled

    // ------------------------------------------------------------------
    // Native bridge path — must be set before any tiler bean constructs
    // ------------------------------------------------------------------

    /**
     * Sets the native dz-tiler bridge library path as a system property so that
     * {@link NativeDzTilerBridgeLoader#getLibrary()} (one-shot lazy read) picks it up
     * before any tiler bean in this configuration is constructed.
     *
     * Because all {@code @Bean} factory methods on this class are instance methods,
     * Spring must instantiate and run {@code @Autowired} injection on this
     * {@code ImageServiceConfiguration} bean before invoking any of its {@code @Bean}
     * methods.
     */
    @Autowired
    void configureNativeBridgeLibraryPath(
            @Value('${images.nativeDzTiler.bridgeLibraryPath:}') String bridgeLibraryPath) {
        if (bridgeLibraryPath) {
            System.setProperty(NativeDzTilerBridgeLoader.BRIDGE_LIB_PATH_PROPERTY, bridgeLibraryPath)
            log.info("Configured native dz tiler bridge library path: {}", bridgeLibraryPath)
        }
    }

    // ------------------------------------------------------------------
    // Executors
    // ------------------------------------------------------------------

    @Bean
    TaskExecutor derivativeLoaderExecutor(
            @Value('${derivative.loader.threads:4}') int threads,
            @Value('${derivative.loader.queueCapacity:100}') int queueCapacity,
            @Value('${derivative.loader.submitTimeoutSeconds:5}') long submitTimeoutSeconds) {
        if (threads < 1) {
            throw new IllegalArgumentException("derivative.loader.threads must be >= 1 but was ${threads}")
        }
        if (queueCapacity < 0) {
            throw new IllegalArgumentException("derivative.loader.queueCapacity must be >= 0 but was ${queueCapacity}")
        }
        // Caller-blocks admission: wait up to submitTimeoutSeconds for a queue slot, then reject (503).
        // Never runs the task on the caller thread.
        return Application.createThreadPoolTaskExecutor(
                "derivative-loader-",
                threads,
                threads,
                queueCapacity,
                new CallerBlocksPolicy("Derivative loader", submitTimeoutSeconds))
    }

    @Bean
    TaskExecutor tilingIoPool(
            @Value('${imageservice.tiling.io.threads:${tiling.ioThreads:2}}') int tilingIoThreads,
            @Value('${imageservice.tiling.io.virtualThreads:${tiling.ioVirtualThreads:true}}') boolean tilingIoVirtualThreads,
            @Value('${imageservice.tiling.io.virtualTaskConcurrencyLimit:${tiling.ioVirtualTaskConcurrencyLimit:0}}') int concurrencyLimit,
            @Value('${imageservice.tiling.io.virtualTaskConcurrencyLimitCloud:${tiling.ioVirtualTaskConcurrencyLimitCloud:512}}') int concurrencyLimitCloud,
            @Value('${imageservice.tiling.io.virtualTaskConcurrencyLimitLocal:${tiling.ioVirtualTaskConcurrencyLimitLocal:32}}') int concurrencyLimitLocal) {
        if (tilingIoVirtualThreads) {
            try {
                SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("tiling-io-")
                executor.setVirtualThreads(true)

                int effectiveLimit = concurrencyLimit
                if (effectiveLimit <= 0) {
                    // Detect cloud storage (S3 or Swift) in configuration
                    def storageConfig = grailsApplication.config.getProperty('imageservice.storage.locations', Map, [:])
                    boolean isLocal = storageConfig.any { k, v ->
                        String type = v?.type?.toString()?.toLowerCase()
                        return type == 'fs' || type == 'filesystem'
                    }
                    effectiveLimit = isLocal ? concurrencyLimitLocal : concurrencyLimitCloud
                    log.info("Auto-detected tiling IO max virtual threads: {} (Cloud storage detected: {})", effectiveLimit, !isLocal)
                }
                executor.setConcurrencyLimit(Math.max(1, effectiveLimit))
                executor.setTaskTerminationTimeout(10_000)
                return executor
            } catch (NoSuchMethodError | Exception e) {
                log.warn("Unable to use virtual threads for tiling IO pool, falling back to regular thread pool. Reason: {}", e.toString())
            }
        }

        // Default and fallback handled here
        return Application.createThreadPoolTaskExecutor("tiling-io-pool-", tilingIoThreads)
    }

    @Bean
    TaskExecutor tilingWorkPool(
            @Value('${imageservice.tiling.level.threads:${tiling.levelThreads:2}}') int tilingLevelThreads) {
        return Application.createThreadPoolTaskExecutor("tiling-work-pool-", tilingLevelThreads)
    }

    // ------------------------------------------------------------------
    // Tiler config
    // ------------------------------------------------------------------

    @Bean
    ImageTilerConfig imageTilerConfig(
            @Qualifier("tilingIoPool") Executor tilingIoPool,
            @Qualifier("tilingWorkPool") Executor tilingWorkPool,
            @Value('${images.padTiles:true}') boolean padTiles) {
        return new ImageTilerConfig(tilingIoPool, tilingWorkPool, Application.TILE_SIZE, 6, TileFormat.JPEG, new Color(221, 221, 221), 0, padTiles)
    }

    // ------------------------------------------------------------------
    // Thumbnailers
    // ------------------------------------------------------------------

    @Bean
    IImageThumbnailer fallbackThumbnailer() {
        return new ImageThumbnailer()
    }

    @Bean("imageThumbnailer")
    @ConditionalOnProperty(name = "images.useStreamingThumbnailer", havingValue = "true")
    IImageThumbnailer streamingImageThumbnailer(
            CommandExecutor commandExecutor,
            @Qualifier("fallbackThumbnailer") IImageThumbnailer fallbackThumbnailer,
            List<ImageLibraryFactory> availableFactories) {
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
    IImageThumbnailer defaultImageThumbnailer(
            @Qualifier("fallbackThumbnailer") IImageThumbnailer fallbackThumbnailer) {
        return fallbackThumbnailer
    }

    // ------------------------------------------------------------------
    // Tilers
    // ------------------------------------------------------------------

    @Bean
    IImageTiler fallbackTiler(
            ImageTilerConfig config,
            @Value('${tiling.tiler.version:V5}') TilerVersion tilerVersion,
            @Value('${tiling.tiler.class:}') String tilerClassName) {
        switch (tilerVersion) {
            case TilerVersion.V1:
                log.trace("Tiler version V1 is deprecated, using V3 instead")
                return new ImageTiler3(config)
            case TilerVersion.V3:
                log.trace("Using Tiler version V3")
                return new ImageTiler3(config)
            case TilerVersion.CUSTOM:
                log.trace("Using custom Tiler class: ${tilerClassName}")
                return loadCustomTiler(config, tilerClassName)
            case TilerVersion.V4:
                log.trace("Using Tiler version V4")
                return new ImageTiler4(config)
            case TilerVersion.V5:
            default:
                log.trace("Using Tiler version V5")
                return new ImageTiler5(config)
        }
    }

    @Bean("imageTiler")
    @ConditionalOnProperty(name = "images.useStreamingTiler", havingValue = "true")
    IImageTiler streamingImageTiler(
            CommandExecutor commandExecutor,
            ImageTilerConfig imageTilerConfig,
            IImageTiler fallbackTiler,
            List<ImageLibraryFactory> availableFactories) {
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
    IOnDemandImageTiler onDemandImageTiler(
            CommandExecutor commandExecutor,
            ImageTilerConfig config,
            List<ImageLibraryFactory> availableFactories) {
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

    // ------------------------------------------------------------------
    // IIIF image processor
    // ------------------------------------------------------------------

    @Bean("iiifImageProcessor")
    @ConditionalOnProperty(name = "images.useStreamingIiifProcessor", havingValue = "true", matchIfMissing = true)
    IiifImageProcessor iiifImageProcessor(
            CommandExecutor commandExecutor,
            List<ImageLibraryFactory> availableFactories) {
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

    // ------------------------------------------------------------------
    // Image library factory discovery
    // ------------------------------------------------------------------

    @Bean
    List<ImageLibraryFactory> availableFactories(
            @Value('${images.preferPureJavaOperations:false}') boolean preferPureJavaOperations) {
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
        return selected
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private Map<String, String> buildLibraryCommandMap() {
        return [
            vips                   : vipsCommand,
            magick                 : magickCommand,
            convert                : magickCommand,
            nativeDzTilerJnaEnabled: Boolean.toString(nativeDzTilerJnaEnabled),
            nativeDzTilerFfmEnabled: Boolean.toString(nativeDzTilerFfmEnabled)
        ]
    }

    private IImageTiler loadCustomTiler(ImageTilerConfig config, String tilerClassName) {
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
}
