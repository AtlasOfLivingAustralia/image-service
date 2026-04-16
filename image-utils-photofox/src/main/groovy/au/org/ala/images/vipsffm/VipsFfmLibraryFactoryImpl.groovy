package au.org.ala.images.vipsffm

import app.photofox.vipsffm.Vips
import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Alternate ServiceLoader implementation of ImageLibraryFactory using lopcode/vips-ffm library.
 */
@Slf4j
@CompileStatic
class VipsFfmLibraryFactoryImpl implements ImageLibraryFactory {

    private static volatile boolean initialized = false
    private static volatile boolean available = false

    VipsFfmLibraryFactoryImpl() {
        log.debug("VipsFfmLibraryFactoryImpl instantiated")
    }

    @Override
    boolean isAvailable(Map<String, String> commands) {
        if (!initialized) {
            init()
        }
        return available
    }

    private synchronized void init() {
        if (initialized) return
        try {
            // Attempt to initialize lopcode/vips-ffm
            Vips.init()
            available = true
            log.info("VipsFfmLibraryFactoryImpl initialized (lopcode/vips-ffm)")
        } catch (Throwable e) {
            log.warn("VipsFfmLibraryFactoryImpl (lopcode/vips-ffm) not available: {}", e.message)
            available = false
        }
        initialized = true
    }

    @Override
    IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, Map<String, String> commands, IImageThumbnailer fallbackThumbnailer) {
        if (!isAvailable(commands)) {
            return null
        }
        return new VipsFfmStreamingImageThumbnailer(fallbackThumbnailer)
    }

    @Override
    IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IImageTiler fallbackTiler) {
        if (!isAvailable(commands)) {
            return null
        }
        return new VipsFfmStreamingImageTiler(fallbackTiler, config)
    }

    @Override
    IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, Map<String, String> commands, IiifImageProcessor fallback) {
        return null
    }

    @Override
    IOnDemandImageTiler createOnDemandTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IOnDemandImageTiler fallback) {
        if (!isAvailable(commands)) {
            return fallback
        }
        return new VipsFfmOnDemandImageTiler(config, fallback)
    }

    @Override
    int getPriority() {
        return 25 // Prefer over our custom FFM (20) and JNA (10)
    }

    @Override
    String getImplementationName() {
        return "lopcode/vips-ffm (Java 22+)"
    }

    @Override
    void shutdown() {
        if (available) {
            try {
                Vips.shutdown()
                log.info("lopcode/vips-ffm shutdown complete")
            } catch (Throwable e) {
                log.warn("Error during lopcode/vips-ffm shutdown: {}", e.message)
            }
        }
    }
}
