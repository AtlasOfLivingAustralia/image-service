package au.org.ala.images.vipsffm

import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
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
    boolean isAvailable() {
        if (!initialized) {
            init()
        }
        return available
    }

    private synchronized void init() {
        if (initialized) return
        try {
            // Attempt to initialize lopcode/vips-ffm
            // Based on library intent, it has a Vips class
            int result = vips.ffm.Vips.vips_init("image-service")
            available = (result == 0)
            if (available) {
                log.info("VipsFfmLibraryFactoryImpl initialized (lopcode/vips-ffm)")
            } else {
                log.warn("lopcode/vips-ffm vips_init failed")
            }
        } catch (Throwable e) {
            log.warn("VipsFfmLibraryFactoryImpl (lopcode/vips-ffm) not available: {}", e.message)
            available = false
        }
        initialized = true
    }

    @Override
    IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallbackThumbnailer) {
        if (!isAvailable() || tool != 'vips') {
            return null
        }
        return new VipsFfmStreamingImageThumbnailer(fallbackThumbnailer)
    }

    @Override
    IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallbackTiler) {
        if (!isAvailable() || tool != 'vips') {
            return null
        }
        int tileSize = config?.tileSize ?: 256
        return new VipsFfmStreamingImageTiler(fallbackTiler, tileSize)
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
                vips.ffm.Vips.vips_shutdown()
                log.info("lopcode/vips-ffm shutdown complete")
            } catch (Throwable e) {
                log.warn("Error during lopcode/vips-ffm shutdown: {}", e.message)
            }
        }
    }
}
