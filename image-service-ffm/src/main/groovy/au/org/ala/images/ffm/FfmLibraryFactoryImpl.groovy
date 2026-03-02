package au.org.ala.images.ffm

import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.FfmStreamingImageThumbnailer
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.tiling.FfmStreamingImageTiler
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * ServiceLoader implementation of ImageLibraryFactory using Java FFM.
 * This is discovered at runtime when the image-service-ffm module is on the classpath.
 */
@Slf4j
@CompileStatic
class FfmLibraryFactoryImpl implements ImageLibraryFactory {

    FfmLibraryFactoryImpl() {
        log.debug("FfmLibraryFactoryImpl instantiated")
    }

    @Override
    boolean isAvailable() {
        return NativeLibraryDetectorFFM.isVipsAvailable()
    }

    @Override
    IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallbackThumbnailer) {
        if (!isAvailable() || tool != 'vips') {
            return null
        }
        return new FfmStreamingImageThumbnailer(fallbackThumbnailer)
    }

    @Override
    IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallbackTiler) {
        if (!isAvailable() || tool != 'vips') {
            return null
        }
        int tileSize = config?.tileSize ?: 256
        return new FfmStreamingImageTiler(fallbackTiler, tileSize)
    }

    @Override
    int getPriority() {
        return 20 // Prefer over JNA
    }

    @Override
    String getImplementationName() {
        return "FFM/Panama (Java 22+) - ${NativeLibraryDetectorFFM.vipsVersion ?: 'version unknown'}"
    }

    @Override
    void shutdown() {
        NativeLibraryDetectorFFM.shutdown()
    }
}
