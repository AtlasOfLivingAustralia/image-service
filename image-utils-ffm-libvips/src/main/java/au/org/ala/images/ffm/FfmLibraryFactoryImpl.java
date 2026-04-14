package au.org.ala.images.ffm;

import au.org.ala.images.factory.ImageLibraryFactory;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.FfmStreamingImageThumbnailer;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.tiling.FfmStreamingImageTiler;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ServiceLoader implementation of ImageLibraryFactory using Java FFM.
 * This is discovered at runtime when the image-service-ffm module is on the classpath.
 */
public class FfmLibraryFactoryImpl implements ImageLibraryFactory {

    private static final Logger log = LoggerFactory.getLogger(FfmLibraryFactoryImpl.class);

    public FfmLibraryFactoryImpl() {
        log.debug("FfmLibraryFactoryImpl instantiated");
    }

    @Override
    public boolean isAvailable() {
        return NativeLibraryDetectorFFM.isVipsAvailable();
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallbackThumbnailer) {
        if (!isAvailable() || !"vips".equals(tool)) {
            return null;
        }
        return new FfmStreamingImageThumbnailer(fallbackThumbnailer);
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallbackTiler) {
        if (!isAvailable() || !"vips".equals(tool)) {
            return null;
        }
        int tileSize = (config != null) ? config.getTileSize() : 256;
        if (tileSize <= 0) tileSize = 256;
        return new FfmStreamingImageTiler(fallbackTiler, tileSize);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, String tool, IiifImageProcessor fallback) {
        if (!isAvailable()) {
            return fallback;
        }
        return new FfmIiifImageProcessor(fallback);
    }

    @Override
    public int getPriority() {
        return 20; // Prefer over JNA
    }

    @Override
    public String getImplementationName() {
        return "FFM/Panama (Java 22+) - " + (NativeLibraryDetectorFFM.getVipsVersion() != null ? NativeLibraryDetectorFFM.getVipsVersion() : "version unknown");
    }

    @Override
    public void shutdown() {
        NativeLibraryDetectorFFM.shutdown();
    }
}
