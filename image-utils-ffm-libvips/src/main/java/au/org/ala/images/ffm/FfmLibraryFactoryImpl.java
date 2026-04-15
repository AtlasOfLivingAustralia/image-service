package au.org.ala.images.ffm;

import java.util.Map;
import au.org.ala.images.factory.ImageLibraryFactory;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.FfmStreamingImageThumbnailer;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.tiling.FfmOnDemandImageTiler;
import au.org.ala.images.tiling.FfmStreamingImageTiler;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.IOnDemandImageTiler;
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
    public boolean isAvailable(Map<String, String> commands) {
        return NativeLibraryDetectorFFM.isVipsAvailable();
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, Map<String, String> commands, IImageThumbnailer fallbackThumbnailer) {
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            return null;
        }
        return new FfmStreamingImageThumbnailer(fallbackThumbnailer);
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IImageTiler fallbackTiler) {
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            return null;
        }
        return new FfmStreamingImageTiler(fallbackTiler, config);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, Map<String, String> commands, IiifImageProcessor fallback) {
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            return fallback;
        }
        return new FfmIiifImageProcessor(fallback);
    }

    @Override
    public IOnDemandImageTiler createOnDemandTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IOnDemandImageTiler fallback) {
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            return fallback;
        }
        return new FfmOnDemandImageTiler(config, fallback);
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
