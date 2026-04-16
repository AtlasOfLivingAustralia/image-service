package au.org.ala.images.factory;

import java.util.Map;
import au.org.ala.images.factory.ImageLibraryFactory;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.iiif.JnaIiifImageProcessor;
import au.org.ala.images.jna.NativeLibraryDetector;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.JnaStreamingImageThumbnailer;
import au.org.ala.images.thumb.StreamingImageThumbnailer;
import au.org.ala.images.thumb.ImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.IOnDemandImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import au.org.ala.images.tiling.JnaOnDemandImageTiler;
import au.org.ala.images.tiling.JnaStreamingImageTiler;
import au.org.ala.images.tiling.StreamingImageTiler;
import au.org.ala.images.tiling.ImageTiler;

/**
 * Factory for JNA-based image processing implementations.
 */
public class JnaImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable(Map<String, String> commands) {
        return NativeLibraryDetector.isVipsAvailable();
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, Map<String, String> commands, IImageThumbnailer fallback) {
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return null;
        }
        
        return new JnaStreamingImageThumbnailer(fallback);
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IImageTiler fallback) {
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return null;
        }
        
        return new JnaStreamingImageTiler(fallback, config);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, Map<String, String> commands, IiifImageProcessor fallback) {
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return fallback;
        }
        return new JnaIiifImageProcessor(fallback);
    }

    @Override
    public IOnDemandImageTiler createOnDemandTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IOnDemandImageTiler fallback) {
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return fallback;
        }
        return new JnaOnDemandImageTiler(config, fallback);
    }

    @Override
    public int getPriority() {
        return 10;
    }

    @Override
    public String getImplementationName() {
        return "JNA (libvips) - " + NativeLibraryDetector.getVipsVersion();
    }

    @Override
    public void shutdown() {
        NativeLibraryDetector.shutdown();
    }
}
