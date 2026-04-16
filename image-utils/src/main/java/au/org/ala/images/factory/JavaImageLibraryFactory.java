package au.org.ala.images.factory;

import java.util.Map;
import au.org.ala.images.factory.ImageLibraryFactory;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.iiif.JavaIiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.ImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.IOnDemandImageTiler;
import au.org.ala.images.tiling.ImageTiler;
import au.org.ala.images.tiling.OnDemandImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;

/**
 * Factory for pure Java image processing implementations.
 */
public class JavaImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable(Map<String, String> commands) {
        return true;
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, Map<String, String> commands, IImageThumbnailer fallback) {
        return new ImageThumbnailer();
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IImageTiler fallback) {
        return new ImageTiler(config);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, Map<String, String> commands, IiifImageProcessor fallback) {
        return new JavaIiifImageProcessor();
    }

    @Override
    public IOnDemandImageTiler createOnDemandTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IOnDemandImageTiler fallback) {
        return new OnDemandImageTiler(config);
    }

    @Override
    public int getPriority() {
        return 0; // Lowest priority
    }

    @Override
    public String getImplementationName() {
        return "Pure Java (ImageIO/imgscalr)";
    }
}
