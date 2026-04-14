package au.org.ala.images.thumb;

import au.org.ala.images.factory.ImageLibraryFactory;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.iiif.JavaIiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.ImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;

/**
 * Factory for pure Java image processing implementations.
 */
public class JavaImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallback) {
        return new ImageThumbnailer();
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallback) {
        return new ImageTiler(config);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, String tool, IiifImageProcessor fallback) {
        return new JavaIiifImageProcessor();
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
