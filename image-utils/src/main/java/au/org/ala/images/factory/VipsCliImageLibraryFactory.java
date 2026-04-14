package au.org.ala.images.factory;

import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.StreamingImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import au.org.ala.images.tiling.StreamingImageTiler;

/**
 * Factory for libvips CLI-based image processing implementations.
 */
public class VipsCliImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable() {
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"vips", "--version"});
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallback) {
        if (!"vips".equals(tool) || !commandExecutor.isInstalled("vips")) {
            return null;
        }
        return new StreamingImageThumbnailer(commandExecutor, "vips");
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallback) {
        if (!"vips".equals(tool) || !commandExecutor.isInstalled("vips")) {
            return null;
        }
        int tileSize = (config != null) ? config.getTileSize() : 256;
        return new StreamingImageTiler(commandExecutor, "vips", 120, tileSize);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, String tool, IiifImageProcessor fallback) {
        // Vips CLI IIIF not yet implemented in a chained way
        return null;
    }

    @Override
    public int getPriority() {
        return 5;
    }

    @Override
    public String getImplementationName() {
        return "libvips CLI";
    }
}
