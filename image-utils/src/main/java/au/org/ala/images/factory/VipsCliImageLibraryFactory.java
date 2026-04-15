package au.org.ala.images.factory;

import java.util.Map;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.StreamingImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.IOnDemandImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import au.org.ala.images.tiling.StreamingImageTiler;
import au.org.ala.images.tiling.VipsCliOnDemandImageTiler;

/**
 * Factory for libvips CLI-based image processing implementations.
 */
public class VipsCliImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable(Map<String, String> commands) {
        String vipsCommand = commands != null ? commands.getOrDefault("vips", "vips") : "vips";
        try {
            Process p = Runtime.getRuntime().exec(new String[]{vipsCommand, "--version"});
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, Map<String, String> commands, IImageThumbnailer fallback) {
        String vipsCommand = commands != null ? commands.getOrDefault("vips", "vips") : "vips";
        if (!commandExecutor.isInstalled(vipsCommand)) {
            return null;
        }
        return new StreamingImageThumbnailer(commandExecutor, vipsCommand);
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IImageTiler fallback) {
        String vipsCommand = commands != null ? commands.getOrDefault("vips", "vips") : "vips";
        if (!commandExecutor.isInstalled(vipsCommand)) {
            return null;
        }
        int tileSize = (config != null) ? config.getTileSize() : 256;
        return new StreamingImageTiler(commandExecutor, vipsCommand, 120, tileSize);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, Map<String, String> commands, IiifImageProcessor fallback) {
        // Vips CLI IIIF not yet implemented in a chained way
        return null;
    }

    @Override
    public IOnDemandImageTiler createOnDemandTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IOnDemandImageTiler fallback) {
        String vipsCommand = commands != null ? commands.getOrDefault("vips", "vips") : "vips";
        if (!commandExecutor.isInstalled(vipsCommand)) {
            return fallback;
        }
        return new VipsCliOnDemandImageTiler(commandExecutor, vipsCommand, config, fallback);
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
