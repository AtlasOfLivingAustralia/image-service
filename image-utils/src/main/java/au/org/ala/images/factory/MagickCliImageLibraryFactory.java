package au.org.ala.images.factory;

import java.util.Map;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.iiif.StreamingIiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.StreamingImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.IOnDemandImageTiler;
import au.org.ala.images.tiling.MagickCliOnDemandImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;

/**
 * Factory for ImageMagick CLI-based image processing implementations.
 */
public class MagickCliImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable(Map<String, String> commands) {
        String magickCommand = commands != null ? commands.getOrDefault("magick", "magick") : "magick";
        try {
            Process p = Runtime.getRuntime().exec(new String[]{magickCommand, "-version"});
            return p.waitFor() == 0;
        } catch (Exception e) {
            try {
                // Try 'convert' if 'magick' is not present (ImageMagick 6)
                String convertCommand = commands != null ? commands.getOrDefault("convert", "convert") : "convert";
                Process p = Runtime.getRuntime().exec(new String[]{convertCommand, "-version"});
                return p.waitFor() == 0;
            } catch (Exception e2) {
                return false;
            }
        }
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, Map<String, String> commands, IImageThumbnailer fallback) {
        String magickCommand = commands != null ? commands.getOrDefault("magick", "magick") : "magick";
        String convertCommand = commands != null ? commands.getOrDefault("convert", "convert") : "convert";
        
        String actualTool = commandExecutor.isInstalled(magickCommand) ? magickCommand : (commandExecutor.isInstalled(convertCommand) ? convertCommand : null);
        if (actualTool != null) {
            return new StreamingImageThumbnailer(commandExecutor, actualTool);
        }
        return null;
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IImageTiler fallback) {
        // Magick tiling not efficiently implemented yet in StreamingImageTiler
        return null;
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, Map<String, String> commands, IiifImageProcessor fallback) {
        String magickCommand = commands != null ? commands.getOrDefault("magick", "magick") : "magick";
        String convertCommand = commands != null ? commands.getOrDefault("convert", "convert") : "convert";

        String actualTool = commandExecutor.isInstalled(magickCommand) ? magickCommand : (commandExecutor.isInstalled(convertCommand) ? convertCommand : null);
        if (actualTool != null) {
            return new StreamingIiifImageProcessor(commandExecutor, actualTool);
        }
        return null;
    }

    @Override
    public IOnDemandImageTiler createOnDemandTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IOnDemandImageTiler fallback) {
        String magickCommand = commands != null ? (commands.containsKey("magick") ? commands.get("magick") : (commands.containsKey("convert") ? commands.get("convert") : "magick")) : "magick";
        if (!commandExecutor.isInstalled(magickCommand)) {
            return fallback;
        }
        return new MagickCliOnDemandImageTiler(commandExecutor, magickCommand, config, fallback);
    }

    @Override
    public int getPriority() {
        return 4;
    }

    @Override
    public String getImplementationName() {
        return "ImageMagick CLI";
    }
}
