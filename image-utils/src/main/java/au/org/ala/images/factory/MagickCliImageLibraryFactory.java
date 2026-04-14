package au.org.ala.images.factory;

import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.iiif.StreamingIiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.StreamingImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;

/**
 * Factory for ImageMagick CLI-based image processing implementations.
 */
public class MagickCliImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable() {
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"magick", "-version"});
            return p.waitFor() == 0;
        } catch (Exception e) {
            try {
                // Try 'convert' if 'magick' is not present (ImageMagick 6)
                Process p = Runtime.getRuntime().exec(new String[]{"convert", "-version"});
                return p.waitFor() == 0;
            } catch (Exception e2) {
                return false;
            }
        }
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallback) {
        String actualTool = commandExecutor.isInstalled("magick") ? "magick" : (commandExecutor.isInstalled("convert") ? "convert" : null);
        if (actualTool != null && ("magick".equals(tool) || "convert".equals(tool))) {
            return new StreamingImageThumbnailer(commandExecutor, actualTool);
        }
        return null;
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallback) {
        // Magick tiling not efficiently implemented yet in StreamingImageTiler
        return null;
    }

    @Override
    public IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, String tool, IiifImageProcessor fallback) {
        String actualTool = commandExecutor.isInstalled("magick") ? "magick" : (commandExecutor.isInstalled("convert") ? "convert" : null);
        if (actualTool != null && ("magick".equals(tool) || "convert".equals(tool))) {
            return new StreamingIiifImageProcessor(commandExecutor, actualTool);
        }
        return null;
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
