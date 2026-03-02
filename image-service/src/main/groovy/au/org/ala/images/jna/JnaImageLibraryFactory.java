package au.org.ala.images.jna;

import au.org.ala.images.factory.ImageLibraryFactory;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.JnaStreamingImageThumbnailer;
import au.org.ala.images.thumb.StreamingImageThumbnailer;
import au.org.ala.images.thumb.ImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import au.org.ala.images.tiling.JnaStreamingImageTiler;
import au.org.ala.images.tiling.StreamingImageTiler;
import au.org.ala.images.tiling.ImageTiler;

/**
 * Factory for JNA-based image processing implementations.
 */
public class JnaImageLibraryFactory implements ImageLibraryFactory {

    @Override
    public boolean isAvailable() {
        return NativeLibraryDetector.isVipsAvailable();
    }

    @Override
    public IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallback) {
        if (!"vips".equals(tool) || !isAvailable()) {
            return null;
        }
        
        IImageThumbnailer cliFallback = commandExecutor.isInstalled(tool) ? 
                new StreamingImageThumbnailer(commandExecutor, tool) : 
                new ImageThumbnailer();
                
        return new JnaStreamingImageThumbnailer(cliFallback);
    }

    @Override
    public IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallback) {
        if (!"vips".equals(tool) || !isAvailable()) {
            return null;
        }
        
        int tileSize = config != null ? config.getTileSize() : 256;
        IImageTiler cliFallback = commandExecutor.isInstalled(tool) ? 
                new StreamingImageTiler(commandExecutor, tool, 120, tileSize) : 
                new ImageTiler(config);
                
        return new JnaStreamingImageTiler(cliFallback, tileSize);
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
