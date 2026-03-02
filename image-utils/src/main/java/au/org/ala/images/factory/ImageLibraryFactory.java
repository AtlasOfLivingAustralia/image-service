package au.org.ala.images.factory;

import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import au.org.ala.images.optimisation.CommandExecutor;

/**
 * Common interface for image processing library factories.
 */
public interface ImageLibraryFactory {

    /**
     * Check if the library is available and initialized.
     * @return true if available
     */
    boolean isAvailable();

    /**
     * Create a thumbnailer.
     * @param commandExecutor executor for CLI tools (if needed)
     * @param tool tool name (e.g., 'vips', 'magick')
     * @param fallback fallback implementation
     * @return thumbnailer or null if not supported by this factory
     */
    IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, String tool, IImageThumbnailer fallback);

    /**
     * Create a tiler.
     * @param commandExecutor executor for CLI tools (if needed)
     * @param config tiler configuration
     * @param tool tool name (e.g., 'vips')
     * @param fallback fallback implementation
     * @return tiler or null if not supported by this factory
     */
    IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool, IImageTiler fallback);

    /**
     * Get the priority of this factory. Higher values are preferred.
     * @return priority
     */
    int getPriority();

    /**
     * Get the implementation name.
     * @return implementation name
     */
    String getImplementationName();

    /**
     * Shutdown the library.
     */
    default void shutdown() {}
}
