package au.org.ala.images.factory;

import java.util.Map;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.IOnDemandImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;

/**
 * Common interface for image processing library factories.
 */
public interface ImageLibraryFactory {

    /**
     * Check if the library is available and initialized.
     * @param commands tool command paths (e.g., 'vips' -> '/usr/bin/vips')
     * @return true if available
     */
    boolean isAvailable(Map<String, String> commands);

    /**
     * Create a thumbnailer.
     * @param commandExecutor executor for CLI tools (if needed)
     * @param commands tool command paths (e.g., 'vips' -> '/usr/bin/vips')
     * @param fallback fallback implementation
     * @return thumbnailer or null if not supported by this factory
     */
    IImageThumbnailer createThumbnailer(CommandExecutor commandExecutor, Map<String, String> commands, IImageThumbnailer fallback);

    /**
     * Create a tiler.
     * @param commandExecutor executor for CLI tools (if needed)
     * @param config tiler configuration
     * @param commands tool command paths (e.g., 'vips' -> '/usr/bin/vips')
     * @param fallback fallback implementation
     * @return tiler or null if not supported by this factory
     */
    IImageTiler createTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IImageTiler fallback);

    /**
     * Create an IIIF image processor.
     * @param commandExecutor executor for CLI tools (if needed)
     * @param commands tool command paths (e.g., 'vips' -> '/usr/bin/vips')
     * @param fallback fallback implementation
     * @return IIIF processor or null if not supported by this factory
     */
    IiifImageProcessor createIiifProcessor(CommandExecutor commandExecutor, Map<String, String> commands, IiifImageProcessor fallback);

    /**
     * Create an on-demand tiler.
     * @param commandExecutor executor for CLI tools (if needed)
     * @param config tiler configuration
     * @param commands tool command paths (e.g., 'vips' -> '/usr/bin/vips')
     * @param fallback fallback implementation
     * @return on-demand tiler or null if not supported by this factory
     */
    IOnDemandImageTiler createOnDemandTiler(CommandExecutor commandExecutor, ImageTilerConfig config, Map<String, String> commands, IOnDemandImageTiler fallback);

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
