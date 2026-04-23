package au.org.ala.images.vipsffm;

import app.photofox.vipsffm.Vips;
import au.org.ala.images.factory.ImageLibraryFactory;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.IOnDemandImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alternate ServiceLoader implementation of ImageLibraryFactory using lopcode/vips-ffm library.
 */
public class VipsFfmLibraryFactoryImpl implements ImageLibraryFactory {

    private static final Logger log = LoggerFactory.getLogger(VipsFfmLibraryFactoryImpl.class);

    private static volatile boolean initialized = false;
    private static volatile boolean available = false;

    public VipsFfmLibraryFactoryImpl() {
        log.debug("VipsFfmLibraryFactoryImpl instantiated");
    }

    @Override
    public boolean isAvailable(Map<String, String> commands) {
        if (!initialized) {
            init();
        }
        return available;
    }

    private synchronized void init() {
        if (initialized) {
            return;
        }
        try {
            // Attempt to initialize lopcode/vips-ffm.
            Vips.init();
            available = true;
            log.info("VipsFfmLibraryFactoryImpl initialized (lopcode/vips-ffm)");
        } catch (Throwable e) {
            log.warn("VipsFfmLibraryFactoryImpl (lopcode/vips-ffm) not available: {}", e.getMessage());
            available = false;
        }
        initialized = true;
    }

    @Override
    public IImageThumbnailer createThumbnailer(
        CommandExecutor commandExecutor,
        Map<String, String> commands,
        IImageThumbnailer fallbackThumbnailer
    ) {
        if (!isAvailable(commands)) {
            return null;
        }
        return new VipsFfmStreamingImageThumbnailer(fallbackThumbnailer);
    }

    @Override
    public IImageTiler createTiler(
        CommandExecutor commandExecutor,
        ImageTilerConfig config,
        Map<String, String> commands,
        IImageTiler fallbackTiler
    ) {
        if (!isAvailable(commands)) {
            return null;
        }
        return new VipsFfmStreamingImageTiler(fallbackTiler, config);
    }

    @Override
    public IiifImageProcessor createIiifProcessor(
        CommandExecutor commandExecutor,
        Map<String, String> commands,
        IiifImageProcessor fallback
    ) {
        if (!isAvailable(commands)) {
            return fallback;
        }
        return new VipsFfmIiifImageProcessor(fallback);
    }

    @Override
    public IOnDemandImageTiler createOnDemandTiler(
        CommandExecutor commandExecutor,
        ImageTilerConfig config,
        Map<String, String> commands,
        IOnDemandImageTiler fallback
    ) {
        if (!isAvailable(commands)) {
            return fallback;
        }
        return new VipsFfmOnDemandImageTiler(config, fallback);
    }

    @Override
    public int getPriority() {
        return 25; // Prefer over our custom FFM (20) and JNA (10).
    }

    @Override
    public String getImplementationName() {
        return "lopcode/vips-ffm (Java 22+)";
    }

    @Override
    public void shutdown() {
        if (available) {
            try {
                Vips.shutdown();
                log.info("lopcode/vips-ffm shutdown complete");
            } catch (Throwable e) {
                log.warn("Error during lopcode/vips-ffm shutdown: {}", e.getMessage());
            }
        }
    }
}


