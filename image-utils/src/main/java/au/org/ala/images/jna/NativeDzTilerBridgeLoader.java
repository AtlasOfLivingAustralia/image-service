package au.org.ala.images.jna;

import com.sun.jna.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Loads the dz-tiler bridge library from either an explicit path or bundled resources.
 */
public final class NativeDzTilerBridgeLoader {

    private static final Logger log = LoggerFactory.getLogger(NativeDzTilerBridgeLoader.class);

    public static final String BRIDGE_LIB_NAME = "ala_vips_dztiler";
    public static final String BRIDGE_LIB_PATH_PROPERTY = "images.nativeDzTiler.bridgeLibraryPath";

    private static volatile NativeDzTilerLibrary INSTANCE;
    private static volatile boolean loadAttempted = false;

    private NativeDzTilerBridgeLoader() {
    }

    public static NativeDzTilerLibrary getLibrary() {
        if (loadAttempted) {
            return INSTANCE;
        }

        synchronized (NativeDzTilerBridgeLoader.class) {
            if (loadAttempted) {
                return INSTANCE;
            }
            loadAttempted = true;

            String configuredPath = System.getProperty(BRIDGE_LIB_PATH_PROPERTY, "").trim();
            if (!configuredPath.isEmpty()) {
                try {
                    INSTANCE = Native.load(configuredPath, NativeDzTilerLibrary.class);
                    log.info("Loaded native dz tiler bridge from configured path: {}", configuredPath);
                    return INSTANCE;
                } catch (UnsatisfiedLinkError e) {
                    log.warn("Failed to load native dz tiler bridge from configured path {}: {}", configuredPath, e.getMessage());
                }
            }

            // Try regular system library lookup first.
            try {
                INSTANCE = Native.load(BRIDGE_LIB_NAME, NativeDzTilerLibrary.class);
                log.info("Loaded native dz tiler bridge from system library path: {}", BRIDGE_LIB_NAME);
                return INSTANCE;
            } catch (UnsatisfiedLinkError e) {
                log.debug("Native dz tiler bridge not found on system library path: {}", e.getMessage());
            }

            // Fall back to bundled resource extraction.
            String resourcePath = "/native/" + osArchTag() + "/" + mappedLibraryFileName();
            try (InputStream is = NativeDzTilerBridgeLoader.class.getResourceAsStream(resourcePath)) {
                if (is == null) {
                    log.debug("No bundled native dz tiler bridge found at {}", resourcePath);
                    return null;
                }

                Path tmp = Files.createTempFile("ala-vips-dztiler-", "-" + mappedLibraryFileName());
                Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
                tmp.toFile().deleteOnExit();
                INSTANCE = Native.load(tmp.toAbsolutePath().toString(), NativeDzTilerLibrary.class);
                log.info("Loaded bundled native dz tiler bridge from {}", resourcePath);
                return INSTANCE;
            } catch (IOException | UnsatisfiedLinkError e) {
                log.warn("Failed to load bundled native dz tiler bridge: {}", e.getMessage());
                return null;
            }
        }
    }

    public static boolean isAvailable() {
        return getLibrary() != null;
    }

    private static String mappedLibraryFileName() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac") || os.contains("darwin")) {
            return "lib" + BRIDGE_LIB_NAME + ".dylib";
        }
        if (os.contains("win")) {
            return BRIDGE_LIB_NAME + ".dll";
        }
        return "lib" + BRIDGE_LIB_NAME + ".so";
    }

    private static String osArchTag() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();

        String osTag;
        if (os.contains("mac") || os.contains("darwin")) {
            osTag = "macos";
        } else if (os.contains("linux")) {
            osTag = "linux";
        } else if (os.contains("win")) {
            osTag = "windows";
        } else {
            osTag = os.replaceAll("[^a-z0-9]", "");
        }

        String archTag;
        if ("x86_64".equals(arch) || "amd64".equals(arch)) {
            archTag = "x86_64";
        } else if ("aarch64".equals(arch) || "arm64".equals(arch)) {
            archTag = "aarch64";
        } else {
            archTag = arch.replaceAll("[^a-z0-9_]", "");
        }

        return osTag + "-" + archTag;
    }
}

