package au.org.ala.images.ffm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Loads the native dz-tiler bridge for FFM calls from either an explicit path or bundled resources.
 */
public final class NativeDzTilerBridgeLoaderFFM {

    private static final Logger log = LoggerFactory.getLogger(NativeDzTilerBridgeLoaderFFM.class);

    public static final String BRIDGE_LIB_NAME = "ala_vips_dztiler";
    public static final String BRIDGE_LIB_PATH_PROPERTY = "images.nativeDzTiler.bridgeLibraryPath";

    private static volatile NativeDzTilerLibraryFFM instance;
    private static volatile boolean loadAttempted = false;

    private NativeDzTilerBridgeLoaderFFM() {
    }

    public static NativeDzTilerLibraryFFM getLibrary() {
        if (loadAttempted) {
            return instance;
        }

        synchronized (NativeDzTilerBridgeLoaderFFM.class) {
            if (loadAttempted) {
                return instance;
            }
            loadAttempted = true;

            String configuredPath = System.getProperty(BRIDGE_LIB_PATH_PROPERTY, "").trim();
            if (!configuredPath.isEmpty()) {
                instance = tryLoad(configuredPath, "configured path");
                if (instance != null) {
                    return instance;
                }
            }

            instance = tryLoad(BRIDGE_LIB_NAME, "system library path");
            if (instance != null) {
                return instance;
            }

            String resourcePath = "/native/" + osArchTag() + "/" + mappedLibraryFileName();
            try (InputStream is = NativeDzTilerBridgeLoaderFFM.class.getResourceAsStream(resourcePath)) {
                if (is == null) {
                    log.debug("No bundled native dz tiler bridge found at {}", resourcePath);
                    return null;
                }

                Path tmp = Files.createTempFile("ala-vips-dztiler-ffm-", "-" + mappedLibraryFileName());
                Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
                tmp.toFile().deleteOnExit();

                instance = tryLoad(tmp.toAbsolutePath().toString(), "bundled resource");
                return instance;
            } catch (IOException e) {
                log.warn("Failed to extract bundled native dz tiler bridge: {}", e.getMessage());
                return null;
            }
        }
    }

    public static boolean isAvailable() {
        return getLibrary() != null;
    }

    private static NativeDzTilerLibraryFFM tryLoad(String libraryReference, String origin) {
        try {
            SymbolLookup lookup = SymbolLookup.libraryLookup(libraryReference, Arena.global());
            NativeDzTilerLibraryFFM loaded = new NativeDzTilerLibraryFFM(lookup);
            log.info("Loaded native dz tiler bridge ({}) from {}", origin, libraryReference);
            return loaded;
        } catch (Throwable t) {
            log.debug("Failed to load native dz tiler bridge ({}) from {}: {}", origin, libraryReference, t.getMessage());
            return null;
        }
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

