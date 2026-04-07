package au.org.ala.images.ffm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.util.Arrays;
import java.util.List;

/**
 * FFM-based utility to detect if native libraries are available on the system.
 * Uses the Foreign Function & Memory API to load and verify native libraries.
 */
public class NativeLibraryDetectorFFM {

    private static final Logger log = LoggerFactory.getLogger(NativeLibraryDetectorFFM.class);

    private static volatile Boolean vipsAvailable = null;
    private static volatile VipsLibraryFFM vipsInstance = null;

    /**
     * Check if libvips is available on the system.
     * This method caches the result for subsequent calls.
     *
     * @return true if libvips can be loaded and initialized
     */
    public static boolean isVipsAvailable() {
        if (vipsAvailable != null) {
            return vipsAvailable;
        }

        synchronized (NativeLibraryDetectorFFM.class) {
            if (vipsAvailable != null) {
                return vipsAvailable;
            }

            VipsLibraryFFM lib = null;
            try {
                // Try to load the library
                lib = loadVipsLibrary();
                if (lib == null) {
                    log.info("libvips not found on system");
                    vipsAvailable = false;
                    return false;
                }

                // Try to initialize
                int result = lib.vipsInit("image-service");
                if (result != 0) {
                    log.warn("libvips found but initialization failed: {}", lib.vipsErrorBuffer());
                    lib.vipsErrorClear();
                    lib.close();
                    vipsAvailable = false;
                    return false;
                }

                // Success! Cache the instance
                vipsInstance = lib;
                vipsAvailable = true;
                log.info("libvips successfully loaded and initialized (FFM)");
                return true;

            } catch (UnsatisfiedLinkError e) {
                log.debug("libvips not available: {}", e.getMessage());
                if (lib != null) lib.close();
                vipsAvailable = false;
                return false;
            } catch (Throwable e) {
                log.warn("Error checking for libvips", e);
                if (lib != null) lib.close();
                vipsAvailable = false;
                return false;
            }
        }
    }

    /**
     * Get the VipsLibraryFFM instance if available.
     * @return VipsLibraryFFM instance or null if not available
     */
    public static VipsLibraryFFM getVipsLibrary() {
        if (isVipsAvailable()) {
            return vipsInstance;
        }
        return null;
    }

    /**
     * Try to load libvips with various common library names using FFM.
     * @return VipsLibraryFFM instance or null
     */
    private static VipsLibraryFFM loadVipsLibrary() {
        // List of possible library names to try
        List<String> vipsLibraryNames = Arrays.asList(
                "vips",           // Standard name
                "libvips",        // With lib prefix
                "vips-42",        // Version 8.x
                "libvips-42",     // Version 8.x with prefix
                "libvips.so.42",  // Specific Linux
                "libvips.42.dylib", // Specific macOS
                "libvips-42.dll"  // Windows
        );

        List<String> gobjectLibraryNames = Arrays.asList(
                "gobject-2.0",
                "libgobject-2.0",
                "libgobject-2.0.so.0",
                "libgobject-2.0.0.dylib",
                "gobject-2.0-0.dll"
        );

        List<String> glibLibraryNames = Arrays.asList(
                "glib-2.0",
                "libglib-2.0",
                "libglib-2.0.so.0",
                "libglib-2.0.0.dylib",
                "glib-2.0-0.dll"
        );

        SymbolLookup vipsLookup = null;
        SymbolLookup gobjectLookup = null;
        SymbolLookup glibLookup = null;

        // Try to load libvips
        for (String name : vipsLibraryNames) {
            try {
                log.debug("Attempting to load libvips as: {}", name);
                vipsLookup = SymbolLookup.libraryLookup(name, Arena.global());
                if (vipsLookup != null) {
                    log.debug("Successfully loaded libvips: {}", name);
                    break;
                }
            } catch (Throwable e) {
                log.trace("Failed to load {}: {}", name, e.getMessage());
            }
        }

        if (vipsLookup == null) {
            log.debug("Could not load libvips with any known name");
            return null;
        }

        // Try to load libgobject
        for (String name : gobjectLibraryNames) {
            try {
                log.debug("Attempting to load libgobject as: {}", name);
                gobjectLookup = SymbolLookup.libraryLookup(name, Arena.global());
                if (gobjectLookup != null) {
                    log.debug("Successfully loaded libgobject: {}", name);
                    break;
                }
            } catch (Throwable e) {
                log.trace("Failed to load {}: {}", name, e.getMessage());
            }
        }

        if (gobjectLookup == null) {
            log.warn("Could not load libgobject - required for VipsSourceCustom");
            return null;
        }

        // Try to load libglib
        for (String name : glibLibraryNames) {
            try {
                log.debug("Attempting to load libglib as: {}", name);
                glibLookup = SymbolLookup.libraryLookup(name, Arena.global());
                if (glibLookup != null) {
                    log.debug("Successfully loaded libglib: {}", name);
                    break;
                }
            } catch (Throwable e) {
                log.trace("Failed to load {}: {}", name, e.getMessage());
            }
        }

        if (glibLookup == null) {
            log.warn("Could not load libglib - required for memory management");
            return null;
        }

        try {
            return new VipsLibraryFFM(vipsLookup, gobjectLookup, glibLookup);
        } catch (Throwable e) {
            log.error("Failed to create VipsLibraryFFM instance", e);
            return null;
        }
    }

    /**
     * Get version information about the loaded library (if available).
     * @return version string or null
     */
    public static String getVipsVersion() {
        if (!isVipsAvailable()) {
            return null;
        }

        return "libvips (FFM-based, version detection not implemented)";
    }

    /**
     * Shutdown the VIPS library if it was initialized.
     * Should be called on application shutdown.
     */
    public static void shutdown() {
        synchronized (NativeLibraryDetectorFFM.class) {
            try {
                if (vipsInstance != null) {
                    vipsInstance.vipsShutdown();
                    vipsInstance.close();
                    log.info("libvips shutdown complete (FFM)");
                }
            } catch (Throwable e) {
                log.warn("Error during libvips shutdown", e);
            } finally {
                vipsInstance = null;
                vipsAvailable = null;
            }
        }
    }
}
