package au.org.ala.images.jna

import com.sun.jna.Native
import com.sun.jna.NativeLibrary
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Utility to detect if native libraries are available on the system.
 * Attempts to load libraries and verify they can be initialized.
 */
@Slf4j
@CompileStatic
class NativeLibraryDetector {

    private static volatile Boolean vipsAvailable = null
    private static volatile VipsLibrary vipsInstance = null
    private static volatile NativeLibrary vipsNativeLibrary = null

    /**
     * Check if libvips is available on the system.
     * This method caches the result for subsequent calls.
     *
     * @return true if libvips can be loaded and initialized
     */
    static boolean isVipsAvailable() {
        if (vipsAvailable != null) {
            return vipsAvailable
        }

        synchronized (NativeLibraryDetector.class) {
            if (vipsAvailable != null) {
                return vipsAvailable
            }

            try {
                // Try to load the library
                // Common library names: vips, libvips, vips-42 (version-specific)
                VipsLibrary lib = loadVipsLibrary()
                if (lib == null) {
                    log.info("libvips not found on system")
                    vipsAvailable = false
                    return false
                }

                // Try to initialize
                int result = lib.vips_init("image-service")
                if (result != 0) {
                    log.warn("libvips found but initialization failed: {}", lib.vips_error_buffer())
                    lib.vips_error_clear()
                    vipsAvailable = false
                    return false
                }

                // Success! Cache the instance
                vipsInstance = lib
                vipsAvailable = true
                log.info("libvips successfully loaded and initialized")
                return true

            } catch (UnsatisfiedLinkError e) {
                log.debug("libvips not available: {}", e.message)
                vipsAvailable = false
                return false
            } catch (Exception e) {
                log.warn("Error checking for libvips", e)
                vipsAvailable = false
                return false
            }
        }
    }

    /**
     * Get the VipsLibrary instance if available.
     * @return VipsLibrary instance or null if not available
     */
    static VipsLibrary getVipsLibrary() {
        if (isVipsAvailable()) {
            return vipsInstance
        }
        return null
    }

    /**
     * Try to load libvips with various common library names.
     * @return VipsLibrary instance or null
     */
    private static VipsLibrary loadVipsLibrary() {
        // List of possible library names to try
        List<String> libraryNames = [
                "vips",           // Standard name
                "libvips",        // With lib prefix
                "vips-42",        // Version 8.x
                "libvips-42",     // Version 8.x with prefix
                "libvips.so.42",  // Specific Linux
                "libvips.42.dylib", // Specific macOS
        ]

        for (String name : libraryNames) {
            try {
                log.debug("Attempting to load libvips as: {}", name)
                VipsLibrary lib = Native.load(name, VipsLibrary.class) as VipsLibrary
                if (lib != null) {
                    log.debug("Successfully loaded: {}", name)
                    vipsNativeLibrary = NativeLibrary.getInstance(name)
                    return lib
                }
            } catch (UnsatisfiedLinkError e) {
                log.trace("Failed to load {}: {}", name, e.message)
            }
        }

        return null
    }

    /**
     * Get version information about the loaded library (if available).
     * @return version string or null
     */
    static String getVipsVersion() {
        if (!isVipsAvailable()) {
            return null
        }

        if (vipsNativeLibrary != null) {
            return "libvips (path: ${vipsNativeLibrary.file?.absolutePath})"
        }

        return "libvips (version unknown)"
    }

    /**
     * Shutdown the VIPS library if it was initialized.
     * Should be called on application shutdown.
     */
    static void shutdown() {
        try {
            if (vipsInstance != null) {
                vipsInstance.vips_shutdown()
                log.info("libvips shutdown complete")
            }
        } catch (Exception e) {
            log.warn("Error during libvips shutdown", e)
        } finally {
            vipsInstance = null
            vipsNativeLibrary = null
            vipsAvailable = false // if vips_shutdown was called then we shouldn't try to reinitialize it again
        }
    }
}
