package au.org.ala.images.ffm

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.foreign.*
import java.lang.invoke.MethodHandle

/**
 * FFM (Foreign Function & Memory API) interface to libvips C library.
 * This replaces the JNA-based interface with the modern Java 22+ Panama FFM API.
 *
 * libvips documentation: https://www.libvips.org/API/current/
 */
@Slf4j
@CompileStatic
class VipsLibraryFFM {

    private final SymbolLookup libvipsLookup
    private final SymbolLookup libgobjectLookup
    private final SymbolLookup libglibLookup
    private final Linker linker
    private final Arena arena

    // Method handles for libvips functions
    private final MethodHandle vips_init
    private final MethodHandle vips_shutdown
    private final MethodHandle vips_error_buffer
    private final MethodHandle vips_error_clear
    private final MethodHandle vips_image_new_from_buffer
    private final MethodHandle vips_image_new_from_source
    private final MethodHandle vips_image_get_width
    private final MethodHandle vips_image_get_height
    private final MethodHandle vips_thumbnail_image
    private final MethodHandle vips_image_write_to_buffer
    private final MethodHandle vips_dzsave
    private final MethodHandle vips_source_custom_new
    private final MethodHandle g_object_unref
    private final MethodHandle g_free
    private final MethodHandle g_signal_connect_data

    // Function descriptors for libvips API
    private static final FunctionDescriptor FD_vips_init = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_shutdown = FunctionDescriptor.ofVoid()
    private static final FunctionDescriptor FD_vips_error_buffer = FunctionDescriptor.of(ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_error_clear = FunctionDescriptor.ofVoid()
    private static final FunctionDescriptor FD_vips_image_new_from_buffer = FunctionDescriptor.of(
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_image_new_from_source = FunctionDescriptor.of(
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_image_get_width = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_image_get_height = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_thumbnail_image = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_image_write_to_buffer = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_dzsave = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_vips_source_custom_new = FunctionDescriptor.of(ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_g_object_unref = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_g_free = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
    private static final FunctionDescriptor FD_g_signal_connect_data = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)

    VipsLibraryFFM(SymbolLookup libvipsLookup, SymbolLookup libgobjectLookup, SymbolLookup libglibLookup) {
        this.libvipsLookup = libvipsLookup
        this.libgobjectLookup = libgobjectLookup
        this.libglibLookup = libglibLookup
        this.linker = Linker.nativeLinker()
        this.arena = Arena.ofShared()

        // Look up and link function symbols
        this.vips_init = lookupFunction(libvipsLookup, "vips_init", FD_vips_init)
        this.vips_shutdown = lookupFunction(libvipsLookup, "vips_shutdown", FD_vips_shutdown)
        this.vips_error_buffer = lookupFunction(libvipsLookup, "vips_error_buffer", FD_vips_error_buffer)
        this.vips_error_clear = lookupFunction(libvipsLookup, "vips_error_clear", FD_vips_error_clear)
        this.vips_image_new_from_buffer = lookupFunction(libvipsLookup, "vips_image_new_from_buffer", FD_vips_image_new_from_buffer)
        this.vips_image_new_from_source = lookupFunction(libvipsLookup, "vips_image_new_from_source", FD_vips_image_new_from_source)
        this.vips_image_get_width = lookupFunction(libvipsLookup, "vips_image_get_width", FD_vips_image_get_width)
        this.vips_image_get_height = lookupFunction(libvipsLookup, "vips_image_get_height", FD_vips_image_get_height)
        this.vips_thumbnail_image = lookupFunction(libvipsLookup, "vips_thumbnail_image", FD_vips_thumbnail_image)
        this.vips_image_write_to_buffer = lookupFunction(libvipsLookup, "vips_image_write_to_buffer", FD_vips_image_write_to_buffer)
        this.vips_dzsave = lookupFunction(libvipsLookup, "vips_dzsave", FD_vips_dzsave)
        this.vips_source_custom_new = lookupFunction(libvipsLookup, "vips_source_custom_new", FD_vips_source_custom_new)
        this.g_object_unref = lookupFunction(libgobjectLookup, "g_object_unref", FD_g_object_unref)
        this.g_free = lookupFunction(libglibLookup, "g_free", FD_g_free)
        this.g_signal_connect_data = lookupFunction(libgobjectLookup, "g_signal_connect_data", FD_g_signal_connect_data)
    }

    private MethodHandle lookupFunction(SymbolLookup lookup, String name, FunctionDescriptor descriptor) {
        MemorySegment symbol = lookup.find(name).orElseThrow {
            new UnsatisfiedLinkError("Unable to find symbol: ${name}")
        }
        return linker.downcallHandle(symbol, descriptor)
    }

    /**
     * Initialize the VIPS library. Must be called before using any other VIPS functions.
     * @param argv0 program name (can be null)
     * @return 0 on success
     */
    int vipsInit(String argv0) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment nameSegment = argv0 ? tempArena.allocateFrom(argv0) : MemorySegment.NULL
            return (int) vips_init.invoke(nameSegment)
        }
    }

    /**
     * Shutdown the VIPS library and free resources.
     */
    void vipsShutdown() throws Throwable {
        vips_shutdown.invoke()
    }

    /**
     * Get the last VIPS error message.
     * @return error message string
     */
    String vipsErrorBuffer() throws Throwable {
        MemorySegment errorPtr = (MemorySegment) vips_error_buffer.invoke()
        if (errorPtr == null || errorPtr.address() == 0) {
            return ""
        }
        return errorPtr.getString(0)
    }

    /**
     * Clear the VIPS error buffer.
     */
    void vipsErrorClear() throws Throwable {
        vips_error_clear.invoke()
    }

    /**
     * Load an image from a buffer in memory.
     * @param buf memory segment containing image data
     * @param len length of image data
     * @param options options string (can be null)
     * @return VipsImage memory segment or null on error
     */
    MemorySegment vipsImageNewFromBuffer(MemorySegment buf, long len, String options) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment optionsSegment = options ? tempArena.allocateFrom(options) : MemorySegment.NULL
            return (MemorySegment) vips_image_new_from_buffer.invoke(buf, len, optionsSegment)
        }
    }

    /**
     * Load an image from a VipsSource.
     * @param source the source to read from
     * @param options options string (can be empty)
     * @return VipsImage memory segment or null on error
     */
    MemorySegment vipsImageNewFromSource(MemorySegment source, String options) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment optionsSegment = options ? tempArena.allocateFrom(options) : tempArena.allocateFrom("")
            return (MemorySegment) vips_image_new_from_source.invoke(source, optionsSegment, MemorySegment.NULL)
        }
    }

    /**
     * Get image width.
     * @param image the image
     * @return width in pixels
     */
    int vipsImageGetWidth(MemorySegment image) throws Throwable {
        return (int) vips_image_get_width.invoke(image)
    }

    /**
     * Get image height.
     * @param image the image
     * @return height in pixels
     */
    int vipsImageGetHeight(MemorySegment image) throws Throwable {
        return (int) vips_image_get_height.invoke(image)
    }

    /**
     * Thumbnail an image. This is a high-level operation that will
     * shrink or expand an image to fit within a bounding box.
     * Note: This simplified version only supports basic width parameter.
     * For advanced options (height, crop, etc.), you'd need to use varargs support.
     *
     * @param input input image
     * @param outPtr pointer to receive output image (must be allocated)
     * @param width target width
     * @return 0 on success
     */
    int vipsThumbnailImage(MemorySegment input, MemorySegment outPtr, int width) throws Throwable {
        return (int) vips_thumbnail_image.invoke(input, outPtr, width, MemorySegment.NULL)
    }

    /**
     * Save an image to a buffer.
     * @param image the image to save
     * @param bufPtr pointer to receive buffer pointer
     * @param lenPtr pointer to receive buffer length
     * @param suffix file format suffix (e.g., ".jpg", ".png")
     * @return 0 on success
     */
    int vipsImageWriteToBuffer(MemorySegment image, MemorySegment bufPtr, MemorySegment lenPtr, String suffix) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment suffixSegment = tempArena.allocateFrom(suffix)
            return (int) vips_image_write_to_buffer.invoke(image, bufPtr, lenPtr, suffixSegment, MemorySegment.NULL)
        }
    }

    /**
     * Create Deep Zoom pyramid tiles from an image.
     * Note: This simplified version only supports basic output path parameter.
     * For advanced options (tile-size, overlap, etc.), you'd need to use varargs support.
     *
     * @param input input image
     * @param outputPath output directory path
     * @return 0 on success
     */
    int vipsDzsave(MemorySegment input, String outputPath) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment pathSegment = tempArena.allocateFrom(outputPath)
            return (int) vips_dzsave.invoke(input, pathSegment, MemorySegment.NULL)
        }
    }

    /**
     * Create a new VipsSourceCustom.
     * @return new VipsSourceCustom memory segment
     */
    MemorySegment vipsSourceCustomNew() throws Throwable {
        return (MemorySegment) vips_source_custom_new.invoke()
    }

    /**
     * Free a VipsImage or other GObject.
     * @param object the object to free
     */
    void gObjectUnref(MemorySegment object) throws Throwable {
        if (object != null && object.address() != 0) {
            g_object_unref.invoke(object)
        }
    }

    /**
     * Free memory allocated by VIPS (e.g., for buffers).
     * @param ptr pointer to free
     */
    void gFree(MemorySegment ptr) throws Throwable {
        if (ptr != null && ptr.address() != 0) {
            g_free.invoke(ptr)
        }
    }

    /**
     * Connect a callback to a GObject signal.
     * @param instance the object instance
     * @param detailedSignal signal name (e.g., "read", "seek")
     * @param cHandler the callback function (as MemorySegment)
     * @param data user data to pass to callback
     * @return handler id
     */
    long gSignalConnectData(MemorySegment instance, String detailedSignal, MemorySegment cHandler, MemorySegment data) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment signalSegment = tempArena.allocateFrom(detailedSignal)
            return (long) g_signal_connect_data.invoke(instance, signalSegment, cHandler, data, MemorySegment.NULL, 0)
        }
    }

    Arena getArena() {
        return arena
    }

    void close() {
        arena.close()
    }
}
