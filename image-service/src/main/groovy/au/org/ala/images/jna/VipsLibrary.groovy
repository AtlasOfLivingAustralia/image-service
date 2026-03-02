package au.org.ala.images.jna

import com.sun.jna.Callback
import com.sun.jna.Library
import com.sun.jna.Native
import com.sun.jna.Pointer
import com.sun.jna.ptr.LongByReference
import com.sun.jna.ptr.PointerByReference
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * JNA interface to libvips C library.
 * This provides direct access to libvips image processing functions.
 *
 * libvips documentation: https://www.libvips.org/API/current/
 */
@CompileStatic
interface VipsLibrary extends Library {

    // VipsImage is an opaque pointer in C, represented as Pointer in JNA
    // VipsSource and VipsSourceCustom are also opaque pointers

    /**
     * Callback for writing data to a custom target.
     * @param target the VipsTarget
     * @param buffer buffer containing data to write
     * @param length number of bytes to write
     * @param user_data user data pointer
     * @return number of bytes written, -1 for error
     */
    interface WriteCallback extends Callback {
        long invoke(Pointer target, Pointer buffer, long length, Pointer user_data)
    }

    /**
     * Callback for reading data from a custom source.
     * @param source the VipsSource
     * @param buffer buffer to write data to
     * @param length number of bytes to read
     * @param user_data user data pointer
     * @return number of bytes read, 0 for EOF, -1 for error
     */
    interface ReadCallback extends Callback {
        long invoke(Pointer source, Pointer buffer, long length, Pointer user_data)
    }

    /**
     * Callback for seeking in a custom source.
     * @param source the VipsSource
     * @param offset offset to seek to
     * @param whence SEEK_SET (0), SEEK_CUR (1), or SEEK_END (2)
     * @param user_data user data pointer
     * @return new position, -1 for error
     */
    interface SeekCallback extends Callback {
        long invoke(Pointer source, long offset, int whence, Pointer user_data)
    }

    /**
     * Initialize the VIPS library. Must be called before using any other VIPS functions.
     * @param argv0 program name (can be null)
     * @return 0 on success
     */
    int vips_init(String argv0)

    /**
     * Shutdown the VIPS library and free resources.
     */
    void vips_shutdown()

    /**
     * Get the last VIPS error message.
     * @return error message string
     */
    String vips_error_buffer()

    /**
     * Clear the VIPS error buffer.
     */
    void vips_error_clear()

    /**
     * Load an image from a buffer in memory.
     * @param buf pointer to image data
     * @param len length of image data
     * @param options options string (can be null)
     * @return VipsImage pointer or null on error
     */
    Pointer vips_image_new_from_buffer(Pointer buf, long len, String options)

    /**
     * Create a new image object from a file descriptor.
     * @param fd file descriptor
     * @return VipsImage pointer or null on error
     */
    Pointer vips_image_new_from_file(String filename)

    /**
     * Free a VipsImage.
     * @param image the image to free
     */
    void g_object_unref(Pointer image)

    /**
     * Get image width.
     * @param image the image
     * @return width in pixels
     */
    int vips_image_get_width(Pointer image)

    /**
     * Get image height.
     * @param image the image
     * @return height in pixels
     */
    int vips_image_get_height(Pointer image)

    /**
     * Thumbnail an image. This is a high-level operation that will
     * shrink or expand an image to fit within a bounding box.
     * @param input input image
     * @param out pointer to receive output image
     * @param width target width
     * @param height target height (optional, use -1 for auto)
     * @return 0 on success
     */
    int vips_thumbnail_image(Pointer input, Pointer out, int width, Object... options)

    /**
     * Thumbnail an image from a buffer.
     * @param buf pointer to image data
     * @param len length of image data
     * @param out pointer to receive output image
     * @param width target width
     * @param options variable arguments (null-terminated)
     * @return 0 on success
     */
    int vips_thumbnail_buffer(Pointer buf, long len, Pointer out, int width, Object... options)

    /**
     * Save an image to a buffer.
     * @param image the image to save
     * @param suffix file format suffix (e.g., ".jpg", ".png")
     * @param buf pointer to receive buffer pointer
     * @param len pointer to receive buffer length
     * @param options variable arguments (null-terminated)
     * @return 0 on success
     */
    int vips_image_write_to_buffer(Pointer image, String suffix, PointerByReference buf, LongByReference len, Object... options)

    /**
     * Save an image to a VipsTarget.
     * @param image the image to save
     * @param suffix file format suffix (e.g., ".jpg", ".png")
     * @param target the target to write to
     * @param options variable arguments (null-terminated)
     * @return 0 on success
     */
    int vips_image_write_to_target(Pointer image, String suffix, Pointer target, Object... options)

    /**
     * Save image to file.
     * @param image the image
     * @param filename output filename
     * @return 0 on success
     */
    int vips_image_write_to_file(Pointer image, String filename)

    /**
     * Create Deep Zoom pyramid tiles from an image.
     * @param input input image
     * @param output output directory path
     * @param options variable arguments (null-terminated)
     * @return 0 on success
     */
    int vips_dzsave(Pointer input, String output, Object... options)

    /**
     * Crop an image (extract a rectangular region).
     * @param input input image
     * @param out pointer to receive output image
     * @param left left coordinate
     * @param top top coordinate
     * @param width width of region
     * @param height height of region
     * @return 0 on success
     */
    int vips_crop(Pointer input, Pointer out, int left, int top, int width, int height)

    /**
     * Resize an image.
     * @param input input image
     * @param out pointer to receive output image
     * @param scale scale factor
     * @param options variable arguments (null-terminated)
     * @return 0 on success
     */
    int vips_resize(Pointer input, Pointer out, double scale, String... options)

    /**
     * Free memory allocated by VIPS (e.g., for buffers).
     * @param ptr pointer to free
     */
    void g_free(Pointer ptr)

    // VipsSource API (libvips 8.9+)

    /**
     * Create a new VipsSourceCustom.
     * @return new VipsSourceCustom pointer
     */
    Pointer vips_source_custom_new()

    /**
     * Create a new VipsTargetCustom.
     * @return new VipsTargetCustom pointer
     */
    Pointer vips_target_custom_new()

    /**
     * Load an image from a VipsSource.
     * @param source the source to read from
     * @param options options string (can be empty)
     * @return VipsImage pointer or null on error
     */
    Pointer vips_image_new_from_source(Pointer source, String options, Object... varargs)

    /**
     * Thumbnail an image from a source.
     * @param source the source to read from
     * @param out pointer to receive output image
     * @param width target width
     * @param options variable arguments (null-terminated)
     * @return 0 on success
     */
    int vips_thumbnail_source(Pointer source, Pointer out, int width, Object... options)

    /**
     * Signal handlers for VipsSourceCustom.
     * These are connected using g_signal_connect.
     */

    /**
     * Connect a callback to a GObject signal.
     * @param instance the object instance
     * @param detailed_signal signal name (e.g., "read", "seek")
     * @param c_handler the callback function
     * @param data user data to pass to callback
     * @return handler id
     */
    long g_signal_connect_data(Pointer instance, String detailed_signal, Callback c_handler,
                               Pointer data, Pointer destroy_data, int connect_flags)

    /**
     * Get the GType for VipsSourceCustom.
     * Used for type checking and object creation.
     */
    Pointer vips_source_custom_get_type()

    /**
     * Create a new GObject of the given type.
     * @param object_type the GType
     * @param first_property_name first property name (or null)
     * @return new object pointer
     */
    Pointer g_object_new(Pointer object_type, String first_property_name)
}
