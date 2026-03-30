package au.org.ala.images.ffm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM (Foreign Function & Memory API) interface to libvips C library.
 * This replaces the JNA-based interface with the modern Panama FFM API.
 *
 * libvips documentation: https://www.libvips.org/API/current/
 */
public class VipsLibraryFFM implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(VipsLibraryFFM.class);

    private final Linker linker;
    private final Arena arena;

    // Method handles for libvips functions
    private final MethodHandle vips_init;
    private final MethodHandle vips_shutdown;
    private final MethodHandle vips_error_buffer;
    private final MethodHandle vips_error_clear;
    private final MethodHandle vips_image_new_from_buffer;
    private final MethodHandle vips_image_new_from_source;
    private final MethodHandle vips_image_get_width;
    private final MethodHandle vips_image_get_height;
    private final MethodHandle vips_thumbnail_image;
    private final MethodHandle vips_image_write_to_buffer;
    private final MethodHandle vips_image_write_to_target;
    private final MethodHandle vips_dzsave;
    private final MethodHandle vips_crop;
    private final MethodHandle vips_rot;
    private final MethodHandle vips_flip;
    private final MethodHandle vips_colourspace;
    private final MethodHandle vips_relational_const;
    private final MethodHandle vips_resize;
    private final MethodHandle vips_source_custom_new;
    private final MethodHandle vips_target_custom_new;
    private final MethodHandle g_object_unref;
    private final MethodHandle g_free;
    private final MethodHandle g_signal_connect_data;

    // Function descriptors for libvips API
    public static final FunctionDescriptor FD_vips_init = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_shutdown = FunctionDescriptor.ofVoid();
    public static final FunctionDescriptor FD_vips_error_buffer = FunctionDescriptor.of(ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_error_clear = FunctionDescriptor.ofVoid();
    public static final FunctionDescriptor FD_vips_image_new_from_buffer = FunctionDescriptor.of(
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_image_new_from_source = FunctionDescriptor.of(
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_image_get_width = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_image_get_height = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_thumbnail_image = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_image_write_to_buffer = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_image_write_to_target = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_dzsave = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_crop = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_rot = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_flip = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_colourspace = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_relational_const = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_resize = FunctionDescriptor.of(
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_source_custom_new = FunctionDescriptor.of(ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_vips_target_custom_new = FunctionDescriptor.of(ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_g_object_unref = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_g_free = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
    public static final FunctionDescriptor FD_g_signal_connect_data = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT);

    public VipsLibraryFFM(SymbolLookup libvipsLookup, SymbolLookup libgobjectLookup, SymbolLookup libglibLookup) {
        this.linker = Linker.nativeLinker();
        this.arena = Arena.ofShared();

        try {
            // Look up and link function symbols
            this.vips_init = lookupFunction(libvipsLookup, "vips_init", FD_vips_init);
            this.vips_shutdown = lookupFunction(libvipsLookup, "vips_shutdown", FD_vips_shutdown);
            this.vips_error_buffer = lookupFunction(libvipsLookup, "vips_error_buffer", FD_vips_error_buffer);
            this.vips_error_clear = lookupFunction(libvipsLookup, "vips_error_clear", FD_vips_error_clear);
            this.vips_image_new_from_buffer = lookupFunction(libvipsLookup, "vips_image_new_from_buffer", FD_vips_image_new_from_buffer);
            this.vips_image_new_from_source = lookupFunction(libvipsLookup, "vips_image_new_from_source", FD_vips_image_new_from_source);
            this.vips_image_get_width = lookupFunction(libvipsLookup, "vips_image_get_width", FD_vips_image_get_width);
            this.vips_image_get_height = lookupFunction(libvipsLookup, "vips_image_get_height", FD_vips_image_get_height);
            this.vips_thumbnail_image = lookupFunction(libvipsLookup, "vips_thumbnail_image", FD_vips_thumbnail_image);
            this.vips_image_write_to_buffer = lookupFunction(libvipsLookup, "vips_image_write_to_buffer", FD_vips_image_write_to_buffer);
            this.vips_image_write_to_target = lookupFunction(libvipsLookup, "vips_image_write_to_target", FD_vips_image_write_to_target);
            this.vips_dzsave = lookupFunction(libvipsLookup, "vips_dzsave", FD_vips_dzsave);
            this.vips_crop = lookupFunction(libvipsLookup, "vips_crop", FD_vips_crop);
            this.vips_rot = lookupFunction(libvipsLookup, "vips_rot", FD_vips_rot);
            this.vips_flip = lookupFunction(libvipsLookup, "vips_flip", FD_vips_flip);
            this.vips_colourspace = lookupFunction(libvipsLookup, "vips_colourspace", FD_vips_colourspace);
            this.vips_relational_const = lookupFunction(libvipsLookup, "vips_relational_const", FD_vips_relational_const);
            this.vips_resize = lookupFunction(libvipsLookup, "vips_resize", FD_vips_resize);
            this.vips_source_custom_new = lookupFunction(libvipsLookup, "vips_source_custom_new", FD_vips_source_custom_new);
            this.vips_target_custom_new = lookupFunction(libvipsLookup, "vips_target_custom_new", FD_vips_target_custom_new);
            this.g_object_unref = lookupFunction(libgobjectLookup, "g_object_unref", FD_g_object_unref);
            this.g_free = lookupFunction(libglibLookup, "g_free", FD_g_free);
            this.g_signal_connect_data = lookupFunction(libgobjectLookup, "g_signal_connect_data", FD_g_signal_connect_data);
        } catch (Throwable t) {
            arena.close();
            throw t;
        }
    }

    private MethodHandle lookupFunction(SymbolLookup lookup, String name, FunctionDescriptor descriptor) {
        MemorySegment symbol = lookup.find(name).orElseThrow(() ->
            new UnsatisfiedLinkError("Unable to find symbol: " + name)
        );
        return linker.downcallHandle(symbol, descriptor);
    }

    public int vipsInit(String argv0) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment nameSegment = argv0 != null ? tempArena.allocateUtf8String(argv0) : MemorySegment.NULL;
            return (int) vips_init.invokeExact(nameSegment);
        }
    }

    public void vipsShutdown() throws Throwable {
        vips_shutdown.invokeExact();
    }

    public String vipsErrorBuffer() throws Throwable {
        MemorySegment errorPtr = (MemorySegment) vips_error_buffer.invokeExact();
        if (errorPtr == null || errorPtr.address() == 0) {
            return "";
        }
        return errorPtr.getUtf8String(0);
    }

    public void vipsErrorClear() throws Throwable {
        vips_error_clear.invokeExact();
    }

    public MemorySegment vipsImageNewFromBuffer(MemorySegment buf, long len, String options) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment optionsSegment = options != null ? tempArena.allocateUtf8String(options) : MemorySegment.NULL;
            return (MemorySegment) vips_image_new_from_buffer.invokeExact(buf, len, optionsSegment, MemorySegment.NULL);
        }
    }

    public MemorySegment vipsImageNewFromSource(MemorySegment source, String options) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment optionsSegment = options != null ? tempArena.allocateUtf8String(options) : tempArena.allocateUtf8String("");
            return (MemorySegment) vips_image_new_from_source.invokeExact(source, optionsSegment, MemorySegment.NULL);
        }
    }

    public int vipsImageGetWidth(MemorySegment image) throws Throwable {
        return (int) vips_image_get_width.invokeExact(image);
    }

    public int vipsImageGetHeight(MemorySegment image) throws Throwable {
        return (int) vips_image_get_height.invokeExact(image);
    }

    public int vipsThumbnailImage(MemorySegment input, MemorySegment outPtr, int width) throws Throwable {
        return (int) vips_thumbnail_image.invokeExact(input, outPtr, width, MemorySegment.NULL);
    }

    public int vipsImageWriteToBuffer(MemorySegment image, MemorySegment bufPtr, MemorySegment lenPtr, String suffix) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment suffixSegment = tempArena.allocateUtf8String(suffix);
            return (int) vips_image_write_to_buffer.invokeExact(image, suffixSegment, bufPtr, lenPtr, MemorySegment.NULL);
        }
    }

    public int vipsImageWriteToTarget(MemorySegment image, String suffix, MemorySegment target) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment suffixSegment = tempArena.allocateUtf8String(suffix);
            return (int) vips_image_write_to_target.invokeExact(image, suffixSegment, target, MemorySegment.NULL);
        }
    }

    public int vipsDzsave(MemorySegment input, String outputPath) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment pathSegment = tempArena.allocateUtf8String(outputPath);
            return (int) vips_dzsave.invokeExact(input, pathSegment, MemorySegment.NULL);
        }
    }

    public int vipsCrop(MemorySegment input, MemorySegment outPtr, int left, int top, int width, int height) throws Throwable {
        return (int) vips_crop.invokeExact(input, outPtr, left, top, width, height, MemorySegment.NULL);
    }

    public int vipsRot(MemorySegment input, MemorySegment outPtr, int angle) throws Throwable {
        return (int) vips_rot.invokeExact(input, outPtr, angle, MemorySegment.NULL);
    }

    public int vipsFlip(MemorySegment input, MemorySegment outPtr, int direction) throws Throwable {
        return (int) vips_flip.invokeExact(input, outPtr, direction, MemorySegment.NULL);
    }

    public int vipsColourspace(MemorySegment input, MemorySegment outPtr, int interpretation) throws Throwable {
        return (int) vips_colourspace.invokeExact(input, outPtr, interpretation, MemorySegment.NULL);
    }

    public int vipsRelationalConst(MemorySegment input, MemorySegment outPtr, int relational, double[] c) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment cSegment = tempArena.allocateArray(ValueLayout.JAVA_DOUBLE, c);
            return (int) vips_relational_const.invokeExact(input, outPtr, relational, cSegment, c.length, MemorySegment.NULL);
        }
    }

    public int vipsResize(MemorySegment input, MemorySegment outPtr, double scale) throws Throwable {
        return (int) vips_resize.invokeExact(input, outPtr, scale, MemorySegment.NULL);
    }

    public MemorySegment vipsSourceCustomNew() throws Throwable {
        return (MemorySegment) vips_source_custom_new.invokeExact();
    }

    public MemorySegment vipsTargetCustomNew() throws Throwable {
        return (MemorySegment) vips_target_custom_new.invokeExact();
    }

    public void gObjectUnref(MemorySegment object) throws Throwable {
        if (object != null && object.address() != 0) {
            g_object_unref.invokeExact(object);
        }
    }

    public void gFree(MemorySegment ptr) throws Throwable {
        if (ptr != null && ptr.address() != 0) {
            g_free.invokeExact(ptr);
        }
    }

    public long gSignalConnectData(MemorySegment instance, String detailedSignal, MemorySegment cHandler, MemorySegment data) throws Throwable {
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment signalSegment = tempArena.allocateUtf8String(detailedSignal);
            return (long) g_signal_connect_data.invokeExact(instance, signalSegment, cHandler, data, MemorySegment.NULL, 0);
        }
    }

    public Arena getArena() {
        return arena;
    }

    public void close() {
        arena.close();
    }
}
