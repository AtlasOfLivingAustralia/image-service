package au.org.ala.images.tiling;

import au.org.ala.images.jna.InputStreamVipsSource;
import au.org.ala.images.jna.NativeLibraryDetector;
import au.org.ala.images.jna.NativeDzTilerBridgeLoader;
import au.org.ala.images.jna.NativeDzTilerLibrary;
import au.org.ala.images.jna.VipsLibrary;
import com.google.common.io.ByteSink;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Native bridge tiler that mirrors dzsave-style google pyramid geometry while emitting
 * tiles via callback directly into {@link TilerSink}.
 */
public class NativeDzStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(NativeDzStreamingImageTiler.class);

    private static final int DEFAULT_JPEG_QUALITY = 82;
    private static final int DEFAULT_PNG_COMPRESSION = 6;
    private static final int VIPS_ACCESS_RANDOM = 1;

    private final IImageTiler fallbackTiler;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final Color tileBackgroundColor;
    private final boolean padTiles;
    private final ZoomFactorStrategy zoomFactorStrategy;
    private final NativeDzTilerLibrary nativeLib;
    private final VipsLibrary vips;

    public NativeDzStreamingImageTiler(IImageTiler fallbackTiler, ImageTilerConfig config) {
        this(fallbackTiler, config, NativeDzTilerBridgeLoader.getLibrary(), NativeLibraryDetector.getVipsLibrary());
    }

    NativeDzStreamingImageTiler(
            IImageTiler fallbackTiler,
            ImageTilerConfig config,
            NativeDzTilerLibrary nativeLib,
            VipsLibrary vips
    ) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = config.getTileSize();
        this.tileFormat = config.getTileFormat();
        this.tileBackgroundColor = config.getTileBackgroundColor();
        this.padTiles = config.isPadTiles();
        this.zoomFactorStrategy = config.getZoomFactorStrategy();
        this.nativeLib = nativeLib;
        this.vips = vips;
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int level)
            throws IOException, InterruptedException {
        return tileImage(imageInputStream, tilerSink, level, level);
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel)
            throws IOException, InterruptedException {
        if (nativeLib == null || vips == null) {
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }
        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        try (InputStreamVipsSource vipsSource = new InputStreamVipsSource(vips, imageInputStream)) {
            if (!vipsSource.isSeekable()) {
                log.debug("Native dz tiler requires seekable input for VIPS_ACCESS_RANDOM; using fallback tiler");
                return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
            }

            int[] dimensions = probeDimensions(vipsSource);
            int[] pyramid = zoomFactorStrategy.getZoomFactors(dimensions[1], dimensions[0]);
            int finalMax = Math.min(maxLevel, pyramid.length - 1);
            if (minLevel > finalMax) {
                return new ImageTilerResults(true, pyramid.length);
            }

            PointerByReference errorRef = new PointerByReference();
            NativeDzTilerLibrary.TileCallback callback = (level, x, y, contentType, data, length, userData) -> {
                try {
                    TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
                    TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
                    ByteSink sink = columnSink.getTileSink(y);

                    byte[] tileBytes = data.getByteArray(0, (int) length);
                    try (OutputStream os = sink.openStream()) {
                        os.write(tileBytes);
                    }
                    return 0;
                } catch (Exception e) {
                    log.error("Failed writing native tile level={}/x={}/y={}", level, x, y, e);
                    return -1;
                }
            };

            int rc;
            rc = nativeLib.ala_vips_google_tms_tiles_from_source(
                    vipsSource.getSource(),
                    pyramid,
                    pyramid.length,
                    tileSize,
                    minLevel,
                    finalMax,
                    tileFormat == TileFormat.PNG ? ".png" : ".jpg",
                    DEFAULT_JPEG_QUALITY,
                    DEFAULT_PNG_COMPRESSION,
                    padTiles,
                    tileBackgroundColor.getRed(),
                    tileBackgroundColor.getGreen(),
                    tileBackgroundColor.getBlue(),
                    callback,
                    Pointer.NULL,
                    errorRef
            );

            if (rc != 0) {
                String nativeError = "native dz tiler failed";
                Pointer errPtr = errorRef.getValue();
                if (errPtr != null && errPtr != Pointer.NULL) {
                    nativeError = errPtr.getString(0);
                    nativeLib.ala_vips_google_tms_free_error(errPtr);
                }
                throw new IOException(nativeError);
            }

            return new ImageTilerResults(true, pyramid.length);
        } catch (Exception e) {
            log.warn("Native dz tiler failed, using fallback: {}", e.getMessage());
            if (fallbackTiler == null) {
                throw e instanceof IOException ? (IOException) e : new IOException(e);
            }
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset();
                    return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
                } catch (IOException resetError) {
                    throw new IOException("Native dz tiler failed and input stream could not be reset for fallback", resetError);
                }
            }
            throw new IOException("Native dz tiler failed and fallback requires a resettable input stream", e);
        }
    }

    private int[] probeDimensions(InputStreamVipsSource vipsSource) throws IOException {
        Pointer image = vips.vips_image_new_from_source(
                vipsSource.getSource(),
                "",
                "access", VIPS_ACCESS_RANDOM,
                (Object) null
        );
        if (image == null || image == Pointer.NULL) {
            String err = vips.vips_error_buffer();
            vips.vips_error_clear();
            throw new IOException("Failed to probe source image dimensions: " + err);
        }
        try {
            int width = vips.vips_image_get_width(image);
            int height = vips.vips_image_get_height(image);
            return new int[]{width, height};
        } finally {
            vips.g_object_unref(image);
        }
    }
}
