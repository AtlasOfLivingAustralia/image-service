package au.org.ala.images.tiling;

import au.org.ala.images.ffm.InputStreamVipsSourceFFM;
import au.org.ala.images.ffm.NativeDzTilerBridgeLoaderFFM;
import au.org.ala.images.ffm.NativeDzTilerLibraryFFM;
import au.org.ala.images.ffm.NativeLibraryDetectorFFM;
import au.org.ala.images.ffm.VipsLibraryFFM;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * FFM variant of the native-bridge deep zoom tiler.
 */
public class FfmNativeDzStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(FfmNativeDzStreamingImageTiler.class);

    private static final int DEFAULT_JPEG_QUALITY = 82;
    private static final int DEFAULT_PNG_COMPRESSION = 6;
    private static final String RANDOM_ACCESS_OPTIONS = "access=random";

    private final IImageTiler fallbackTiler;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final ZoomFactorStrategy zoomFactorStrategy;
    private final NativeDzTilerLibraryFFM nativeDzLib;
    private final VipsLibraryFFM vips;

    public FfmNativeDzStreamingImageTiler(IImageTiler fallbackTiler, ImageTilerConfig config) {
        this(fallbackTiler, config, NativeDzTilerBridgeLoaderFFM.getLibrary(), NativeLibraryDetectorFFM.getVipsLibrary());
    }

    FfmNativeDzStreamingImageTiler(
            IImageTiler fallbackTiler,
            ImageTilerConfig config,
            NativeDzTilerLibraryFFM nativeDzLib,
            VipsLibraryFFM vips
    ) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = config.getTileSize();
        this.tileFormat = config.getTileFormat();
        this.zoomFactorStrategy = config.getZoomFactorStrategy();
        this.nativeDzLib = nativeDzLib;
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
        if (nativeDzLib == null || vips == null) {
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }
        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        try (InputStreamVipsSourceFFM vipsSource = new InputStreamVipsSourceFFM(vips, imageInputStream)) {
            if (!vipsSource.isSeekable()) {
                log.debug("FFM native dz tiler requires seekable input for random-access decode; using fallback tiler");
                return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
            }

            int[] dimensions = probeDimensions(vipsSource);
            int[] pyramid = zoomFactorStrategy.getZoomFactors(dimensions[1], dimensions[0]);
            int finalMax = Math.min(maxLevel, pyramid.length - 1);
            if (minLevel > finalMax) {
                return new ImageTilerResults(true, pyramid.length);
            }

            nativeDzLib.alaVipsGoogleTmsTilesFromSource(
                    vipsSource.getSource(),
                    pyramid,
                    pyramid.length,
                    tileSize,
                    minLevel,
                    finalMax,
                    tileFormat == TileFormat.PNG ? ".png" : ".jpg",
                    DEFAULT_JPEG_QUALITY,
                    DEFAULT_PNG_COMPRESSION,
                    (level, x, y, contentType, data, length, userData) -> {
                        try {
                            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
                            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
                            ByteSink sink = columnSink.getTileSink(y);
                            byte[] tileBytes = toByteArray(data, length);
                            try (OutputStream os = sink.openStream()) {
                                os.write(tileBytes);
                            }
                            return 0;
                        } catch (Exception e) {
                            log.error("Failed writing FFM native tile level={}/x={}/y={}", level, x, y, e);
                            return -1;
                        }
                    },
                    MemorySegment.NULL
            );

            return new ImageTilerResults(true, pyramid.length);
        } catch (Exception e) {
            log.warn("FFM native dz tiler failed, using fallback: {}", e.getMessage());
            if (fallbackTiler == null) {
                throw e instanceof IOException ? (IOException) e : new IOException(e);
            }
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset();
                    return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
                } catch (IOException resetError) {
                    throw new IOException("FFM native dz tiler failed and input stream could not be reset for fallback", resetError);
                }
            }
            throw new IOException("FFM native dz tiler failed and fallback requires a resettable input stream", e);
        }
    }

    private int[] probeDimensions(InputStreamVipsSourceFFM vipsSource) throws IOException {
        try {
            MemorySegment image = vips.vipsImageNewFromSource(vipsSource.getSource(), RANDOM_ACCESS_OPTIONS);
            if (image == null || image.address() == 0) {
                String err = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to probe source image dimensions: " + err);
            }
            try {
                int width = vips.vipsImageGetWidth(image);
                int height = vips.vipsImageGetHeight(image);
                return new int[]{width, height};
            } finally {
                vips.gObjectUnref(image);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to probe source image dimensions", t);
        }
    }

    private static byte[] toByteArray(MemorySegment data, long length) {
        if (length <= 0) {
            return new byte[0];
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile payload exceeds JVM byte[] size limits: " + length);
        }
        MemorySegment bounded = data.reinterpret(length);
        return bounded.toArray(ValueLayout.JAVA_BYTE);
    }
}

