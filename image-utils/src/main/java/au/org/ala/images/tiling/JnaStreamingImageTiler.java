package au.org.ala.images.tiling;

import au.org.ala.images.jna.InputStreamVipsSource;
import au.org.ala.images.jna.NativeLibraryDetector;
import au.org.ala.images.jna.OutputStreamVipsTarget;
import au.org.ala.images.jna.VipsLibrary;
import com.google.common.io.ByteSink;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.ptr.PointerByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Experimental JNA-based tiler that uses libvips directly via JNA.
 * This avoids process spawning overhead and can be more efficient for high-throughput scenarios.
 * Automatically falls back to process-based approach if libvips is not available.
 */
public class JnaStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(JnaStreamingImageTiler.class);

    private final VipsLibrary vips;
    private final IImageTiler fallbackTiler;
    private final int tileSize;
    private final Executor levelExecutor;
    private final ZoomFactorStrategy zoomFactorStrategy;
    private final int vipsConcurrency;
    private final String encodeSuffix;
    private final boolean sameThreadExecutor;

    public JnaStreamingImageTiler(IImageTiler fallbackTiler, ImageTilerConfig config) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = config.getTileSize();
        this.levelExecutor = config.getLevelExecutor();
        this.zoomFactorStrategy = config.getZoomFactorStrategy();
        this.vipsConcurrency = config.getVipsConcurrency();
        this.encodeSuffix = resolveEncodeSuffix(config.getTileFormat());
        this.sameThreadExecutor = isSameThreadExecutor(this.levelExecutor);
        this.vips = NativeLibraryDetector.getVipsLibrary();

        if (vips != null) {
            applyVipsConcurrency();
            log.info("JnaStreamingImageTiler initialized with native libvips");
        } else {
            log.info("JnaStreamingImageTiler: libvips not available, will use fallback");
        }
    }

    private void applyVipsConcurrency() {
        if (vipsConcurrency <= 0) {
            return;
        }
        try {
            vips.vips_concurrency_set(vipsConcurrency);
            log.info("Set libvips concurrency to {} (JNA)", vips.vips_concurrency_get());
        } catch (Throwable e) {
            log.warn("Failed to set libvips concurrency to {} (JNA)", vipsConcurrency, e);
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int level) throws IOException, InterruptedException {
        if (vips == null) {
            log.debug("Using fallback tiler for single level");
            return fallbackTiler.tileImage(imageInputStream, tilerSink, level);
        }

        if (level < 0) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }

        if (imageInputStream.markSupported()) {
            imageInputStream.mark(10 * 1024 * 1024);
        }

        try {
            return tileLevelWithVipsJna(imageInputStream, tilerSink, level);
        } catch (Exception e) {
            log.error("JNA single-level tiling failed, trying fallback", e);
            if (fallbackTiler == null) {
                throw e;
            }
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset();
                } catch (IOException resetEx) {
                    log.warn("Failed to reset stream after failure", resetEx);
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, level);
        }
    }

    private ImageTilerResults tileLevelWithVipsJna(InputStream imageInputStream, TilerSink tilerSink, int level) throws IOException {
        InputStreamVipsSource vipsSource = null;
        Pointer inputImage = null;

        try {
            vipsSource = new InputStreamVipsSource(vips, imageInputStream);
            inputImage = vips.vips_image_new_from_source(vipsSource.getSource(), "", (Object) null);
            if (inputImage == null || inputImage == Pointer.NULL) {
                String error = vips.vips_error_buffer();
                vips.vips_error_clear();
                throw new IOException("Failed to load image from source: " + error);
            }

            int width = vips.vips_image_get_width(inputImage);
            int height = vips.vips_image_get_height(inputImage);
            int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width);
            int zoomLevels = pyramid.length;
            int maxLevel = zoomLevels - 1;

            if (level > maxLevel) {
                log.warn("Requested level {} is higher than maxLevel {}", level, maxLevel);
                return new ImageTilerResults(true, zoomLevels);
            }

            generateLevelTiles(inputImage, tilerSink.getLevelSink(level), level, pyramid[level]);
            return new ImageTilerResults(true, zoomLevels);

        } finally {
            if (inputImage != null) {
                vips.g_object_unref(inputImage);
            }
            if (vipsSource != null) {
                vipsSource.close();
            }
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        if (vips == null) {
            log.debug("Using fallback tiler");
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }

        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        try {
            return tileWithVipsJna(imageInputStream, tilerSink, minLevel, maxLevel);
        } catch (Exception e) {
            log.error("JNA tiling failed, trying fallback", e);
            if (fallbackTiler == null) {
                throw e;
            }
            if (imageInputStream.markSupported()) {
                imageInputStream.reset();
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }
    }

    private ImageTilerResults tileWithVipsJna(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        InputStreamVipsSource vipsSource = null;
        Pointer inputImage = null;

        try {
            vipsSource = new InputStreamVipsSource(vips, imageInputStream);
            inputImage = vips.vips_image_new_from_source(vipsSource.getSource(), "", (Object) null);
            if (inputImage == null || inputImage == Pointer.NULL) {
                String error = vips.vips_error_buffer();
                vips.vips_error_clear();
                throw new IOException("Failed to load image from source with libvips: " + error);
            }

            int originalWidth = vips.vips_image_get_width(inputImage);
            int originalHeight = vips.vips_image_get_height(inputImage);
            int[] pyramid = zoomFactorStrategy.getZoomFactors(originalHeight, originalWidth);
            int zoomLevels = pyramid.length;
            int finalMaxLevel = Math.min(maxLevel, zoomLevels - 1);

            if (minLevel > finalMaxLevel) {
                log.debug("tileWithVipsJna: asked for levels {} to {}, but only {} levels available", minLevel, maxLevel, zoomLevels);
                return new ImageTilerResults(true, zoomLevels);
            }

            for (int level = minLevel; level <= finalMaxLevel; level++) {
                generateLevelTiles(inputImage, tilerSink.getLevelSink(level), level, pyramid[level]);
            }

            return new ImageTilerResults(true, zoomLevels);
        } finally {
            if (inputImage != null && inputImage != Pointer.NULL) {
                vips.g_object_unref(inputImage);
            }
            if (vipsSource != null) {
                vipsSource.close();
            }
        }
    }

    private void generateLevelTiles(Pointer inputImage, TilerSink.LevelSink levelSink, int level, int subsample) throws IOException {
        double scale = 1.0d / (double) subsample;
        PointerByReference out = new PointerByReference();
        int resizeResult = vips.vips_resize(inputImage, out, scale, (Object) null);
        if (resizeResult != 0) {
            String error = vips.vips_error_buffer();
            vips.vips_error_clear();
            throw new IOException("vips_resize failed at level " + level + ": " + error);
        }

        Pointer resizedImage = out.getValue();
        try {
            int resizedWidth = vips.vips_image_get_width(resizedImage);
            int resizedHeight = vips.vips_image_get_height(resizedImage);
            int cols = (int) Math.ceil(resizedWidth / (double) tileSize);
            int rows = (int) Math.ceil(resizedHeight / (double) tileSize);
            boolean parallelFanOut = shouldUseParallelFanOut(cols, rows);

            if (parallelFanOut) {
                resizedImage = materializeLevelImage(resizedImage, level);
                resizedWidth = vips.vips_image_get_width(resizedImage);
                resizedHeight = vips.vips_image_get_height(resizedImage);
            }

            final Pointer levelImage = resizedImage;
            final int levelWidth = resizedWidth;
            final int levelHeight = resizedHeight;

            log.debug("Tiling level {}: {}x{} (subsample {}), {}x{} tiles", level, levelWidth, levelHeight, subsample, cols, rows);

            if (parallelFanOut) {
                List<CompletableFuture<Void>> futures = new ArrayList<>(cols * rows);
                AtomicBoolean errorOccurred = new AtomicBoolean(false);

                for (int col = 0; col < cols; col++) {
                    final int finalCol = col;
                    TilerSink.ColumnSink columnSink = levelSink.getColumnSink(finalCol, 0, 1);

                    for (int tmsRow = 0; tmsRow < rows; tmsRow++) {
                        final int finalTmsRow = tmsRow;

                        if (errorOccurred.get()) {
                            break;
                        }

                        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                            if (errorOccurred.get()) {
                                return;
                            }
                            try {
                                generateSingleTile(levelImage, columnSink, level, finalCol, finalTmsRow, levelWidth, levelHeight);
                            } catch (Exception e) {
                                errorOccurred.set(true);
                                log.error("Error generating tile level {}/{}/{}", level, finalCol, finalTmsRow, e);
                            }
                        }, levelExecutor);

                        futures.add(future);
                    }
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                if (errorOccurred.get()) {
                    throw new IOException("One or more tiles failed to generate at level " + level);
                }
            } else {
                for (int col = 0; col < cols; col++) {
                    TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, 1);
                    for (int tmsRow = 0; tmsRow < rows; tmsRow++) {
                        generateSingleTile(levelImage, columnSink, level, col, tmsRow, levelWidth, levelHeight);
                    }
                }
            }
        } finally {
            vips.g_object_unref(resizedImage);
        }
    }

    private void generateSingleTile(Pointer levelImage, TilerSink.ColumnSink columnSink, int level, int col, int tmsRow,
                                    int resizedWidth, int resizedHeight) throws IOException {
        int left = col * tileSize;
        int width = Math.min(tileSize, resizedWidth - left);

        int top = Math.max(0, resizedHeight - (tmsRow + 1) * tileSize);
        int bottom = resizedHeight - tmsRow * tileSize;
        int height = bottom - top;

        PointerByReference tileOut = new PointerByReference();
        int cropResult = vips.vips_crop(levelImage, tileOut, left, top, width, height, (Object) null);
        if (cropResult != 0) {
            String error = vips.vips_error_buffer();
            vips.vips_error_clear();
            throw new IOException("vips_crop failed at level " + level + " tile " + col + "/" + tmsRow + ": " + error);
        }

        Pointer tileImage = tileOut.getValue();
        try {
            ByteSink tileSink = columnSink.getTileSink(tmsRow);
            writeTile(tileImage, tileSink);
        } finally {
            vips.g_object_unref(tileImage);
        }
    }

    private Pointer materializeLevelImage(Pointer resizedImage, int level) throws IOException {
        // Prefer the dedicated copy-to-memory function (no varargs, widely available).
        Pointer materialized = vips.vips_image_copy_memory(resizedImage);
        if (materialized != null && materialized != Pointer.NULL) {
            vips.g_object_unref(resizedImage);
            return materialized;
        }

        // Fallback: force evaluation via a dummy write-to-buffer encode pass.
        forceEvaluateImage(resizedImage, level);
        return resizedImage;
    }

    private void forceEvaluateImage(Pointer image, int level) throws IOException {
        PointerByReference bufPtr = new PointerByReference();
        LongByReference lenPtr = new LongByReference();
        int saveResult = vips.vips_image_write_to_buffer(image, encodeSuffix, bufPtr, lenPtr, (Object) null);
        if (saveResult != 0) {
            String error = vips.vips_error_buffer();
            vips.vips_error_clear();
            throw new IOException("Failed to force level materialization at level " + level + ": " + error);
        }

        Pointer buf = bufPtr.getValue();
        if (buf != null && buf != Pointer.NULL) {
            vips.g_free(buf);
        }
    }

    private void writeTile(Pointer tileImage, ByteSink tileSink) throws IOException {
        try (java.io.OutputStream os = tileSink.openStream();
             OutputStreamVipsTarget vipsTarget = new OutputStreamVipsTarget(vips, os)) {
            if (vips.vips_image_write_to_target(tileImage, encodeSuffix, vipsTarget.getTarget(), (Object) null) != 0) {
                String error = vips.vips_error_buffer();
                vips.vips_error_clear();
                throw new IOException("vips_image_write_to_target failed: " + error);
            }
        }
    }

    private boolean shouldUseParallelFanOut(int cols, int rows) {
        return levelExecutor != null && !sameThreadExecutor && cols * rows > 1;
    }

    private static boolean isSameThreadExecutor(Executor executor) {
        if (executor == null) {
            return true;
        }
        AtomicBoolean ranInline = new AtomicBoolean(false);
        Thread caller = Thread.currentThread();
        try {
            executor.execute(() -> ranInline.set(Thread.currentThread() == caller));
            return ranInline.get();
        } catch (RuntimeException ex) {
            return false;
        }
    }

    private static String resolveEncodeSuffix(TileFormat tileFormat) {
        return tileFormat == TileFormat.PNG ? ".png" : ".jpg";
    }
}
