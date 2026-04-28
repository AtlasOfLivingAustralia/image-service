package au.org.ala.images.tiling;

import au.org.ala.images.ffm.InputStreamVipsSourceFFM;
import au.org.ala.images.ffm.NativeLibraryDetectorFFM;
import au.org.ala.images.ffm.OutputStreamVipsTargetFFM;
import au.org.ala.images.ffm.VipsLibraryFFM;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FFM-based tiler that uses libvips directly via the Foreign Function & Memory API.
 */
public class FfmStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(FfmStreamingImageTiler.class);

    private final VipsLibraryFFM vips;
    private final IImageTiler fallbackTiler;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final Color tileBackgroundColor;
    private final ZoomFactorStrategy zoomFactorStrategy;
    private final Executor levelExecutor;
    private final int vipsConcurrency;
    private final String encodeSuffix;
    private final boolean sameThreadExecutor;
    private final boolean padTiles;

    public FfmStreamingImageTiler(IImageTiler fallbackTiler, ImageTilerConfig config) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = config.getTileSize();
        this.tileFormat = config.getTileFormat();
        this.tileBackgroundColor = config.getTileBackgroundColor();
        this.levelExecutor = config.getLevelExecutor();
        this.vipsConcurrency = config.getVipsConcurrency();
        this.encodeSuffix = resolveEncodeSuffix(config.getTileFormat());
        this.sameThreadExecutor = isSameThreadExecutor(this.levelExecutor);
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary();
        this.zoomFactorStrategy = config.getZoomFactorStrategy();
        this.padTiles = config.isPadTiles();

        if (vips != null) {
            applyVipsConcurrency();
            log.info("FfmStreamingImageTiler initialized with native libvips (FFM)");
        } else {
            log.info("FfmStreamingImageTiler: libvips not available, will use fallback");
        }
    }

    private void applyVipsConcurrency() {
        if (vipsConcurrency <= 0) {
            return;
        }
        try {
            vips.vipsConcurrencySet(vipsConcurrency);
            log.info("Set libvips concurrency to {} (FFM)", vips.vipsConcurrencyGet());
        } catch (Throwable e) {
            log.warn("Failed to set libvips concurrency to {} (FFM)", vipsConcurrency, e);
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
            return tileLevelWithVipsFFM(imageInputStream, tilerSink, level);
        } catch (Exception e) {
            log.error("FFM single-level tiling failed, trying fallback", e);
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

    private ImageTilerResults tileLevelWithVipsFFM(InputStream imageInputStream, TilerSink tilerSink, int level) throws IOException {
        InputStreamVipsSourceFFM vipsSource = null;
        MemorySegment inputImage = null;

        try {
            vipsSource = new InputStreamVipsSourceFFM(vips, imageInputStream);
            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "");
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to load image from source: " + error);
            }

            int width = vips.vipsImageGetWidth(inputImage);
            int height = vips.vipsImageGetHeight(inputImage);
            int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width);
            int zoomLevels = pyramid.length;
            int maxLevel = zoomLevels - 1;

            if (level > maxLevel) {
                log.warn("Requested level {} is higher than maxLevel {}", level, maxLevel);
                return new ImageTilerResults(true, zoomLevels);
            }

            generateLevelTiles(inputImage, tilerSink.getLevelSink(level), level, pyramid[level]);
            return new ImageTilerResults(true, zoomLevels);

        } catch (Throwable e) {
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("Error tiling image with FFM", e);
        } finally {
            try {
                if (inputImage != null && inputImage.address() != 0) {
                    vips.gObjectUnref(inputImage);
                }
            } catch (Throwable t) {
                log.warn("Error cleaning up vips image", t);
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

        if (imageInputStream.markSupported()) {
            imageInputStream.mark(Integer.MAX_VALUE);
        }

        try {
            return tileWithVipsFFM(imageInputStream, tilerSink, minLevel, maxLevel);
        } catch (Exception e) {
            log.error("FFM tiling failed, trying fallback", e);
            if (fallbackTiler == null) {
                throw e;
            }
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset();
                } catch (IOException resetEx) {
                    log.warn("Failed to reset image input stream after FFM tiling failure; proceeding with fallback anyway", resetEx);
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }
    }

    private ImageTilerResults tileWithVipsFFM(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        InputStreamVipsSourceFFM vipsSource = null;
        MemorySegment inputImage = null;

        try {
            vipsSource = new InputStreamVipsSourceFFM(vips, imageInputStream);
            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "");
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to load image from source with libvips: " + error);
            }

            int originalWidth = vips.vipsImageGetWidth(inputImage);
            int originalHeight = vips.vipsImageGetHeight(inputImage);
            int[] pyramid = zoomFactorStrategy.getZoomFactors(originalHeight, originalWidth);
            int zoomLevels = pyramid.length;
            int finalMaxLevel = Math.min(maxLevel, zoomLevels - 1);

            if (minLevel > finalMaxLevel) {
                log.debug("tileWithVipsFFM: asked for levels {} to {}, but only {} levels available", minLevel, maxLevel, zoomLevels);
                return new ImageTilerResults(true, zoomLevels);
            }

            for (int level = minLevel; level <= finalMaxLevel; level++) {
                generateLevelTiles(inputImage, tilerSink.getLevelSink(level), level, pyramid[level]);
            }

            return new ImageTilerResults(true, zoomLevels);

        } catch (Throwable e) {
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("Error tiling image with FFM", e);
        } finally {
            if (inputImage != null && inputImage.address() != 0) {
                try {
                    vips.gObjectUnref(inputImage);
                } catch (Throwable e) {
                    log.warn("Error unreffing input image", e);
                }
            }
            if (vipsSource != null) {
                vipsSource.close();
            }
        }
    }

    private void generateLevelTiles(MemorySegment inputImage, TilerSink.LevelSink levelSink, int level, int subsample) throws IOException {
        try (Arena arena = Arena.ofShared()) {
            double scale = 1.0d / (double) subsample;
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            int resizeResult = vips.vipsResize(inputImage, outPtr, scale);
            if (resizeResult != 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("vips_resize failed at level " + level + ": " + error);
            }

            MemorySegment resizedImage = outPtr.get(ValueLayout.ADDRESS, 0);
            if (resizedImage == null || resizedImage.address() == 0) {
                throw new IOException("vips_resize returned null image at level " + level);
            }

            int resizedWidth = vips.vipsImageGetWidth(resizedImage);
            int resizedHeight = vips.vipsImageGetHeight(resizedImage);
            int cols = (int) Math.ceil(resizedWidth / (double) tileSize);
            int rows = (int) Math.ceil(resizedHeight / (double) tileSize);
            boolean parallelFanOut = shouldUseParallelFanOut(cols, rows);

            if (parallelFanOut) {
                resizedImage = materializeLevelImage(resizedImage, arena, level);
                resizedWidth = vips.vipsImageGetWidth(resizedImage);
                resizedHeight = vips.vipsImageGetHeight(resizedImage);
            }

            try {
                final MemorySegment levelImage = resizedImage;
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

                                try (Arena threadArena = Arena.ofConfined()) {
                                    generateSingleTile(levelImage, columnSink, level, finalCol, finalTmsRow, levelWidth, levelHeight, threadArena);
                                } catch (Throwable e) {
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
                            try (Arena threadArena = Arena.ofConfined()) {
                                generateSingleTile(levelImage, columnSink, level, col, tmsRow, levelWidth, levelHeight, threadArena);
                            }
                        }
                    }
                }
            } finally {
                vips.gObjectUnref(resizedImage);
            }
        } catch (Throwable e) {
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("Error generating tiles at level " + level, e);
        }
    }

    private void generateSingleTile(MemorySegment levelImage, TilerSink.ColumnSink columnSink, int level, int col, int tmsRow,
                                    int resizedWidth, int resizedHeight, Arena arena) throws Throwable {
        int left = col * tileSize;
        int width = Math.min(tileSize, resizedWidth - left);

        int top = Math.max(0, resizedHeight - (tmsRow + 1) * tileSize);
        int bottom = resizedHeight - tmsRow * tileSize;
        int height = bottom - top;

        MemorySegment tileOutPtr = arena.allocate(ValueLayout.ADDRESS);
        int cropResult = vips.vipsCrop(levelImage, tileOutPtr, left, top, width, height);
        if (cropResult != 0) {
            String error = vips.vipsErrorBuffer();
            vips.vipsErrorClear();
            throw new IOException("vips_crop failed at level " + level + " tile " + col + "/" + tmsRow + ": " + error);
        }

        MemorySegment tileImage = tileOutPtr.get(ValueLayout.ADDRESS, 0);
        try {
            ByteSink tileSink = columnSink.getTileSink(tmsRow);
            writeTile(tileImage, tileSink, width, height, arena);
        } finally {
            vips.gObjectUnref(tileImage);
        }
    }

    private MemorySegment materializeLevelImage(MemorySegment resizedImage, Arena arena, int level) throws Throwable {
        // Prefer the dedicated vips_copy_memory() — no varargs, stable across all libvips versions.
        MemorySegment materialized = vips.vipsCopyMemory(resizedImage);
        if (materialized != null && materialized.address() != 0) {
            vips.gObjectUnref(resizedImage);
            return materialized;
        }

        // Fallback: force evaluation via a dummy write-to-buffer encode pass.
        forceEvaluateImage(resizedImage, arena, level);
        return resizedImage;
    }

    private void forceEvaluateImage(MemorySegment image, Arena arena, int level) throws IOException {
        try {
            MemorySegment bufPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment lenPtr = arena.allocate(ValueLayout.JAVA_LONG);
            int saveResult = vips.vipsImageWriteToBuffer(image, bufPtr, lenPtr, encodeSuffix);
            if (saveResult != 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to force level materialization at level " + level + ": " + error);
            }
            MemorySegment buf = bufPtr.get(ValueLayout.ADDRESS, 0);
            vips.gFree(buf);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException("Failed to force level materialization at level " + level, e);
        }
    }

    private void writeTile(MemorySegment tileImage, ByteSink tileSink, int width, int height, Arena arena) throws IOException {
        try {
            MemorySegment outputImage = prepareImageForOutput(tileImage, width, height, arena);
            try {
                try (OutputStreamVipsTargetFFM target = new OutputStreamVipsTargetFFM(vips, tileSink.openStream())) {
                    if (vips.vipsImageWriteToTarget(outputImage, encodeSuffix, target.getTarget()) != 0) {
                        String error = vips.vipsErrorBuffer();
                        vips.vipsErrorClear();
                        throw new IOException("vips_image_write_to_target failed: " + error);
                    }
                }
            } finally {
                if (outputImage != null && outputImage.address() != 0 && !outputImage.equals(tileImage)) {
                    vips.gObjectUnref(outputImage);
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to write tile via target", t);
        }
    }

    private MemorySegment prepareImageForOutput(MemorySegment tileImage, int width, int height, Arena arena) throws IOException {
        try {
            if (!TilePadding.requiresPadding(padTiles, tileSize, width, height)) {
                return tileImage;
            }

            MemorySegment paddedInput = tileImage;
            MemorySegment alphaImage = null;
            if (tileFormat == TileFormat.PNG && vips.vipsImageHasAlpha(tileImage) == 0) {
                MemorySegment alphaPtr = arena.allocate(ValueLayout.ADDRESS);
                if (vips.vipsAddAlpha(tileImage, alphaPtr) != 0) {
                    String error = vips.vipsErrorBuffer();
                    vips.vipsErrorClear();
                    throw new IOException("vips_addalpha failed: " + error);
                }
                alphaImage = alphaPtr.get(ValueLayout.ADDRESS, 0);
                paddedInput = alphaImage;
            }

            MemorySegment background = null;
            try {
                background = vips.vipsArrayDoubleNew(resolveBackgroundArray());
                MemorySegment paddedPtr = arena.allocate(ValueLayout.ADDRESS);
                if (vips.vipsEmbed(paddedInput, paddedPtr, 0, tileSize - height, tileSize, tileSize, 5, background) != 0) {
                    String error = vips.vipsErrorBuffer();
                    vips.vipsErrorClear();
                    throw new IOException("vips_embed failed: " + error);
                }
                return paddedPtr.get(ValueLayout.ADDRESS, 0);
            } finally {
                if (background != null && background.address() != 0) {
                    vips.vipsAreaUnref(background);
                }
                if (alphaImage != null && alphaImage.address() != 0) {
                    vips.gObjectUnref(alphaImage);
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("Failed to prepare tile image", t);
        }
    }

    private double[] resolveBackgroundArray() {
        if (tileFormat == TileFormat.PNG) {
            return new double[]{0d, 0d, 0d, 0d};
        }
        return new double[]{
                tileBackgroundColor.getRed(),
                tileBackgroundColor.getGreen(),
                tileBackgroundColor.getBlue()
        };
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
