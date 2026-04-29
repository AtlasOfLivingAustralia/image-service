package au.org.ala.images.vipsffm;

import app.photofox.vipsffm.VImage;
import app.photofox.vipsffm.Vips;
import app.photofox.vipsffm.VipsOption;
import app.photofox.vipsffm.enums.VipsCompassDirection;
import app.photofox.vipsffm.enums.VipsExtend;
import au.org.ala.images.tiling.IImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import au.org.ala.images.tiling.ImageTilerResults;
import au.org.ala.images.tiling.TileFormat;
import au.org.ala.images.tiling.TilerSink;
import au.org.ala.images.tiling.ZoomFactorStrategy;
import com.google.common.io.ByteSink;
import java.awt.Color;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alternate FFM-based tiler that uses the lopcode/vips-ffm library.
 */
public class VipsFfmStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(VipsFfmStreamingImageTiler.class);

    private static final boolean PHASE_TIMING_ENABLED = readFlag("tiler.photofox.phaseTiming", "TILER_PHOTOFOX_PHASE_TIMING");
    private static final int PARALLEL_MIN_TILES = Integer.getInteger("tiler.photofox.parallel.minTiles", 64);
    private static final int MAX_PARALLEL_WORKERS = Integer.getInteger("tiler.photofox.parallel.maxWorkers", 2);

    private final IImageTiler fallbackTiler;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final Color tileBackgroundColor;
    private final ZoomFactorStrategy zoomFactorStrategy;
    /** Executor used to parallelise per-tile encode+write operations. May be null (sequential fallback). */
    private final Executor ioExecutor;
    private final boolean sameThreadExecutor;
    private final int vipsConcurrency;
    /** Encode suffix passed to VImage.writeToStream(), e.g. ".jpg" or ".png". */
    private final String encodeSuffix;
    private final boolean padTiles;

    private static final class LevelPhaseMetrics {
        final long levelStartNanos = System.nanoTime();
        final LongAdder tileCount = new LongAdder();
        final LongAdder extractNanos = new LongAdder();
        final LongAdder encodeWallNanos = new LongAdder();
        final LongAdder sinkWriteNanos = new LongAdder();
        final LongAdder sinkOpenCloseNanos = new LongAdder();

        void logLevelSummary(int level, int cols, int rows, boolean parallel, int workers) {
            if (!PHASE_TIMING_ENABLED) {
                return;
            }
            long totalNanos = System.nanoTime() - levelStartNanos;
            long extractMs = nanosToMs(extractNanos.sum());
            long encodeWallMs = nanosToMs(encodeWallNanos.sum());
            long sinkWriteMs = nanosToMs(sinkWriteNanos.sum());
            long sinkOpenCloseMs = nanosToMs(sinkOpenCloseNanos.sum());
            long encodeCpuMs = Math.max(0L, encodeWallMs - sinkWriteMs);
            long totalMs = nanosToMs(totalNanos);
            System.out.println(
                "[BENCHMARK][PHOTOFOX_PHASE] level=" + level +
                " tiles=" + tileCount.sum() +
                " grid=" + cols + "x" + rows +
                " parallel=" + parallel +
                " workers=" + workers +
                " totalMs=" + totalMs +
                " extractMs=" + extractMs +
                " encodeWallMs=" + encodeWallMs +
                " encodeCpuMs=" + encodeCpuMs +
                " sinkWriteMs=" + sinkWriteMs +
                " sinkOpenCloseMs=" + sinkOpenCloseMs
            );
            log.info(
                "[BENCHMARK][PHOTOFOX_PHASE] level={} tiles={} grid={}x{} parallel={} workers={} totalMs={} extractMs={} encodeWallMs={} encodeCpuMs={} sinkWriteMs={} sinkOpenCloseMs={}",
                level,
                tileCount.sum(),
                cols,
                rows,
                parallel,
                workers,
                totalMs,
                extractMs,
                encodeWallMs,
                encodeCpuMs,
                sinkWriteMs,
                sinkOpenCloseMs
            );
        }

        private static long nanosToMs(long nanos) {
            return Math.floorDiv(nanos, 1_000_000L);
        }
    }

    private static final class TimedOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final LongAdder sinkWriteNanos;

        TimedOutputStream(OutputStream delegate, LongAdder sinkWriteNanos) {
            this.delegate = delegate;
            this.sinkWriteNanos = sinkWriteNanos;
        }

        @Override
        public void write(int b) throws IOException {
            long t0 = System.nanoTime();
            try {
                delegate.write(b);
            } finally {
                sinkWriteNanos.add(System.nanoTime() - t0);
            }
        }

        @Override
        public void write(byte[] b) throws IOException {
            long t0 = System.nanoTime();
            try {
                delegate.write(b);
            } finally {
                sinkWriteNanos.add(System.nanoTime() - t0);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            long t0 = System.nanoTime();
            try {
                delegate.write(b, off, len);
            } finally {
                sinkWriteNanos.add(System.nanoTime() - t0);
            }
        }

        @Override
        public void flush() throws IOException {
            long t0 = System.nanoTime();
            try {
                delegate.flush();
            } finally {
                sinkWriteNanos.add(System.nanoTime() - t0);
            }
        }
    }

    public VipsFfmStreamingImageTiler(IImageTiler fallbackTiler, ImageTilerConfig config) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = config.getTileSize();
        this.tileFormat = config.getTileFormat();
        this.tileBackgroundColor = config.getTileBackgroundColor();
        this.zoomFactorStrategy = config.getZoomFactorStrategy();
        this.ioExecutor = config.getIoExecutor();
        this.sameThreadExecutor = isSameThreadExecutor(this.ioExecutor);
        this.vipsConcurrency = config.getVipsConcurrency();
        this.encodeSuffix = config.getTileFormat() == TileFormat.PNG ? ".png" : ".jpg";
        this.padTiles = config.isPadTiles();
        applyVipsConcurrencyIfConfigured();
        log.info(
            "VipsFfmStreamingImageTiler initialized (lopcode/vips-ffm), ioExecutor={}, sameThreadExecutor={}",
            ioExecutor != null ? ioExecutor : "none (sequential)",
            sameThreadExecutor
        );
    }

    private void applyVipsConcurrencyIfConfigured() {
        if (vipsConcurrency <= 0) {
            return;
        }
        try {
            Method method = Vips.class.getMethod("concurrencySet", int.class);
            method.invoke(null, vipsConcurrency);
            log.info("Set libvips concurrency to {} (photofox)", vipsConcurrency);
        } catch (Throwable t) {
            log.warn(
                "Failed to set libvips concurrency to {} (photofox) - method not available in this version",
                vipsConcurrency,
                t
            );
        }
    }

    private VImage materializeLevelImage(VImage image) {
        try {
            // Prefer forcing pixel data in memory for shared reads across many tile crops.
            return image.copy(VipsOption.Boolean("memory", true));
        } catch (Throwable ignored) {
            try {
                return image.copy();
            } catch (Throwable ignoredToo) {
                // Fallback to lazy image if copy options are unavailable.
                return image;
            }
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int level)
        throws IOException, InterruptedException {
        log.debug("Tiling image for level {} with vips-ffm", level);

        if (level < 0) {
            throw new IllegalArgumentException("Invalid level: " + level);
        }

        if (imageInputStream.markSupported()) {
            imageInputStream.mark(10 * 1024 * 1024);
        }

        try {
            return tileLevelWithVipsFfm(imageInputStream, tilerSink, level);
        } catch (Exception e) {
            log.error("vips-ffm single-level tiling failed, trying fallback", e);
            if (fallbackTiler == null) {
                if (e instanceof IOException ioException) {
                    throw ioException;
                }
                if (e instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new IOException("vips-ffm single-level tiling failed", e);
            }
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset();
                } catch (IOException resetException) {
                    log.warn("Failed to reset stream", resetException);
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, level);
        }
    }

    private ImageTilerResults tileLevelWithVipsFfm(InputStream imageInputStream, TilerSink tilerSink, int level)
        throws IOException {
        try (Arena arena = Arena.ofShared()) {
            VImage image = VImage.newFromStream(arena, imageInputStream);
            int width = image.getWidth();
            int height = image.getHeight();

            int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width);
            int zoomLevels = pyramid.length;
            int maxLevel = zoomLevels - 1;

            if (level > maxLevel) {
                log.warn("Requested level {} is higher than maxLevel {}", level, maxLevel);
                return new ImageTilerResults(true, zoomLevels);
            }

            generateLevelTiles(image, tilerSink.getLevelSink(level), level, pyramid[level]);
            return new ImageTilerResults(true, zoomLevels);
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel)
        throws IOException, InterruptedException {
        log.debug("Tiling image with vips-ffm: minLevel={}, maxLevel={}", minLevel, maxLevel);

        if (imageInputStream.markSupported()) {
            imageInputStream.mark(Integer.MAX_VALUE);
        }
        try {
            return tileWithVipsFfm(imageInputStream, tilerSink, minLevel, maxLevel);
        } catch (Exception e) {
            log.error("vips-ffm tiling failed, trying fallback", e);
            if (fallbackTiler == null) {
                if (e instanceof IOException ioException) {
                    throw ioException;
                }
                if (e instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new IOException("vips-ffm tiling failed", e);
            }
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset();
                } catch (IOException resetException) {
                    log.warn(
                        "Failed to reset image input stream after vips-ffm tiling failure; proceeding with fallback tiler",
                        resetException
                    );
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }
    }

    private ImageTilerResults tileWithVipsFfm(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel)
        throws IOException {
        // ofShared() is required here: parallel tile writes may run on executor threads
        // different from the thread that opened this arena.
        try (Arena arena = Arena.ofShared()) {
            VImage image = VImage.newFromStream(arena, imageInputStream);
            int[] pyramid = zoomFactorStrategy.getZoomFactors(image.getHeight(), image.getWidth());
            int zoomLevels = pyramid.length;
            int finalMaxLevel = Math.min(maxLevel, zoomLevels - 1);

            if (minLevel > finalMaxLevel) {
                log.debug(
                    "tileWithVipsFfm: asked for levels {} to {}, but only {} levels available",
                    minLevel,
                    maxLevel,
                    zoomLevels
                );
                return new ImageTilerResults(true, zoomLevels);
            }

            for (int level = minLevel; level <= finalMaxLevel; level++) {
                generateLevelTiles(image, tilerSink.getLevelSink(level), level, pyramid[level]);
            }

            return new ImageTilerResults(true, zoomLevels);
        }
    }

    private void generateLevelTiles(VImage image, TilerSink.LevelSink levelSink, int level, int subsample) throws IOException {
        LevelPhaseMetrics metrics = new LevelPhaseMetrics();
        double scale = 1.0d / (double) subsample;
        VImage resizedImage = image.resize(scale);
        int resizedWidth = resizedImage.getWidth();
        int resizedHeight = resizedImage.getHeight();

        int cols = (int) Math.ceil(resizedWidth / (double) tileSize);
        int rows = (int) Math.ceil(resizedHeight / (double) tileSize);
        int workers = computeParallelWorkers(cols, rows);
        boolean parallelFanOut = workers > 1;

        if (shouldMaterializeLevel(cols, rows, parallelFanOut)) {
            resizedImage = materializeLevelImage(resizedImage);
            resizedWidth = resizedImage.getWidth();
            resizedHeight = resizedImage.getHeight();
        }

        log.debug("Tiling level {}: {}x{} (subsample {}), {}x{} tiles", level, resizedWidth, resizedHeight, subsample, cols, rows);

        if (parallelFanOut) {
            AtomicBoolean errorOccurred = new AtomicBoolean(false);
            AtomicReference<Exception> firstError = new AtomicReference<>(null);
            List<CompletableFuture<Void>> futures = new ArrayList<>(workers);
            final VImage levelImage = resizedImage;
            final int levelWidth = resizedWidth;
            final int levelHeight = resizedHeight;

            for (int worker = 0; worker < workers; worker++) {
                final int workerIndex = worker;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    if (errorOccurred.get()) {
                        return;
                    }
                    for (int col = workerIndex; col < cols; col += workers) {
                        if (errorOccurred.get()) {
                            return;
                        }
                        try {
                            processColumn(levelImage, levelSink, level, col, rows, levelWidth, levelHeight, metrics, errorOccurred);
                        } catch (Exception e) {
                            errorOccurred.set(true);
                            firstError.compareAndSet(null, e);
                            log.error("Error generating tile column level {}/{}", level, col, e);
                            return;
                        }
                    }
                }, ioExecutor);
                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            if (errorOccurred.get()) {
                Exception e = firstError.get();
                if (e instanceof IOException ioException) {
                    throw ioException;
                }
                throw new IOException("One or more tiles failed to generate at level " + level, e);
            }
        } else {
            for (int col = 0; col < cols; col++) {
                processColumn(resizedImage, levelSink, level, col, rows, resizedWidth, resizedHeight, metrics, null);
            }
        }

        metrics.logLevelSummary(level, cols, rows, parallelFanOut, workers);
    }

    private void processColumn(
        VImage resizedImage,
        TilerSink.LevelSink levelSink,
        int level,
        int col,
        int rows,
        int resizedWidth,
        int resizedHeight,
        LevelPhaseMetrics metrics,
        AtomicBoolean errorOccurred
    ) throws IOException {
        TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, 1);
        int left = col * tileSize;
        int width = Math.min(tileSize, resizedWidth - left);

        for (int tmsRow = 0; tmsRow < rows; tmsRow++) {
            if (errorOccurred != null && errorOccurred.get()) {
                return;
            }
            int top = Math.max(0, resizedHeight - (tmsRow + 1) * tileSize);
            int bottom = resizedHeight - tmsRow * tileSize;
            int height = bottom - top;

            try {
                long tExtract0 = System.nanoTime();
                VImage tileImage = resizedImage.extractArea(left, top, width, height);
                metrics.extractNanos.add(System.nanoTime() - tExtract0);
                metrics.tileCount.increment();

                ByteSink tileSink = columnSink.getTileSink(tmsRow);
                writeTile(tileImage, tileSink, width, height, metrics);
            } catch (Exception e) {
                log.error("Error generating tile level {}/{}/{}", level, col, tmsRow, e);
                throw new IOException("One or more tiles failed to generate at level " + level, e);
            }
        }
    }

    private void writeTile(VImage tileImage, ByteSink tileSink, int width, int height, LevelPhaseMetrics metrics) throws IOException {
        try {
            VImage outputImage = prepareImageForOutput(tileImage, width, height);
            long tOpen0 = System.nanoTime();
            OutputStream outputStream = tileSink.openStream();
            metrics.sinkOpenCloseNanos.add(System.nanoTime() - tOpen0);
            long tEncode0 = System.nanoTime();
            TimedOutputStream timed = new TimedOutputStream(outputStream, metrics.sinkWriteNanos);
            try {
                outputImage.writeToStream(timed, encodeSuffix);
            } finally {
                long tClose0 = System.nanoTime();
                timed.close();
                metrics.sinkOpenCloseNanos.add(System.nanoTime() - tClose0);
            }
            metrics.encodeWallNanos.add(System.nanoTime() - tEncode0);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to encode tile", e);
        }
    }

    private VImage prepareImageForOutput(VImage tileImage, int width, int height) throws Exception {
        if (!padTiles || (width == tileSize && height == tileSize)) {
            return tileImage;
        }

        VImage paddedInput = tileImage;
        if (tileFormat == TileFormat.PNG && !tileImage.hasAlpha()) {
            paddedInput = tileImage.bandjoinConst(List.of(0d));
        }

        return paddedInput.gravity(
            VipsCompassDirection.COMPASS_DIRECTION_SOUTH_WEST,
            tileSize,
            tileSize,
            VipsOption.Enum("extend", VipsExtend.EXTEND_BACKGROUND),
            VipsOption.ArrayDouble("background", backgroundValues())
        );
    }

    private List<Double> backgroundValues() {
        if (tileFormat == TileFormat.PNG) {
            return List.of(0d, 0d, 0d, 0d);
        }
        return List.of(
            (double) tileBackgroundColor.getRed(),
            (double) tileBackgroundColor.getGreen(),
            (double) tileBackgroundColor.getBlue()
        );
    }

    private boolean shouldMaterializeLevel(int cols, int rows, boolean parallelFanOut) {
        int tileCount = cols * rows;
        return parallelFanOut || tileCount >= PARALLEL_MIN_TILES;
    }

    private int computeParallelWorkers(int cols, int rows) {
        if (!shouldUseParallelFanOut(cols, rows)) {
            return 1;
        }
        return Math.max(1, Math.min(cols, MAX_PARALLEL_WORKERS));
    }

    private boolean shouldUseParallelFanOut(int cols, int rows) {
        return ioExecutor != null && !sameThreadExecutor && cols * rows > 1;
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
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private static boolean readFlag(String propertyName, String envName) {
        String prop = System.getProperty(propertyName);
        if (prop != null) {
            return Boolean.parseBoolean(prop);
        }
        String env = System.getenv(envName);
        if (env != null) {
            return Boolean.parseBoolean(env);
        }
        return false;
    }
}
