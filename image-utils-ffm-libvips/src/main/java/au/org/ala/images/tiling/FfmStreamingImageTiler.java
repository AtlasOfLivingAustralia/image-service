package au.org.ala.images.tiling;

import au.org.ala.images.ffm.InputStreamVipsSourceFFM;
import au.org.ala.images.ffm.NativeLibraryDetectorFFM;
import au.org.ala.images.ffm.VipsLibraryFFM;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * FFM-based tiler that uses libvips directly via the Foreign Function & Memory API.
 */
public class FfmStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(FfmStreamingImageTiler.class);

    private final VipsLibraryFFM vips;
    private final IImageTiler fallbackTiler;
    private final int tileSize;
    private final ZoomFactorStrategy zoomFactorStrategy;
    private final Executor ioExecutor;
    private final Executor levelExecutor;

    public FfmStreamingImageTiler(IImageTiler fallbackTiler, ImageTilerConfig config) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = config.getTileSize();
        this.ioExecutor = config.getIoExecutor();
        this.levelExecutor = config.getLevelExecutor();
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary();
        this.zoomFactorStrategy = config.getZoomFactorStrategy();

        if (vips != null) {
            log.info("FfmStreamingImageTiler initialized with native libvips (FFM)");
        } else {
            log.info("FfmStreamingImageTiler: libvips not available, will use fallback");
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
            imageInputStream.mark(10 * 1024 * 1024); // 10MB mark
        }

        try {
            return tileLevelWithVipsFFM(imageInputStream, tilerSink, level);
        } catch (Exception e) {
            log.error("FFM single-level tiling failed, trying fallback", e);
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

    private ImageTilerResults tileLevelWithVipsFFM(InputStream imageInputStream, TilerSink tilerSink, int level) throws IOException, InterruptedException {
        InputStreamVipsSourceFFM vipsSource = null;
        MemorySegment inputImage = null;
        MemorySegment resizedImage = null;

        try (Arena arena = Arena.ofShared()) {
            vipsSource = new InputStreamVipsSourceFFM(vips, imageInputStream);
            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "");
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to load image from source: " + error);
            }

            int width = vips.vipsImageGetWidth(inputImage);
            int height = vips.vipsImageGetHeight(inputImage);

            zoomFactorStrategy.getZoomFactors(width, height);

            int maxLevel = (int) Math.ceil(Math.log(Math.max(width, height) / (double) tileSize) / Math.log(2));
            double scale = Math.pow(0.5, maxLevel - level);

            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            int result = vips.vipsResize(inputImage, outPtr, scale);
            if (result != 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("vips_resize failed: " + error);
            }
            resizedImage = outPtr.get(ValueLayout.ADDRESS, 0);

            int resizedWidth = vips.vipsImageGetWidth(resizedImage);
            int resizedHeight = vips.vipsImageGetHeight(resizedImage);

            int cols = (int) Math.ceil(resizedWidth / (double) tileSize);
            int rows = (int) Math.ceil(resizedHeight / (double) tileSize);

            log.debug("Tiling level {}: {}x{} (scale {}), {}x{} tiles", level, resizedWidth, resizedHeight, scale, cols, rows);

            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            AtomicBoolean errorOccurred = new AtomicBoolean(false);

            for (int x = 0; x < cols; x++) {
                final int col = x;
                TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, rows);

                for (int y = 0; y < rows; y++) {
                    final int row = y;
                    if (errorOccurred.get()) break;

                    final MemorySegment finalResizedImage = resizedImage;
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        if (errorOccurred.get()) return;

                        try (Arena threadArena = Arena.ofConfined()) {
                            int left = col * tileSize;
                            int top = row * tileSize;
                            int w = Math.min(tileSize, resizedWidth - left);
                            int h = Math.min(tileSize, resizedHeight - top);

                            MemorySegment tileOutPtr = threadArena.allocate(ValueLayout.ADDRESS);
                            int cropResult = vips.vipsCrop(finalResizedImage, tileOutPtr, left, top, w, h);
                            if (cropResult != 0) {
                                throw new IOException("vips_crop failed");
                            }
                            MemorySegment tileImage = tileOutPtr.get(ValueLayout.ADDRESS, 0);

                            try {
                                MemorySegment bufPtr = threadArena.allocate(ValueLayout.ADDRESS);
                                MemorySegment lenPtr = threadArena.allocate(ValueLayout.JAVA_LONG);
                                int saveResult = vips.vipsImageWriteToBuffer(tileImage, bufPtr, lenPtr, ".png");
                                if (saveResult != 0) {
                                    throw new IOException("vips_image_write_to_buffer failed");
                                }

                                MemorySegment buf = bufPtr.get(ValueLayout.ADDRESS, 0);
                                long len = lenPtr.get(ValueLayout.JAVA_LONG, 0);
                                byte[] data = buf.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
                                vips.gFree(buf);

                                ByteSink tileSink = columnSink.getTileSink(row);
                                tileSink.write(data);
                            } finally {
                                vips.gObjectUnref(tileImage);
                            }
                        } catch (Throwable e) {
                            log.error("Error generating tile {}/{}", col, row, e);
                            errorOccurred.set(true);
                        }
                    }, levelExecutor);
                    futures.add(future);
                }
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            if (errorOccurred.get()) {
                throw new IOException("One or more tiles failed to generate");
            }

            return new ImageTilerResults(true, maxLevel + 1);

        } catch (Throwable e) {
            if (e instanceof IOException) throw (IOException) e;
            if (e instanceof InterruptedException) throw (InterruptedException) e;
            throw new IOException("Error tiling image with FFM", e);
        } finally {
            try {
                if (resizedImage != null && resizedImage.address() != 0) vips.gObjectUnref(resizedImage);
                if (inputImage != null && inputImage.address() != 0) vips.gObjectUnref(inputImage);
            } catch (Throwable t) {
                log.warn("Error cleaning up vips images", t);
            }
            if (vipsSource != null) vipsSource.close();
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
        Path tempOutDir = null;

        try {
            vipsSource = new InputStreamVipsSourceFFM(vips, imageInputStream);
            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "");
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to load image from source with libvips: " + error);
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition());

            tempOutDir = Files.createTempDirectory("tile-out-ffm-");
            File tilesBase = tempOutDir.resolve("tiles").toFile();

            int result = vips.vipsDzsave(inputImage, tilesBase.getAbsolutePath(),
                    "tile-size", tileSize,
                    "overlap", 0,
                    "suffix", ".png",
                    "depth", 1,
                    "layout", 2);
            if (result != 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("vips_dzsave failed: " + error);
            }

            File tilesDir = tilesBase;
            if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                // Some versions/layouts might still use _files suffix
                tilesDir = tempOutDir.resolve("tiles_files").toFile();
            }

            if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                throw new IOException("vips_dzsave did not create expected tiles directory: " + tilesBase.getAbsolutePath() + " or " + tilesDir.getAbsolutePath());
            }

            int maxZoomLevel = 0;
            File[] levelDirs = tilesDir.listFiles(File::isDirectory);
            if (levelDirs != null) {
                for (File levelDir : levelDirs) {
                    int level = Integer.parseInt(levelDir.getName());
                    if (level < minLevel || level > maxLevel) {
                        continue;
                    }
                    maxZoomLevel = Math.max(maxZoomLevel, level);

                    TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);

                    File[] colDirs = levelDir.listFiles(File::isDirectory);
                    if (colDirs != null) {
                        for (File colDir : colDirs) {
                            int col = Integer.parseInt(colDir.getName());
                            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, Integer.MAX_VALUE);

                            File[] rowFiles = colDir.listFiles(File::isFile);
                            if (rowFiles != null) {
                                for (File rowFile : rowFiles) {
                                    int row = Integer.parseInt(rowFile.getName().replace(".png", "").replace(".jpg", ""));
                                    ByteSink tileSink = columnSink.getTileSink(row);
                                    com.google.common.io.Files.asByteSource(rowFile).copyTo(tileSink);
                                }
                            }
                        }
                    }
                }
            }

            return new ImageTilerResults(true, maxZoomLevel + 1);

        } catch (Throwable e) {
            if (e instanceof IOException) throw (IOException) e;
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
            if (tempOutDir != null) {
                deleteDirectory(tempOutDir);
            }
        }
    }

    private void deleteDirectory(Path path) {
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        } catch (IOException e) {
            log.warn("Failed to delete temp directory: " + path, e);
        }
    }
}
