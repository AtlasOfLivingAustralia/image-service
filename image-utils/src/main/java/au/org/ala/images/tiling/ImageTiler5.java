package au.org.ala.images.tiling;

import au.org.ala.images.util.DefaultImageReaderSelectionStrategy;
import com.google.common.io.ByteSink;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.spi.IIORegistry;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.RasterFormatException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Memory-optimized tiler implementation that writes tiles immediately with strict sequential processing.
 */
public class ImageTiler5 implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(ImageTiler5.class);
    public static final int SLICE_SIZE = 8192;

    private final int _tileSize;
    private final TileFormat _tileFormat;
    private final Color _tileBackgroundColor;
    private final ZoomFactorStrategy _zoomFactorStrategy;

    private final Executor levelThreadPool;
    private final Executor ioThreadPool;

    private static final GraphicsEnvironment GRAPHICS_ENV =
            GraphicsEnvironment.getLocalGraphicsEnvironment();

    public ImageTiler5(ImageTilerConfig config) {
        if (config != null) {
            ioThreadPool = config.getIoExecutor();
            levelThreadPool = config.getLevelExecutor();
            _tileSize = config.getTileSize();
            _tileFormat = config.getTileFormat();
            _tileBackgroundColor = config.getTileBackgroundColor();
            _zoomFactorStrategy = config.getZoomFactorStrategy();

            // Warn if using the same executor instance for both pools
            // This can cause priority inversion in single-threaded scenarios
            if (ioThreadPool == levelThreadPool && ioThreadPool != null) {
                log.warn("Same executor instance used for both levelThreadPool and ioThreadPool. " +
                        "This may cause priority inversion where I/O tasks queue behind compute tasks, " +
                        "leading to increased memory usage. Consider using separate executor instances " +
                        "for single-threaded scenarios, or use virtual threads for massive parallelism.");
            }
        } else {
            ioThreadPool = null;
            levelThreadPool = null;
            _tileSize = 256;
            _tileFormat = TileFormat.JPEG;
            _tileBackgroundColor = Color.gray;
            _zoomFactorStrategy = new DefaultZoomFactorStrategy(_tileSize);
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        try {
            int zoomLevels = startTiling(imageInputStream, tilerSink, minLevel, maxLevel);
            return new ImageTilerResults(true, zoomLevels);
        } catch (Exception e) {
            log.error("Tiling operation failed", e);
            return new ImageTilerResults(false, 0);
        }
    }

    private int startTiling(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        log.debug("tileImage");

        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        // Read image bytes once
        byte[] imageBytes;
        try (var inputStream = imageInputStream) {
            imageBytes = IOUtils.toByteArray(inputStream);
        }

        // Get image dimensions and calculate pyramid
        Point dimensions = getImageDimensions(imageBytes);
        int[] pyramid = _zoomFactorStrategy.getZoomFactors(dimensions.y, dimensions.x);
        int zoomLevels = pyramid.length;

        final int finalMaxLevel = Math.min(maxLevel, zoomLevels - 1);

        if (minLevel > finalMaxLevel) {
            log.debug("tileImage: asked for levels {} to {}, but only {} levels available", minLevel, maxLevel, zoomLevels);
            return zoomLevels;
        }

        // Determine which levels need full-image processing vs slice-based processing
        int extremeZoomThreshold = findExtremeZoomThreshold(pyramid, dimensions);

        log.debug("tileImage: extreme zoom threshold is level {}, total levels: {}", extremeZoomThreshold, zoomLevels);

        // Process extreme and normal levels SEQUENTIALLY for strict memory control
        CompletableFuture<Void> allProcessing = CompletableFuture.completedFuture(null);

        // Process extreme zoom levels (if any) - read full image with subsampling
        if (minLevel <= extremeZoomThreshold && extremeZoomThreshold < zoomLevels) {
            int extremeMaxLevel = Math.min(extremeZoomThreshold, finalMaxLevel);
            log.debug("tileImage: processing extreme zoom levels {} to {} with full-image approach", minLevel, extremeMaxLevel);
            allProcessing = allProcessing.thenCompose(ignored ->
                processExtremeZoomLevelsStreaming(imageBytes, dimensions, pyramid, minLevel, extremeMaxLevel, tilerSink));
        }

        // Process normal zoom levels with streaming slice-based approach
        int normalMinLevel = Math.max(minLevel, extremeZoomThreshold + 1);
        if (normalMinLevel <= finalMaxLevel) {
            log.debug("tileImage: processing normal zoom levels {} to {} with streaming slice-based approach (sequential after extreme)", normalMinLevel, finalMaxLevel);
            final int finalNormalMinLevel = normalMinLevel;
            allProcessing = allProcessing.thenCompose(ignored ->
                processNormalZoomLevelsStreaming(imageBytes, dimensions, pyramid, finalNormalMinLevel, finalMaxLevel, tilerSink));
        }

        // Wait for all processing to complete
        allProcessing.join();

        log.debug("tileImage: all tiles completed");
        return zoomLevels;
    }

    /**
     * Find the highest zoom level (most zoomed out) where slice size < tile size.
     */
    private int findExtremeZoomThreshold(int[] pyramid, Point dimensions) {
        for (int level = 0; level < pyramid.length; level++) {
            int subsample = pyramid[level];
            double sliceSizeAtLevel = (double) SLICE_SIZE / (double) subsample;

            if (sliceSizeAtLevel >= _tileSize * 2) {
                return level - 1;
            }
        }
        return pyramid.length - 1;
    }

    /**
     * Process extreme zoom levels with streaming - parallelize across levels.
     * Memory usage controlled by executor configuration.
     * Returns a future that completes when all extreme levels are processed and resources are cleaned up.
     */
    private CompletableFuture<Void> processExtremeZoomLevelsStreaming(byte[] imageBytes, Point dimensions, int[] pyramid,
                                                   int minLevel, int maxLevel, TilerSink tilerSink) {
        UnsynchronizedByteArrayInputStream bais;
        try {
            bais = UnsynchronizedByteArrayInputStream.builder()
                    .setByteArray(imageBytes)
                    .setOffset(0)
                    .get();
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }

        ImageInputStream iis;
        try {
            iis = ImageIO.createImageInputStream(bais);
            if (iis == null) {
                return CompletableFuture.failedFuture(new IOException("Failed to create ImageInputStream"));
            }
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }

        try {
            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                try {
                    iis.close();
                } catch (IOException e) {
                    log.warn("Error closing ImageInputStream", e);
                }
                return CompletableFuture.failedFuture(new IOException("No image readers for image"));
            }

            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                try {
                    iis.close();
                } catch (IOException e) {
                    log.warn("Error closing ImageInputStream", e);
                }
                return CompletableFuture.failedFuture(new IOException("No suitable image reader selected"));
            }

            reader.setInput(iis, true, false);

            // Process extreme levels SEQUENTIALLY for memory control
            CompletableFuture<Void> levelChain = CompletableFuture.completedFuture(null);

            for (int level = minLevel; level <= maxLevel; level++) {
                final int finalLevel = level;
                final int subsample = pyramid[level];

                // Chain each level sequentially
                levelChain = levelChain.thenCompose(ignored -> {
                    log.debug("Processing extreme zoom level {} with subsample {}", finalLevel, subsample);
                    return processFullImageLevelStreaming(reader, dimensions, subsample, tilerSink.getLevelSink(finalLevel));
                });
            }

            // Wait for all extreme levels to complete, then cleanup
            CompletableFuture<Void> allLevels = levelChain;

            // Chain resource cleanup after all levels complete
            return allLevels.whenComplete((result, error) -> {
                if (error != null) {
                    log.error("Error processing extreme zoom levels", error);
                }
                reader.dispose();
                try {
                    iis.close();
                } catch (IOException e) {
                    log.warn("Error closing ImageInputStream", e);
                }
            });

        } catch (Exception e) {
            try {
                iis.close();
            } catch (IOException ex) {
                log.warn("Error closing ImageInputStream", ex);
            }
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Process a single zoom level using the full image - write tiles immediately.
     * Note: NOT synchronized at method level to allow parallel extreme level processing.
     * Only the actual reader.read() call is synchronized.
     * Returns a future that completes when all tiles are written and image is flushed.
     */
    private CompletableFuture<Void> processFullImageLevelStreaming(ImageReader reader, Point dimensions,
                                                             int subsample, TilerSink.LevelSink levelSink) {
        log.debug("processFullImageLevelStreaming: subsample={}", subsample);

        BufferedImage fullImage;

        // Synchronize only the actual read operation - not the entire method!
        try {
            synchronized (reader) {
                ImageReadParam params = reader.getDefaultReadParam();
                params.setSourceSubsampling(subsample, subsample, 0, 0);
                fullImage = reader.read(0, params);
            }
        } catch (IOException e) {
            log.error("Error reading image at subsample {}", subsample, e);
            return CompletableFuture.failedFuture(e);
        }

        log.debug("processFullImageLevelStreaming: read image {}x{}", fullImage.getWidth(), fullImage.getHeight());

        // Split and write tiles immediately (happens in parallel across levels now!)
        CompletableFuture<Void> allTiles = splitAndWriteTilesImmediately(fullImage, levelSink);

        // Chain: image is flushed by splitAndWriteTilesImmediately after all tiles are done generating
        return allTiles;
    }

    /**
     * Split a full image into tiles using levelThreadPool for generation,
     * streaming results to ioThreadPool for writing. No accumulation.
     * Returns a future that completes when all tiles are generated and written.
     */
    private CompletableFuture<Void> splitAndWriteTilesImmediately(BufferedImage image, TilerSink.LevelSink levelSink) {
        int cols = (int) Math.ceil((double) image.getWidth() / _tileSize);
        int rows = (int) Math.ceil((double) image.getHeight() / _tileSize);

        int imageHeight = image.getHeight();
        int imageWidth = image.getWidth();

        log.debug("splitAndWriteTilesImmediately: {}x{} image -> {}x{} tiles (parallel generation + streaming writes)",
                  imageWidth, imageHeight, cols, rows);

        // Generate tiles on levelThreadPool, stream writes to ioThreadPool
        List<CompletableFuture<Void>> allFutures = new ArrayList<>(cols * rows);

        for (int col = 0; col < cols; col++) {
            final int finalCol = col;
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(finalCol, 0, 1);

            for (int row = rows - 1; row >= 0; row--) {
                final int finalRow = row;

                // Generate tile on levelThreadPool
                CompletableFuture<TileGenerationResult> tileGeneration = CompletableFuture.supplyAsync(() -> {
                    int x = finalCol * _tileSize;
//                    int y = finalRow * _tileSize;
                    int tw = Math.min(_tileSize, imageWidth - x);
//                    int th = Math.min(_tileSize, image.getHeight() - y);
                    int th;
                    int y;
                    if ((finalRow + 1) * _tileSize > imageHeight) {
                        y = 0;
                        th = imageHeight - (finalRow * _tileSize);
                    } else {
                        y = imageHeight - (finalRow + 1) * _tileSize;
                        th = _tileSize;
                    }

                    BufferedImage tile = null;
                    if (tw > 0 && th > 0) {
                        synchronized (image) {
                            tile = image.getSubimage(x, y, tw, th);
                        }
                    }

                    BufferedImage destTile = createDestTile();
                    Graphics g = GRAPHICS_ENV.createGraphics(destTile);

                    if (_tileFormat == TileFormat.JPEG && tile != null) {
                        g.setColor(_tileBackgroundColor);
                        g.fillRect(0, 0, _tileSize, _tileSize);
                    }

                    if (tile != null) {
                        g.drawImage(tile, 0, _tileSize - th, null);
                    }

                    g.dispose();

                    ByteSink tileSink = columnSink.getTileSink(finalRow);
                    return new TileGenerationResult(tileSink, destTile);
                }, levelThreadPool);

                // Stream to ioThreadPool for writing
                CompletableFuture<Void> writeCompletion = tileGeneration.thenComposeAsync(tileResult ->
                    CompletableFuture.runAsync(() -> {
                        try {
                            writeTile(tileResult.tileSink, tileResult.image);
                        } catch (IOException e) {
                            log.error("Error writing tile {},{}", finalCol, finalRow, e);
                            throw new java.io.UncheckedIOException(e);
                        } finally {
                            tileResult.image.flush();
                        }
                    }, ioThreadPool)
                );

                allFutures.add(writeCompletion);
            }
        }

        // Wait for all writes to complete, then flush the source image
        CompletableFuture<Void> allWritesComplete = CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]));

        // Chain flush after all writes are done (tiles no longer need the source image)
        return allWritesComplete.thenRun(image::flush);
    }

    /**
     * Process normal zoom levels with streaming - sequential level processing for memory control.
     * Critical: Levels are processed sequentially (chained) to ensure only one slice is in memory at a time.
     * Combined with sequential slice processing, this guarantees O(slice_size) memory usage, not O(image_size).
     * Returns a future that completes when all normal levels are processed.
     */
    private CompletableFuture<Void> processNormalZoomLevelsStreaming(byte[] imageBytes, Point dimensions, int[] pyramid,
                                                   int minLevel, int maxLevel, TilerSink tilerSink) {

        // Process levels SEQUENTIALLY to control memory usage
        // Critical for large images: only one level active at a time = only one slice in memory at a time
        CompletableFuture<Void> levelChain = CompletableFuture.completedFuture(null);

        for (int level = minLevel; level <= maxLevel; level++) {
            final int finalLevel = level;
            final int subsample = pyramid[level];

            // Chain each level sequentially: previous level must complete before next starts
            levelChain = levelChain.thenCompose(ignored -> {
                log.debug("Processing normal zoom level {} with subsample {}", finalLevel, subsample);
                TilerSink.LevelSink levelSink = tilerSink.getLevelSink(finalLevel);

                // Process slices for this level with streaming writes (also sequential)
                return processSlicesForLevelStreaming(imageBytes, dimensions, subsample, levelSink);
            });
        }

        // Return future that completes when all levels are done
        return levelChain;
    }

    /**
     * Process all slices for a single level sequentially to control memory usage.
     * Critical: Slices are processed one at a time (chained sequentially) to prevent
     * loading all slice BufferedImages into memory simultaneously.
     * Within each slice, tiles can be generated and written in parallel based on executor configuration.
     * Returns a future that completes when all slices are processed.
     */
    private CompletableFuture<Void> processSlicesForLevelStreaming(byte[] imageBytes, Point dimensions,
                                                int subsample, TilerSink.LevelSink levelSink) {

        UnsynchronizedByteArrayInputStream bais;
        try {
            bais = UnsynchronizedByteArrayInputStream.builder()
                    .setByteArray(imageBytes)
                    .setOffset(0)
                    .get();
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }

        ImageInputStream iis;
        try {
            iis = ImageIO.createImageInputStream(bais);
            if (iis == null) {
                return CompletableFuture.failedFuture(new IOException("Failed to create ImageInputStream"));
            }
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }

        try {
            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                try {
                    iis.close();
                } catch (IOException e) {
                    log.warn("Error closing ImageInputStream", e);
                }
                return CompletableFuture.failedFuture(new IOException("No image readers for image"));
            }

            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                try {
                    iis.close();
                } catch (IOException e) {
                    log.warn("Error closing ImageInputStream", e);
                }
                return CompletableFuture.failedFuture(new IOException("No suitable image reader selected"));
            }

            reader.setInput(iis, true, false);

            try {
                int w = dimensions.x;
                int h = dimensions.y;

                var segmentSize = SLICE_SIZE;
                var xs = (int) Math.ceil((double) w / (double) segmentSize);
                var ys = (int) Math.ceil((double) h / (double) segmentSize);

                // Process slices SEQUENTIALLY to control memory usage
                // This is critical: even with parallel executors, we process one slice at a time
                // to avoid loading all slices into memory simultaneously
                CompletableFuture<Void> sliceChain = CompletableFuture.completedFuture(null);

                for (int i = 0; i < xs; i++) {
                    for (int j = ys-1; j >= 0; --j) {
                        final int finalI = i;
                        final int finalJ = j;
                        Point sliceCoords = new Point(i, j);

                        // Chain each slice sequentially: previous slice must complete before next starts
                        sliceChain = sliceChain.thenCompose(ignored -> {
                            // Read slice on ioThreadPool (I/O bound operation)
                            CompletableFuture<BufferedImage> sliceReadFuture = CompletableFuture.supplyAsync(() -> {
                                try {
                                    var params = reader.getDefaultReadParam();

                                    int rectWidth = (finalI + 1) * segmentSize > w ? w - (finalI * segmentSize) : segmentSize;
                                    int rectX = finalI * segmentSize;
                                    int rectHeight = (finalJ + 1) * segmentSize > h ? h - (finalJ * segmentSize) : segmentSize;
                                    int rectY = h - (finalJ * segmentSize) - rectHeight;;

                                    // Safety check
                                    if (rectHeight <= 0) {
                                        throw new IllegalStateException(String.format(
                                                "Invalid slice dimensions: p.y=%d, h=%d, rectY=%d, rectHeight=%d, segmentSize=%d",
                                                finalJ, h, rectY, rectHeight, segmentSize));
                                    }

                                    params.setSourceRegion(new Rectangle(rectX, rectY, rectWidth, rectHeight));

                                    // Synchronize only the actual read operation
                                    synchronized (reader) {
                                        return reader.read(0, params);
                                    }
                                } catch (Exception e) {
                                    log.error("Error reading slice {},{}", finalI, finalJ, e);
                                    throw new RuntimeException(e);
                                }
                            }, ioThreadPool);

                            // Chain: read slice (ioThreadPool) -> process slice (levelThreadPool)
                            return sliceReadFuture
                                .thenComposeAsync(slice -> processSliceAsync(slice, sliceCoords, subsample, levelSink),
                                                levelThreadPool);
                        });
                    }
                }

                // Wait for all slices to complete, then cleanup
                CompletableFuture<Void> allSlices = sliceChain;

                // Chain resource cleanup after all slices complete
                return allSlices.whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Error processing slices", error);
                    }
                    reader.dispose();
                    try {
                        iis.close();
                    } catch (IOException e) {
                        log.warn("Error closing ImageInputStream", e);
                    }
                });

            } catch (Exception e) {
                reader.dispose();
                try {
                    iis.close();
                } catch (IOException ex) {
                    log.warn("Error closing ImageInputStream", ex);
                }
                return CompletableFuture.failedFuture(e);
            }
        } catch (Exception e) {
            try {
                iis.close();
            } catch (IOException ex) {
                log.warn("Error closing ImageInputStream", ex);
            }
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Process a slice asynchronously - tiles are generated and written in true streaming fashion.
     * Each tile is written as soon as it's generated (no waiting for all tiles to complete).
     */
    private CompletableFuture<Void> processSliceAsync(BufferedImage slice, Point sliceCoords,
                                                      int subsample, TilerSink.LevelSink levelSink) {
        try {
            // Generate stream of tile generation futures (don't wait for them yet!)
            List<CompletableFuture<Void>> writeFutures =
                generateTileFuturesFromSlice(slice, sliceCoords, subsample, levelSink);

            // Wait for all tiles to be generated AND written, THEN flush slice
            CompletableFuture<Void> allTiles = CompletableFuture.allOf(writeFutures.toArray(new CompletableFuture[0]));
            return allTiles.thenRun(slice::flush);
        } catch (Exception e) {
            log.error("Error processing slice {},{}", sliceCoords.x, sliceCoords.y, e);
            slice.flush();
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Generate tile futures from a slice - returns list of futures that chain:
     * generate tile (levelThreadPool) -> write tile (ioThreadPool)
     * Each tile is written as soon as it's generated, not waiting for others.
     */
    private List<CompletableFuture<Void>> generateTileFuturesFromSlice(BufferedImage slice, Point sliceCoords,
                                                                        int subsample, TilerSink.LevelSink levelSink) {
        int srcHeight = slice.getHeight();
        int srcWidth = slice.getWidth();

        // Calculate tile position based on actual pixel positions
        int startCol;
        int startRow;
        if (SLICE_SIZE % subsample == 0) {
            int sliceSizeAtLevel = SLICE_SIZE / subsample;
            if (sliceSizeAtLevel % _tileSize == 0) {
                int maxTilesPerSliceAtLevel = sliceSizeAtLevel / _tileSize;
                startCol = sliceCoords.x * maxTilesPerSliceAtLevel;
                startRow = sliceCoords.y * maxTilesPerSliceAtLevel;
            } else if (sliceSizeAtLevel < _tileSize) {
                log.warn("slice size {} at subsample {} is smaller than tile size {}", sliceSizeAtLevel, subsample, _tileSize);
                startCol = sliceCoords.x;
                startRow = sliceCoords.y;
            } else {
                log.warn("slice size {} at subsample {} is not evenly divisible by tile size {}", sliceSizeAtLevel, subsample, _tileSize);
                double maxTilesPerSliceAtLevel = (double)sliceSizeAtLevel / (double)_tileSize;
                startCol = (int)((double)sliceCoords.x * maxTilesPerSliceAtLevel);
                startRow = (int)((double)sliceCoords.y * maxTilesPerSliceAtLevel);

            }
        } else {
            log.warn("slice size {} is not evenly divisible by subsample {}", SLICE_SIZE, subsample);
            double sliceSizeAtLevel = (double) SLICE_SIZE / (double) subsample;
            double maxTilesPerSliceAtLevel = sliceSizeAtLevel / (double) _tileSize;

            startCol = (int) ((double) sliceCoords.x * maxTilesPerSliceAtLevel);
            startRow = (int) ((double) sliceCoords.y * maxTilesPerSliceAtLevel);
        }
        int height;
        if (srcHeight % subsample == 0) {
            height = srcHeight / subsample;
        } else {
            height = (int) Math.ceil(((double) srcHeight) / ((double) subsample));
        }
        int width;
        if (srcWidth % subsample == 0) {
            width = srcWidth / subsample;
        } else {
            width = (int) Math.ceil(((double) srcWidth) / ((double) subsample));
        }
        int rows;
        if (height % _tileSize == 0) {
            rows = height / _tileSize;
        } else {
            rows = (int) Math.ceil(((double) height) / ((double) _tileSize));
        }
        int cols;
        if (width % _tileSize == 0) {
            cols = width / _tileSize;
        } else {
            cols = (int) Math.ceil(((double) width) / ((double) _tileSize));
        }

        BufferedImage resized = Scalr.resize(slice, width, height);

        double sliceSizeAtLevel = (double) SLICE_SIZE / (double) subsample;
        double maxTilesPerSliceAtLevel = sliceSizeAtLevel / (double) _tileSize;

//        int startCol = (int) ((double) sliceCoords.x * maxTilesPerSliceAtLevel);
//        int startRow = (int) ((double) sliceCoords.y * maxTilesPerSliceAtLevel);

        List<CompletableFuture<Void>> tileWriterFutures = new ArrayList<>(cols*rows);

        for (int col = 0; col < cols; col++) {
            final int stripColOffset = col * _tileSize;
            if (stripColOffset >= resized.getWidth()) {
                continue;
            }

            int actualCol = startCol + col;
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(actualCol, 0, 1);

            int tw = Math.min(_tileSize, resized.getWidth() - stripColOffset);
            final int finalTw = tw;

            for (int y = rows - 1; y >= 0; --y) {
                final int finalY = y;
                int actualRow = startRow + y;
                ByteSink tileSink = columnSink.getTileSink(actualRow);

                // Chain: generate tile (levelThreadPool) -> write tile (ioThreadPool)
                // Tile is written AS SOON AS it's generated!
                var tileFuture = CompletableFuture
                    .supplyAsync(() -> {
                        // Generate tile on levelThreadPool
                        int th;
                        int rowOffset;
                        if ((finalY+1) * _tileSize > height) {
                            th = height - (finalY * _tileSize);
                            rowOffset = 0;
                        } else {
                            rowOffset = height - (finalY+1) * _tileSize;
                            th = _tileSize;
                        }
//                        int rowOffset = finalY * _tileSize;
//                        int th = Math.min(_tileSize, resized.getHeight() - rowOffset);

                        BufferedImage tile = null;
                        if (finalTw > 0 && th > 0) {
                            synchronized (resized) {
                                try {
                                    tile = resized.getSubimage(stripColOffset, rowOffset, finalTw, th);
                                } catch (RasterFormatException rfe) {
                                    log.error("RasterFormatException getting subimage for tile {},{} at slice {},{}: stripColOffset={}, rowOffset={}, finalTw={}, th={}, resizedWidth={}, resizedHeight={}",
                                            actualCol, actualRow, sliceCoords.x, sliceCoords.y,
                                            stripColOffset, rowOffset, finalTw, th,
                                            resized.getWidth(), resized.getHeight());
                                    throw rfe;
                                }
                            }
                        }

                        BufferedImage destTile = createDestTile();
                        Graphics g = GRAPHICS_ENV.createGraphics(destTile);

                        if (_tileFormat == TileFormat.JPEG && tile != null) {
                            g.setColor(_tileBackgroundColor);
                            g.fillRect(0, 0, _tileSize, _tileSize);
                        }

                        if (tile != null) {
                            g.drawImage(tile, 0, _tileSize - th, null);
                        }

                        g.dispose();

                        return new TileGenerationResult(tileSink, destTile);
                    }, levelThreadPool);

                var tileWriterFuture = tileFuture
                    .thenComposeAsync(tileResult -> {
                        // Write tile on ioThreadPool as soon as it's generated!
                        return CompletableFuture.runAsync(() -> {
                            try {
                                writeTile(tileResult.tileSink, tileResult.image);
                            } catch (IOException e) {
                                log.error("Error writing tile", e);
                                throw new java.io.UncheckedIOException(e);
                            } finally {
                                tileResult.image.flush();
                            }
                        }, ioThreadPool);
                    });

                tileWriterFutures.add(tileWriterFuture);
            }
        }

        // Wait for ALL tile generation AND writes to complete before flushing resized image
        // The tiles are still referencing 'resized' during async execution!
        CompletableFuture<Void> allTilesComplete = CompletableFuture.allOf(tileWriterFutures.toArray(new CompletableFuture[0]));

        // Chain cleanup after all tiles are written
        allTilesComplete.thenRun(resized::flush);

        return tileWriterFutures;
    }

    /**
     * Result of tile generation - contains the tile image and where to write it.
     * Subscriber will write this on ioThreadPool.
     */
    private static class TileGenerationResult {
        final ByteSink tileSink;
        final BufferedImage image;

        TileGenerationResult(ByteSink tileSink, BufferedImage image) {
            this.tileSink = tileSink;
            this.image = image;
        }
    }

    /**
     * Write a single tile immediately.
     */
    private void writeTile(ByteSink tileSink, BufferedImage image) throws IOException {
        try {
            String format = _tileFormat == TileFormat.PNG ? "png" : "jpeg";
            try (OutputStream tileStream = tileSink.openStream()) {
                if (!ImageIO.write(image, format, tileStream)) {
                    log.error("Failed to write tile");
                    throw new IOException("Failed to write tile");
                }
            }
        } catch (Exception | Error ex) {
            log.error("Exception occurred saving tile", ex);
            throw ex;
        }
    }

    private BufferedImage createDestTile() {
        BufferedImage destTile;

        if (_tileFormat == TileFormat.PNG) {
            destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_4BYTE_ABGR);
        } else {
            destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_3BYTE_BGR);
        }
        return destTile;
    }

    private Point getImageDimensions(byte[] imageBytes) throws IOException {
        var bais = UnsynchronizedByteArrayInputStream.builder()
                .setByteArray(imageBytes)
                .setOffset(0)
                .get();

        ImageInputStream iis = ImageIO.createImageInputStream(bais);
        if (iis == null) {
            throw new IOException("Failed to create ImageInputStream");
        }

        try {
            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                throw new IOException("No image readers for image");
            }

            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                throw new IOException("No suitable image reader selected");
            }

            reader.setInput(iis, true, false);

            try {
                int w = reader.getWidth(0);
                int h = reader.getHeight(0);
                return new Point(w, h);
            } finally {
                reader.dispose();
            }
        } finally {
            iis.close();
        }
    }
}


