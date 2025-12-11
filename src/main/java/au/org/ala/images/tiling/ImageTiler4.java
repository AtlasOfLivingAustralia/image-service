package au.org.ala.images.tiling;

import au.org.ala.images.util.DefaultImageReaderSelectionStrategy;
import com.google.common.io.ByteSink;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.commons.lang3.tuple.Pair;
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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Advanced tiler implementation that properly handles all zoom levels including extreme ones.
 * 
 * Key improvements over ImageTiler3:
 * - Detects when slices would be smaller than tiles at extreme zoom levels
 * - Uses ImageReader subsampling for efficient extreme zoom level processing
 * - Reads entire image (not sliced) for extreme zoom levels
 * - Maintains slice-based architecture for normal zoom levels (memory efficient)
 */
public class ImageTiler4 implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(ImageTiler4.class);
    public static final int SLICE_SIZE = 8192;

    private int _tileSize = 256;
    private TileFormat _tileFormat = TileFormat.JPEG;
    private Color _tileBackgroundColor = Color.gray;
    private boolean _exceptionOccurred = false;
    private ZoomFactorStrategy _zoomFactorStrategy = new DefaultZoomFactorStrategy(_tileSize);

    private ExecutorService levelThreadPool;
    private ExecutorService ioThreadPool;

    private static final GraphicsEnvironment GRAPHICS_ENV =
            GraphicsEnvironment.getLocalGraphicsEnvironment();

    static {
        ImageIO.scanForPlugins();
        IIORegistry.getDefaultInstance();
        ImageIO.setUseCache(false);
    }

    public ImageTiler4(ImageTilerConfig config) {
        if (config != null) {
            ioThreadPool = config.getIoExecutor();
            levelThreadPool = config.getLevelExecutor();
            _tileSize = config.getTileSize();
            _tileFormat = config.getTileFormat();
            _tileBackgroundColor = config.getTileBackgroundColor();
            _zoomFactorStrategy = config.getZoomFactorStrategy();
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        int zoomLevels = startTiling(imageInputStream, tilerSink, minLevel, maxLevel);

        if (!_exceptionOccurred) {
            return new ImageTilerResults(true, zoomLevels);
        } else {
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

        // Process extreme zoom levels (if any) - read full image with subsampling
        if (minLevel <= extremeZoomThreshold && extremeZoomThreshold < zoomLevels) {
            int extremeMaxLevel = Math.min(extremeZoomThreshold, finalMaxLevel);
            log.debug("tileImage: processing extreme zoom levels {} to {} with full-image approach", minLevel, extremeMaxLevel);
            processExtremeZoomLevels(imageBytes, dimensions, pyramid, minLevel, extremeMaxLevel, tilerSink);
        }

        // Process normal zoom levels with slice-based approach
        int normalMinLevel = Math.max(minLevel, extremeZoomThreshold + 1);
        if (normalMinLevel <= finalMaxLevel) {
            log.debug("tileImage: processing normal zoom levels {} to {} with slice-based approach", normalMinLevel, finalMaxLevel);
            processNormalZoomLevels(imageBytes, dimensions, pyramid, normalMinLevel, finalMaxLevel, tilerSink);
        }

        log.debug("tileImage: all tiles completed");
        return zoomLevels;
    }

    /**
     * Find the highest zoom level (most zoomed out) where slice size < tile size.
     * Levels at or below this threshold need full-image processing.
     */
    private int findExtremeZoomThreshold(int[] pyramid, Point dimensions) {
        for (int level = 0; level < pyramid.length; level++) {
            int subsample = pyramid[level];
            double sliceSizeAtLevel = (double) SLICE_SIZE / (double) subsample;
            
            // If slice size is at least 2x tile size, we can safely use slicing
            if (sliceSizeAtLevel >= _tileSize * 2) {
                return level - 1; // Previous level was the last extreme one
            }
        }
        return pyramid.length - 1; // All levels are extreme
    }

    /**
     * Process extreme zoom levels by reading the full image with ImageReader subsampling.
     * This is more efficient than reading slices and prevents tile overlap issues.
     */
    private void processExtremeZoomLevels(byte[] imageBytes, Point dimensions, int[] pyramid, 
                                          int minLevel, int maxLevel, TilerSink tilerSink) throws IOException {
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
                // Process each extreme zoom level
                List<CompletableFuture<List<SaveTileTask>>> futures = new ArrayList<>();
                
                for (int level = minLevel; level <= maxLevel; level++) {
                    int subsample = pyramid[level];
                    final int finalLevel = level;
                    
                    CompletableFuture<List<SaveTileTask>> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            return processFullImageLevel(reader, dimensions, subsample, tilerSink.getLevelSink(finalLevel));
                        } catch (Exception e) {
                            log.error("Error processing extreme zoom level " + finalLevel, e);
                            _exceptionOccurred = true;
                            return new ArrayList<>();
                        }
                    }, levelThreadPool);
                    
                    futures.add(future);
                }

                // Wait for all level processing to complete
                List<SaveTileTask> allTasks = futures.stream()
                        .flatMap(f -> f.join().stream())
                        .collect(Collectors.toList());

                // Execute IO tasks
                CompletableFuture.allOf(
                    allTasks.stream()
                        .map(task -> CompletableFuture.runAsync(task, ioThreadPool))
                        .toArray(CompletableFuture[]::new)
                ).join();

            } finally {
                reader.dispose();
            }
        } finally {
            if (iis != null) {
                iis.close();
            }
        }
    }

    /**
     * Process a single zoom level using the full image with ImageReader subsampling.
     */
    private synchronized List<SaveTileTask> processFullImageLevel(ImageReader reader, Point dimensions, 
                                                       int subsample, TilerSink.LevelSink levelSink) throws IOException {
        log.debug("processFullImageLevel: subsample={}", subsample);

        // Use ImageReader's built-in subsampling for efficiency
        ImageReadParam params = reader.getDefaultReadParam();
        params.setSourceSubsampling(subsample, subsample, 0, 0);
        
        BufferedImage fullImage = reader.read(0, params);
        
        log.debug("processFullImageLevel: read image {}x{}", fullImage.getWidth(), fullImage.getHeight());

        // Now split this full (subsampled) image into tiles
        return splitFullImageIntoTiles(fullImage, levelSink);
    }

    /**
     * Split a full (already subsampled) image into tiles.
     */
    private List<SaveTileTask> splitFullImageIntoTiles(BufferedImage image, TilerSink.LevelSink levelSink) {
        int cols = (int) Math.ceil((double) image.getWidth() / _tileSize);
        int rows = (int) Math.ceil((double) image.getHeight() / _tileSize);
        
        log.debug("splitFullImageIntoTiles: {}x{} image -> {}x{} tiles", 
                  image.getWidth(), image.getHeight(), cols, rows);

        List<SaveTileTask> tasks = new ArrayList<>(cols * rows);

        for (int col = 0; col < cols; col++) {
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, 1);

            for (int row = 0; row < rows; row++) {
                int x = col * _tileSize;
                int y = row * _tileSize;
                int tw = Math.min(_tileSize, image.getWidth() - x);
                int th = Math.min(_tileSize, image.getHeight() - y);

                BufferedImage tile = null;
                if (tw > 0 && th > 0) {
                    tile = image.getSubimage(x, y, tw, th);
                }

                BufferedImage destTile = createDestTile();
                Graphics g = GRAPHICS_ENV.createGraphics(destTile);

                if (_tileFormat == TileFormat.JPEG && tile != null) {
                    g.setColor(_tileBackgroundColor);
                    g.fillRect(0, 0, _tileSize, _tileSize);
                }

                if (tile != null) {
                    g.drawImage(tile, 0, 0, null);
                }
                
                g.dispose();

                ByteSink tileSink = columnSink.getTileSink(row);
                tasks.add(new SaveTileTask(tileSink, destTile));
            }
        }

        return tasks;
    }

    /**
     * Process normal zoom levels using the slice-based approach for memory efficiency.
     */
    private void processNormalZoomLevels(byte[] imageBytes, Point dimensions, int[] pyramid,
                                         int minLevel, int maxLevel, TilerSink tilerSink) throws IOException {
        var result = getBufferedImagesSliced(imageBytes, dimensions);

        List<SaveTileTask> ioStream;
        try (var images = result.imageStream) {
            ioStream = images.flatMap(pair -> {
                var coords = pair.getLeft();
                var image = pair.getRight();
                try {
                    var intStream = IntStream.rangeClosed(minLevel, maxLevel);
                    if (minLevel == 0 && maxLevel == Integer.MAX_VALUE) {
                        intStream = intStream.map(index -> maxLevel - index);
                    }
                    return intStream
                            .mapToObj(level -> submitLevelForProcessing(image, coords, pyramid[level], tilerSink.getLevelSink(level)))
                            .flatMap(future -> {
                                try {
                                    return future.join();
                                } catch (Exception e) {
                                    log.error("execution exception", e);
                                    _exceptionOccurred = true;
                                    return Stream.empty();
                                }
                            });
                } finally {
                    if (image != null) {
                        image.flush();
                    }
                }
            }).collect(Collectors.toList());
        }
        
        try {
            CompletableFuture.allOf(
                ioStream.stream()
                    .map(task -> CompletableFuture.runAsync(task, ioThreadPool))
                    .collect(Collectors.toList())
                    .toArray(CompletableFuture[]::new)
            ).join();
        } catch (Exception e) {
            log.error("execution exception", e);
            _exceptionOccurred = true;
        }
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

    private static final class GetBufferedImageResult {
        final Stream<Pair<Point, BufferedImage>> imageStream;

        public GetBufferedImageResult(Stream<Pair<Point, BufferedImage>> imageStream) {
            this.imageStream = imageStream;
        }
    }

    private GetBufferedImageResult getBufferedImagesSliced(byte[] imageBytes, Point dimensions) throws IOException {
        var bais = UnsynchronizedByteArrayInputStream.builder()
                .setByteArray(imageBytes)
                .setOffset(0)
                .get();

        ImageInputStream iis = ImageIO.createImageInputStream(bais);
        if (iis == null) {
            throw new IOException("Failed to create ImageInputStream");
        }

        Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
        if (!readers.hasNext()) {
            throw new IOException("No image readers for image");
        }

        ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
        if (reader == null) {
            throw new IOException("No suitable image reader selected");
        }
        
        reader.setInput(iis, true, false);

        var stream = Stream.<Point>builder();

        int w = dimensions.x;
        int h = dimensions.y;

        var segmentSize = SLICE_SIZE;
        var xs = (int) Math.ceil((double) w / (double) segmentSize);
        var ys = (int) Math.ceil((double) h / (double) segmentSize);

        for (int i = 0; i < xs; ++i) {
            for (int j = 0; j < ys; ++j) {
                stream.accept(new Point(i, j));
            }
        }

        return new GetBufferedImageResult(stream.build().map(p -> {
            var params = reader.getDefaultReadParam();

            int rectWidth = (p.x + 1) * segmentSize > w ? w - (p.x * segmentSize) : segmentSize;
            int rectHeight = (p.y + 1) * segmentSize > h ? h - (p.y * segmentSize) : segmentSize;
            int rectX = p.x * segmentSize;
            int rectY = p.y * segmentSize;

            params.setSourceRegion(new Rectangle(rectX, rectY, rectWidth, rectHeight));
            
            try {
                return Pair.of(p, reader.read(0, params));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).onClose(() -> {
            var input = reader.getInput();
            if (input instanceof Closeable) {
                try {
                    ((Closeable) input).close();
                } catch (Exception e) {
                    // ignored
                }
            }
            reader.dispose();
        }));
    }

    private CompletableFuture<Stream<SaveTileTask>> submitLevelForProcessing(BufferedImage bufferedImage, Point sliceCoords, int subSample, TilerSink.LevelSink levelSink) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return tileImageAtSubSampleLevel(bufferedImage, sliceCoords, subSample, levelSink);
            } catch (IOException e) {
                _exceptionOccurred = true;
                log.error("Exception occurred during tiling image task", e);
                return Stream.empty();
            }
        }, levelThreadPool);
    }

    private Stream<SaveTileTask> tileImageAtSubSampleLevel(BufferedImage bufferedImage, Point sliceCoords, int subsample, TilerSink.LevelSink levelSink) throws IOException {
        int srcHeight = bufferedImage.getHeight();
        int srcWidth = bufferedImage.getWidth();

        int height = (int) Math.ceil(((double) srcHeight) / ((double) subsample));
        int width = (int) Math.ceil(((double) srcWidth) / ((double) subsample));
        int rows = (int) Math.ceil(((double) height) / ((double) _tileSize));
        int cols = (int) Math.ceil(((double) width) / ((double) _tileSize));

        var resized = Scalr.resize(bufferedImage, width, height);

        List<SaveTileTask> saveTileTasks = splitIntoTiles(resized, sliceCoords, levelSink, cols, rows, subsample);

        return saveTileTasks.stream();
    }

    private List<SaveTileTask> splitIntoTiles(BufferedImage strip, Point sliceCoords, TilerSink.LevelSink levelSink, int cols, int rows, int subsample) {
        var result = new ArrayList<SaveTileTask>(cols * rows);

        // Calculate tile position based on actual pixel positions
        double sliceSizeAtLevel = (double) SLICE_SIZE / (double) subsample;
        double maxTilesPerSliceAtLevel = sliceSizeAtLevel / (double) _tileSize;

        int startCol = (int) ((double) sliceCoords.x * maxTilesPerSliceAtLevel);
        int startRow = (int) ((double) sliceCoords.y * maxTilesPerSliceAtLevel);

        for (int col = 0; col < cols; col++) {
            int stripColOffset = col * _tileSize;
            if (stripColOffset >= strip.getWidth()) {
                continue;
            }

            int actualCol = startCol + col;
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(actualCol, 0, 1);

            int tw = Math.min(_tileSize, strip.getWidth() - stripColOffset);

            for (int y = 0; y < rows; y++) {
                int rowOffset = y * _tileSize;
                int th = Math.min(_tileSize, strip.getHeight() - rowOffset);

                BufferedImage tile = null;
                if (tw > 0 && th > 0) {
                    tile = strip.getSubimage(stripColOffset, rowOffset, tw, th);
                }

                BufferedImage destTile = createDestTile();
                Graphics g = GRAPHICS_ENV.createGraphics(destTile);

                if (_tileFormat == TileFormat.JPEG && tile != null) {
                    g.setColor(_tileBackgroundColor);
                    g.fillRect(0, 0, _tileSize, _tileSize);
                }

                if (tile != null) {
                    g.drawImage(tile, 0, 0, null);
                }
                
                g.dispose();

                int actualRow = startRow + y;
                ByteSink tileSink = columnSink.getTileSink(actualRow);
                result.add(new SaveTileTask(tileSink, destTile));
            }
        }
        return result;
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

    class SaveTileTask implements Runnable {
        protected ByteSink tileSink;
        protected BufferedImage image;

        public SaveTileTask(ByteSink tileSink, BufferedImage image) {
            this.tileSink = tileSink;
            this.image = image;
        }

        public void run() {
            try {
                String format = _tileFormat == TileFormat.PNG ? "png" : "jpeg";
                try (OutputStream tileStream = tileSink.openStream()) {
                    if (!ImageIO.write(image, format, tileStream)) {
                        _exceptionOccurred = true;
                    }
                }
            } catch (Exception | Error ex) {
                _exceptionOccurred = true;
                log.error("Exception occurred saving file task", ex);
            } finally {
                if (image != null) {
                    image.flush();
                }
            }
        }
    }
}

