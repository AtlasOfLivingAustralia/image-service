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
 * Alternate tiler implementation that works with input streams and uses
 * a different threading model.
 */
public class ImageTiler3 implements IImageTiler{

    private static final Logger log = LoggerFactory.getLogger(ImageTiler3.class);
    public static final int SLICE_SIZE = 8192;

    private int _tileSize = 256;
    private int _maxColsPerStrip = SLICE_SIZE / _tileSize ;
    private TileFormat _tileFormat = TileFormat.JPEG;
    private Color _tileBackgroundColor = Color.gray;
    private boolean _exceptionOccurred =  false; // crude mechanism for the worker threads to communicate serious failure
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

    public ImageTiler3(ImageTilerConfig config) {
        if (config != null) {
            ioThreadPool = config.getIoExecutor();
            levelThreadPool = config.getLevelExecutor();
            _tileSize = config.getTileSize();
            _maxColsPerStrip = SLICE_SIZE / _tileSize;
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

        var result = getBufferedImages(imageInputStream);
        var dimensions = result.imageDimensions;
        log.debug("tileImage:read image");

        int[] pyramid = _zoomFactorStrategy.getZoomFactors(dimensions.y, dimensions.x);
        int zoomLevels = pyramid.length;

        final int finalMaxLevel = Math.min(maxLevel, zoomLevels - 1);

        if (minLevel > finalMaxLevel) {
            log.debug("tileImage: asked for levels {} to {}, but only {} levels available", minLevel, maxLevel, zoomLevels);
            return zoomLevels; // No levels to process
        }

        List<SaveTileTask> ioStream;
        try (var images = result.imageStream) {
            ioStream = images.flatMap(pair -> {

                log.debug("tileImage:getZoomFactors");

                var coords = pair.getLeft();
                var image = pair.getRight();
                try {
                    var intStream = IntStream.rangeClosed(minLevel, finalMaxLevel);
                    if (minLevel == 0 && maxLevel == Integer.MAX_VALUE) {
                        // If we're doing the whole pyramid, start from the largest level and work down
                        intStream = intStream.map(index -> finalMaxLevel - index);
                        // otherwise we're doing user requested levels, so start only process the requested levels
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
            CompletableFuture.allOf(ioStream.stream().map(task -> CompletableFuture.runAsync(task, ioThreadPool)).collect(Collectors.toList()).toArray(CompletableFuture[]::new)).join();
        } catch (Exception e) {
            log.error("execution exception", e);
            _exceptionOccurred = true;
        }

        log.debug("tileImage: all tiles completed");
        return zoomLevels;
    }

    private static final class GetBufferedImageResult {
        final Stream<Pair<Point, BufferedImage>> imageStream;
        final Point imageDimensions;

        public GetBufferedImageResult(Stream<Pair<Point, BufferedImage>> imageStream, Point imageDimensions) {
            this.imageStream = imageStream;
            this.imageDimensions = imageDimensions;
        }
    }

    private GetBufferedImageResult getBufferedImages(InputStream imageInputStream) throws IOException {
        // maintain memory usage by splitting image into 8k or 4k chunks
        log.trace("getBufferedImages");
        byte[] imageBytes;
        try (var inputStream = imageInputStream) {
            imageBytes = IOUtils.toByteArray(inputStream);
        }
        log.trace("getBufferedImages:inputStream to imageBytes");

        // Create ImageInputStream directly without helper method
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
            throw new IOException("No compatible ImageReader found");
        }

        // Use selection strategy to prefer TwelveMonkeys readers
        ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
        if (reader == null) {
            throw new IOException("No suitable ImageReader selected");
        }
        reader.setInput(iis, true, false); // Set ignoreMetadata to false to allow reading metadata

        log.trace("getBufferedImages:created ImageReader");
        BufferedImage image;
        var stream = Stream.<Point>builder();

        try {

            int w = reader.getWidth(0);
            int h = reader.getHeight(0);

            var ratio = SLICE_SIZE / _tileSize;
            var segmentSize = ratio * _tileSize;
            log.trace("getBufferedImages: w: {}, h: {}, _tileSize: {}, ratio: {}, segmentSize: {}", w, h, _tileSize, ratio, segmentSize);
            var xs = (int)Math.ceil((double)w / (double) segmentSize);
            var ys = (int)Math.ceil((double)h / (double) segmentSize);
            log.trace("getBufferedImages: xs: {}, ys: {}", xs, ys);

            for (int i = 0; i < xs; ++i) {
                for (int j = ys - 1; j >= 0; --j) {
                    log.trace("getBufferedImages: accepting new point ({}, {})", i, j);
                    stream.accept(new Point(i,j));
                }
            }

            return new GetBufferedImageResult(stream.build().map( p -> {
                var params = reader.getDefaultReadParam();

                int rectWidth;
                int rectHeight;
                if ((p.x + 1) * segmentSize > w) {
                    rectWidth = w - (p.x * segmentSize);
                } else {
                    rectWidth = segmentSize;
                }
                if ((p.y + 1) * segmentSize > h) {
                    rectHeight = h - (p.y * segmentSize);
                } else {
                    rectHeight = segmentSize;
                }
                int rectX = p.x * segmentSize;
                int rectY = h - (p.y * segmentSize) - rectHeight;
                log.trace("getBufferedImages: sourceRegion: x: {}, y: {}, rectWidth: {}, rectHeight: {}", rectX, rectY, rectWidth, rectHeight);
//                log.debug("getBufferedImages: sourceRegion: x: {}, y: {}, rectWidth: {}, rectHeight: {}", p.x, p.y, rectWidth, rectHeight);

                params.setSourceRegion(new Rectangle(rectX, rectY, rectWidth, rectHeight));
//                params.setSourceRegion(new Rectangle(p.x, p.y, rectWidth, rectHeight));
//                params.setSourceSubsampling(subsample, subsample, 0, 0);
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
            }), new Point(w, h));


        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

        }
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

        log.trace("tileImageAtSubSampleLevel(subsample = {})", subsample);

        int srcHeight = bufferedImage.getHeight();
        int srcWidth = bufferedImage.getWidth();

        int height = (int) Math.ceil( ((double) srcHeight) / ((double) subsample));
        int width = (int) Math.ceil( ((double) srcWidth) / ((double) subsample));
        int rows = (int) Math.ceil( ((double) height) / ((double) _tileSize));
        int cols = (int) Math.ceil( ((double) width) / ((double) _tileSize));

        log.trace("tileImageAtSubSampleLevel: srcHeight {} srcWidth {} height {} width {} cols {} rows {} image {}", srcHeight, srcWidth, height, width, cols, rows, bufferedImage);

//            AffineTransform at = new AffineTransform();
//            at.scale(2.0, 2.0);
//            AffineTransformOp scaleOp =
//                    new AffineTransformOp(at, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
//            after = scaleOp.filter(before, after);

        var resized = Scalr.resize(bufferedImage, width, height);

        List<SaveTileTask> saveTileTasks = splitIntoTiles(resized, sliceCoords, levelSink, cols, rows, subsample);

        return saveTileTasks.stream();
    }

    private List<SaveTileTask> splitIntoTiles(BufferedImage strip, Point sliceCoords, TilerSink.LevelSink levelSink, int cols, int rows, int subsample) {
        var result = new ArrayList<SaveTileTask>(cols * rows);

        // Calculate max tiles per slice at this zoom level.
        // NOTE: At extreme zoom levels (high subsample), when SLICE_SIZE/subsample < _tileSize,
        // a single slice is smaller than one tile. This means multiple slices may map to the same
        // tile coordinates, potentially causing incomplete tiles. This is a known limitation of the
        // slice-based architecture and primarily affects the most zoomed-out levels (e.g., level 0)
        // which are rarely used in practice. A proper fix would require reading the entire image
        // (not sliced) for these extreme zoom levels.
        double sliceSizeAtLevel = (double) SLICE_SIZE / (double) subsample;
        double maxTilesPerSliceAtLevel = sliceSizeAtLevel / (double) _tileSize;
        final int stripHeight = strip.getHeight();

        // Use floating point to get accurate position, then truncate
        int startCol = (int)((double)sliceCoords.x * maxTilesPerSliceAtLevel);
        int startRow = (int)((double)sliceCoords.y * maxTilesPerSliceAtLevel);

        // Now divide the strip up into tiles
        for (int col = 0; col < cols; col++) {

            int stripColOffset = col * _tileSize;
            if (stripColOffset >= strip.getWidth()) {
                continue;
            }

            int actualCol = startCol + col;

            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(actualCol, 0, 1);

            int tw = _tileSize;
            if (tw + (col * _tileSize) > strip.getWidth()) {
                tw = strip.getWidth() - (col * _tileSize);
            }

            if (tw > strip.getWidth()) {
                tw = strip.getWidth();
            }

            for (int y = rows - 1; y >= 0; y--) {
                // row 0 = y strip height
                // row 1 = y strip height - tile size
                // ...
                // row n = y strip height - (n * tile size)
                int th;
                int rowOffset;
                if ((y+1) * _tileSize > stripHeight) {
                    th = stripHeight - (y * _tileSize);
                    rowOffset = 0;
                } else {
                    th = _tileSize;
                    rowOffset = stripHeight - (y+1) * _tileSize;
                }

                BufferedImage tile = null;
                if (tw > 0 && th > 0) {
                    // If height or width == 0 we can't actually take a subimage, so leave the tile blank
                    tile = strip.getSubimage(stripColOffset, rowOffset, tw, th);
                }


                BufferedImage destTile = createDestTile();
                Graphics g = GRAPHICS_ENV.createGraphics(destTile);

                // We have to create a blank tile, and transfer the clipped tile into the appropriate spot (top left)
                if (_tileFormat == TileFormat.JPEG && tile != null) {
                    // JPEG doesn't support transparency, and this tile is an edge tile so fill the tile with a background color first
                    g.setColor(_tileBackgroundColor);
                    g.fillRect(0, 0, _tileSize, _tileSize);
                }

                if (tile != null) {
                    // Now blit the tile to the destTile at bottom-left
                    g.drawImage(tile, 0, _tileSize - th, null);
                }
                // Clean up!
                g.dispose();
                // Shunt this off to the io writers.
                int actualRow = startRow + y;
                ByteSink tileSink = columnSink.getTileSink(actualRow);
                result.add(new SaveTileTask(tileSink, destTile));
            }
        }
        return result;
    }

    private BufferedImage createDestTile() {
        BufferedImage destTile; // We copy the image to a fresh buffered image so that we don't copy over any incompatible color profiles

        // PNG can support transparency, so use that rather than a background color
        if (_tileFormat == TileFormat.PNG) {
            destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_4BYTE_ABGR);
        } else {
            destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_3BYTE_BGR);
        }
        return destTile;
    }

    /************************************************************/
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

//                if (_tileFormat == TileFormat.JPEG) {
//                    ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
//                    ImageOutputStream ios = ImageIO.createImageOutputStream(file);
//                    writer.setOutput(ios);
//                    ImageWriteParam param = writer.getDefaultWriteParam();
//                    param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
//                    param.setCompressionQuality(1.0F); // Highest quality
//                    writer.write(image);
//                    writer.dispose();
//                    ios.close();
//                } else {
//                    if (!ImageIO.write(image,"png", file)) {
//                        _exceptionOccurred = true;
//                    }
//                }

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
