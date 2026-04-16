package au.org.ala.images.tiling;

import au.org.ala.images.util.DefaultImageReaderSelectionStrategy;
import com.google.common.io.ByteSink;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.*;
import javax.imageio.spi.IIORegistry;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(ImageTiler.class);

    private final int _tileSize;
    private final int _maxColsPerStrip;
    private final Executor _levelThreadPool;
    private final Executor _ioThreadPool;
    private final TileFormat _tileFormat;
    private final Color _tileBackgroundColor;
    private final ZoomFactorStrategy _zoomFactorStrategy;

    public ImageTiler(ImageTilerConfig config) {
        if (config != null) {
            _levelThreadPool = config.getLevelExecutor();
            _ioThreadPool = config.getIoExecutor();
            _tileSize = config.getTileSize();
            _maxColsPerStrip = config.getMaxColumnsPerStrip();
            _tileFormat = config.getTileFormat();
            _tileBackgroundColor = config.getTileBackgroundColor();
            _zoomFactorStrategy = config.getZoomFactorStrategy();
        } else {
            _tileSize = 256;
            _maxColsPerStrip = 6;
            _levelThreadPool = null;
            _ioThreadPool = null;
            _tileFormat = TileFormat.JPEG;
            _tileBackgroundColor = Color.gray;
            _zoomFactorStrategy = new DefaultZoomFactorStrategy();
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {

        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        try {
            AtomicBoolean errorOccurred = new AtomicBoolean(false);
            byte[] imageBytes = IOUtils.toByteArray(imageInputStream);
            int[] pyramid = _zoomFactorStrategy.getZoomFactors(imageBytes);

            int to = Math.max(0, minLevel);
            int from = Math.min(pyramid.length - 1, maxLevel);

            // Submit it reverse order so the big jobs get started first
            List<Future<?>> futures = new ArrayList<>(from - to + 1);
            for (int level = from; level >= to ; level--) {
                futures.add(submitLevelForProcessing(level, imageBytes, pyramid, tilerSink.getLevelSink(level), errorOccurred));
            }

            for (Future<?> future : futures) {
                future.get();
            }

            if (!errorOccurred.get()) {
                return new ImageTilerResults(true, pyramid.length);
            } else {
                return new ImageTilerResults(false, 0);
            }

        } catch (Throwable th) {
            log.error("Exception occurred tiling image", th);
        }

        return new ImageTilerResults(false, 0);
    }

    private Future<?> submitLevelForProcessing(int level, byte[] imageBytes, int[] pyramid, TilerSink.LevelSink levelSink, AtomicBoolean errorOccurred) {
        int subSample = pyramid[level];
        log.debug("Submitting level {} (subsample {}) for processing", level, subSample);
        FutureTask<Void> task = new FutureTask<>(new TileImageTask(imageBytes, subSample, levelSink, errorOccurred), null);
        _levelThreadPool.execute(task);
        return task;
    }

    private void tileImageAtSubSampleLevel(byte[] bytes, int subsample, TilerSink.LevelSink levelSink, AtomicBoolean errorOccurred) throws IOException {

        // Create ImageInputStream directly without helper method
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
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

        if (reader != null) {

            try {
                int srcHeight = reader.getHeight(0);
                int srcWidth = reader.getWidth(0);

                int height = (int) Math.ceil( ((double) srcHeight) / ((double) subsample));
                int rows = (int) Math.ceil( ((double) height) / ((double) _tileSize));

                int stripWidth = _tileSize * subsample * _maxColsPerStrip;
                int numberOfStrips = (int) Math.ceil( ((double) srcWidth) / ((double) stripWidth));

                log.debug("Image size: subsample {} srcWidth {} srcHeight {} height {} rows {} stripWidth {} numberOfStrips {}", subsample, srcWidth, srcHeight, height, rows, stripWidth, numberOfStrips);

                for (int stripIndex = 0; stripIndex < numberOfStrips; stripIndex++) {

                    int srcStripOffset = stripIndex * stripWidth;
                    if (srcStripOffset > srcWidth) {
                        log.debug("Skipping subsample {} strip {} as it is beyond the image width", subsample, stripIndex);
                        continue;
                    }

                    Rectangle stripRect = new Rectangle(srcStripOffset, 0, stripWidth, srcHeight);
                    log.debug("Processing subsample {} strip {} at offset {}", subsample, stripIndex, srcStripOffset);
                    ImageReadParam params = reader.getDefaultReadParam();
                    params.setSourceRegion(stripRect);
                    params.setSourceSubsampling(subsample, subsample, 0, 0);
                    BufferedImage strip = reader.read(0, params);
                    splitStripIntoTiles(strip, levelSink, rows, stripIndex, errorOccurred);
                }
            } finally {
                var input = reader.getInput();
                if (input instanceof Closeable) {
                    try {
                        ((Closeable) input).close();
                    } catch (Exception e) {
                        // ignored
                    }
                }
                reader.dispose();
            }

        } else {
            throw new RuntimeException("No readers found suitable for file");
        }
    }

    private void splitStripIntoTiles(BufferedImage strip, TilerSink.LevelSink levelSink, int rows, int stripIndex, AtomicBoolean errorOccurred) {
        // Now divide the strip up into tiles
        for (int col = 0; col < _maxColsPerStrip; col++) {

            int stripColOffset = col * _tileSize;
            if (stripColOffset >= strip.getWidth()) {
                continue;
            }

            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, stripIndex, _maxColsPerStrip);

            int tw = _tileSize;
            if (tw + (col * _tileSize) > strip.getWidth()) {
                tw = strip.getWidth() - (col * _tileSize);
            }

            if (tw > strip.getWidth()) {
                tw = strip.getWidth();
            }

            for (int y = 0; y < rows; y++) {
                int th = _tileSize;

                // Start from the bottom of the row and work up
                int rowOffset = strip.getHeight() - ((rows - y) * _tileSize);

                if (rowOffset < 0) {
                    th = strip.getHeight() - ((rows - 1) * _tileSize);
                    rowOffset = 0;
                }

                BufferedImage tile = null;
                if (tw > 0 && th > 0) {
                    // If height or width == 0 we can't actually take a subimage, so leave the tile blank
                    tile = strip.getSubimage(stripColOffset, rowOffset, tw, th);
                }


                BufferedImage destTile; // We copy the image to a fresh buffered image so that we don't copy over any incompatible color profiles

                // PNG can support transparency, so use that rather than a background color
                if (_tileFormat == TileFormat.PNG) {
                    destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_4BYTE_ABGR);
                } else {
                    destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_3BYTE_BGR);
                }
                Graphics g = destTile.getGraphics();

                // We have to create a blank tile, and transfer the clipped tile into the appropriate spot (bottom left)
                if (_tileFormat == TileFormat.JPEG && tile != null) {
                    // JPEG doesn't support transparency, and this tile is an edge tile so fill the tile with a background color first
                    g.setColor(_tileBackgroundColor);
                    g.fillRect(0, 0, _tileSize, _tileSize);
                }

                if (tile != null) {
                    // Now blit the tile to the destTile
                    g.drawImage(tile, 0, _tileSize - tile.getHeight(), null);
                }
                // Clean up!
                g.dispose();
                // Shunt this off to the io writers.
                ByteSink tileSink = columnSink.getTileSink(rows - y - 1);
                _ioThreadPool.execute(new ImageTiler.SaveTileTask(tileSink, destTile, errorOccurred));
            }
        }
    }

    /************************************************************/
    class SaveTileTask implements Runnable {

        protected final ByteSink tileSink;
        protected final BufferedImage image;
        protected final AtomicBoolean errorOccurred;

        public SaveTileTask(ByteSink tileSink, BufferedImage image, AtomicBoolean errorOccurred) {
            this.tileSink = tileSink;
            this.image = image;
            this.errorOccurred = errorOccurred;
        }

        public void run() {
            try {

                String format = _tileFormat == TileFormat.PNG ? "png" : "jpeg";
                try (OutputStream tileStream = tileSink.openStream()) {
                    if (!ImageIO.write(image, format, tileStream)) {
                        errorOccurred.set(true);
                    }
                }

            } catch (Exception | Error ex) {
                errorOccurred.set(true);
                log.error("Exception occurred saving file task", ex);
            }
        }

    }

    /************************************************************/
    class TileImageTask implements Runnable {

        private final byte[] _bytes;
        private final int _subSample;
        private final TilerSink.LevelSink _levelSink;
        private final AtomicBoolean _errorOccurred;

        public TileImageTask(byte[] bytes, int subSample, TilerSink.LevelSink levelSink, AtomicBoolean errorOccurred) {
            _bytes = bytes;
            _subSample = subSample;
            _levelSink = levelSink;
            _errorOccurred = errorOccurred;
        }

        public void run() {
            try {
                tileImageAtSubSampleLevel(_bytes, _subSample, _levelSink, _errorOccurred);
            } catch (Exception | Error ex) {
                _errorOccurred.set(true);
                log.error("Exception occurred during tiling image task", ex);
            }
        }
    }

}
