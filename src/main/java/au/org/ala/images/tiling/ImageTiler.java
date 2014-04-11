package au.org.ala.images.tiling;

import au.org.ala.images.util.FastByteArrayInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import javax.imageio.*;
import javax.imageio.spi.IIORegistry;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ImageTiler {

    private int _tileSize = 256;
    private int _maxColsPerStrip = 6;
    private int _ioThreadCount = 2;
    private int _maxLevelThreads = 2;
    private TileFormat _tileFormat = TileFormat.JPEG;
    private Color _tileBackgroundColor = Color.gray;
    private boolean _exceptionOccurred =  false; // crude mechanism for the worker threads to communicate serious failure
    private ZoomFactorStrategy _zoomFactorStrategy = new DefaultZoomFactorStrategy();

    static {
        ImageIO.scanForPlugins();
        IIORegistry.getDefaultInstance();
        ImageIO.setUseCache(false);
    }

    public ImageTiler(ImageTilerConfig config) {
        if (config != null) {
            _ioThreadCount = config.getIOThreadCount();
            _maxLevelThreads = config.getLevelThreadCount();
            _tileSize = config.getTileSize();
            _maxColsPerStrip = config.getMaxColumnsPerStrip();
            _tileFormat = config.getTileFormat();
            _tileBackgroundColor = config.getTileBackgroundColor();
            _zoomFactorStrategy = config.getZoomFactorStrategy();
        }
    }

    public ImageTilerResults tileImage(File imageFile, File destinationDirectory) throws IOException, InterruptedException {

        try {
            _exceptionOccurred = false; // reset the error flag
            // Clean up existing tiles
            if (destinationDirectory.exists()) {
                FileUtils.deleteDirectory(destinationDirectory);
            }
            // make sure dir exists
            destinationDirectory.mkdirs();
            byte[] imageBytes = IOUtils.toByteArray(FileUtils.openInputStream(imageFile));
            int[] pyramid = _zoomFactorStrategy.getZoomFactors(imageFile, imageBytes);

            ExecutorService levelThreadPool = Executors.newFixedThreadPool(Math.min(pyramid.length, _maxLevelThreads));
            ExecutorService ioThreadPool = Executors.newFixedThreadPool(_ioThreadCount);

            // Submit it reverse order so the big jobs get started first
            for (int level = pyramid.length - 1; level >= 0 ; level--) {
                submitLevelForProcessing(level, imageBytes, pyramid, destinationDirectory, levelThreadPool, ioThreadPool);
            }

            levelThreadPool.shutdown();
            levelThreadPool.awaitTermination(30, TimeUnit.MINUTES);

            ioThreadPool.shutdown();
            ioThreadPool.awaitTermination(30, TimeUnit.MINUTES);

            if (!_exceptionOccurred) {
                return new ImageTilerResults(true, pyramid.length);
            } else {
                return new ImageTilerResults(false, 0);
            }

        } catch (Throwable th) {
            th.printStackTrace();
        }

        return new ImageTilerResults(false, 0);
    }

    private void submitLevelForProcessing(int level, byte[] imageBytes, int[] pyramid, File destinationDirectory, ExecutorService levelThreadPool, ExecutorService ioThreadPool) {
        int subSample = pyramid[level];
        File levelDir = new File(destinationDirectory.getPath() + "/" + level);
        levelDir.mkdirs();
        levelThreadPool.submit(new TileImageTask(imageBytes, subSample, levelDir.getPath(), ioThreadPool));
    }

    private void tileImageAtSubSampleLevel(byte[] bytes, int subsample, String destinationPath, ExecutorService ioThreadPool) throws IOException {

        FastByteArrayInputStream bis = new FastByteArrayInputStream(bytes, bytes.length);
        ImageInputStream iis = ImageIO.createImageInputStream(bis);
        Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);

        if (iter.hasNext()) {
            ImageReader reader = iter.next();
            reader.setInput(iis);

            int srcHeight = reader.getHeight(0);
            int srcWidth = reader.getWidth(0);

            int height = (int) Math.ceil( ((double) reader.getHeight(0)) / ((double) subsample));
            int rows = (int) Math.ceil( ((double) height) / ((double) _tileSize));

            File levelDir = new File(destinationPath);
            levelDir.mkdirs();

            int stripWidth = _tileSize * subsample * _maxColsPerStrip;
            int numberOfStrips = (int) Math.ceil( ((double) srcWidth) / ((double) stripWidth));

            for (int stripIndex = 0; stripIndex < numberOfStrips; stripIndex++) {

                int srcStripOffset = stripIndex * stripWidth;
                if (srcStripOffset > srcWidth) {
                    continue;
                }

                Rectangle stripRect = new Rectangle(srcStripOffset, 0, stripWidth, srcHeight);
                ImageReadParam params = reader.getDefaultReadParam();
                params.setSourceRegion(stripRect);
                params.setSourceSubsampling(subsample, subsample, 0, 0);
                BufferedImage strip = reader.read(0, params);
                splitStripIntoTiles(strip, levelDir, rows, stripIndex, ioThreadPool);
            }
            iis.close();
            bis.close();
            reader.dispose();
            System.gc();
        } else {
            throw new RuntimeException("No readers found suitable for file");
        }
    }

    private void splitStripIntoTiles(BufferedImage strip, File levelDir, int rows, int stripIndex, ExecutorService ioThreadPool) {
        // Now divide the strip up into tiles
        for (int col = 0; col < _maxColsPerStrip; col++) {

            int stripColOffset = col * _tileSize;
            if (stripColOffset > strip.getWidth()) {
                continue;
            }

            File colDir = new File(levelDir.getPath() + "/" + (col + (stripIndex * _maxColsPerStrip)));
            colDir.mkdirs();

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

                BufferedImage tile = strip.getSubimage(stripColOffset, rowOffset, tw, th);
                BufferedImage destTile; // We copy the image to a fresh buffered image so that we don't copy over any incompatible color profiles


                // PNG can support transparency, so use that rather than a background color
                if (_tileFormat == TileFormat.PNG) {
                    destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_4BYTE_ABGR);
                } else {
                    destTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_3BYTE_BGR);
                }
                Graphics g = destTile.getGraphics();

                // We have to create a blank tile, and transfer the clipped tile into the appropriate spot (bottom left)
                if (_tileFormat == TileFormat.JPEG && tile.getHeight() < _tileSize || tile.getWidth() < _tileSize) {
                    // JPEG doesn't support transparency, and this tile is an edge tile so fill the tile with a background color first
                    g.setColor(_tileBackgroundColor);
                    g.fillRect(0, 0, _tileSize, _tileSize);
                }

                // Now blit the tile to the destTile
                g.drawImage(tile, 0, _tileSize - tile.getHeight(), null);
                // Clean up!
                g.dispose();
                tile = destTile;
                // Shunt this off to the io writers.
                File tileFile = new File(colDir.getPath() + String.format("/%d.png", (rows - y - 1)));
                ioThreadPool.submit(new ImageTiler.SaveTileTask(tileFile, tile));
            }
        }
    }

    /************************************************************/
    class SaveTileTask implements Runnable {

        protected File file;
        protected BufferedImage image;

        public SaveTileTask(File file, BufferedImage image) {
            this.file = file;
            this.image = image;
        }

        public void run() {
            try {

                String format = _tileFormat == TileFormat.PNG ? "png" : "jpeg";
                if (!ImageIO.write(image, format, file)) {
                    _exceptionOccurred = true;
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

            } catch (Exception ex) {
                _exceptionOccurred = true;
                ex.printStackTrace();
            } catch (Error error) {
                _exceptionOccurred = true;
                error.printStackTrace();
            }
        }

    }

    /************************************************************/
    class TileImageTask implements Runnable {

        private byte[] _bytes;
        private int _subSample;
        private String _destinationPath;
        private ExecutorService _ioThreadPool;

        public TileImageTask(byte[] bytes, int subSample, String destinationPath, ExecutorService ioThreadPool) {
            _bytes = bytes;
            _subSample = subSample;
            _destinationPath = destinationPath;
            _ioThreadPool = ioThreadPool;
        }

        public void run() {
            try {
                tileImageAtSubSampleLevel(_bytes, _subSample, _destinationPath, _ioThreadPool);
            } catch (Exception ex) {
                _exceptionOccurred = true;
                ex.printStackTrace();
            } catch (Error err) {
                System.err.println(err.getMessage());
                _exceptionOccurred = true;
                err.printStackTrace();
            }
        }
    }

}
