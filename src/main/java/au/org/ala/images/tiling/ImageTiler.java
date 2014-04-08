package au.org.ala.images.tiling;

import au.org.ala.images.util.FastByteArrayInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
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

    public ImageTiler(ImageTilerConfig config) {
        if (config != null) {
            _ioThreadCount = config.getIOThreadCount();
            _maxLevelThreads = config.getLevelThreadCount();
            _tileSize = config.getTileSize();
            _maxColsPerStrip = config.getMaxColumnsPerStrip();
            _tileFormat = config.getTileFormat();
            _tileBackgroundColor = config.getTileBackgroundColor();
        }
    }

    public ImageTilerResults tileImage(File imageFile, File destinationDirectory) throws IOException, InterruptedException {

        try {
            // Clean up existing tiles
            if (destinationDirectory.exists()) {
                FileUtils.deleteDirectory(destinationDirectory);
            }
            // make sure dir exists
            destinationDirectory.mkdirs();


            int[] pyramid = new int[] { 128, 64, 32, 16, 8, 4, 2, 1 };

            if (imageFile.length() < 5*1024*1024) {
                pyramid = new int[] { 16, 8, 4, 2, 1 };
            }

            ExecutorService levelThreadPool = Executors.newFixedThreadPool(Math.min(pyramid.length, _maxLevelThreads));
            ExecutorService ioThreadPool = Executors.newFixedThreadPool(_ioThreadCount);


            byte[] imageBytes = IOUtils.toByteArray(FileUtils.openInputStream(imageFile));

            // Submit it reverse order so the big jobs get started first
            for (int level = pyramid.length - 1; level >= 0 ; level--) {
                submitLevelForProcessing(level, imageBytes, pyramid, destinationDirectory, levelThreadPool, ioThreadPool);
            }

//            System.out.println("Waiting for Image Processing threads...");
            levelThreadPool.shutdown();
            levelThreadPool.awaitTermination(30, TimeUnit.MINUTES);

//            System.out.println("Waiting for IO threads..." );
            ioThreadPool.shutdown();
            ioThreadPool.awaitTermination(30, TimeUnit.MINUTES);

            return new ImageTilerResults(true, pyramid.length);
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

                if (tile.getHeight() < _tileSize || tile.getWidth() < _tileSize) {
                    BufferedImage blankTile = null;

                    if (_tileFormat == TileFormat.PNG) {
                        blankTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_4BYTE_ABGR);
                    } else {
                        blankTile = new BufferedImage(_tileSize, _tileSize, BufferedImage.TYPE_3BYTE_BGR);
                    }

                    Graphics g = blankTile.getGraphics();
                    // We have to create a blank tile, and transfer the clipped tile into the appropriate spot (bottom left)
                    if (_tileFormat == TileFormat.JPEG) {
                        // file the tile with a background color
                        g.setColor(_tileBackgroundColor);
                        g.fillRect(0,0, _tileSize, _tileSize);
                    }

                    g.drawImage(tile, 0, _tileSize - tile.getHeight(), null);
                    g.dispose();
                    tile = blankTile;
                }

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
                ImageIO.write(image,  _tileFormat == TileFormat.PNG ? "PNG" : "JPEG", file);
            } catch (Exception ex) {
                ex.printStackTrace();
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
                System.err.println(ex.getMessage());
                ex.printStackTrace();
            } catch (Error err) {
                System.err.println(err.getMessage());
                err.printStackTrace();
            }
        }
    }

}
