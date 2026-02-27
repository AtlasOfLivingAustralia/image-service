package au.org.ala.images.tiling;

import java.awt.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ImageTilerConfig {

    private ExecutorService _ioExecutor;
    private ExecutorService _levelExecutor;
    private int _ioThreads = 2;
    private int _levelThreads = 2;
    private int _tileSize = 256;
    private int _maxColumnsPerStrip = 6;
    private TileFormat _tileFormat = TileFormat.JPEG;
    private Color _tileBackgroundColor = new Color(221, 221, 221);
    private ZoomFactorStrategy _zoomFactorStrategy = new DefaultZoomFactorStrategy(_tileSize);

    public ImageTilerConfig() {
        _ioExecutor = Executors.newFixedThreadPool(_ioThreads);
        _levelExecutor = Executors.newFixedThreadPool(_levelThreads);
    }

    public ImageTilerConfig(ExecutorService ioExecutor, ExecutorService levelExecutor) {
        this._levelExecutor = levelExecutor;
        this._ioExecutor = ioExecutor;
    }

    public ImageTilerConfig(int ioThreads, int levelThreads, int tileSize, int maxColumnsPerStrip, TileFormat tileFormat) {
        _ioThreads = ioThreads;
        _levelThreads = levelThreads;
        _tileSize = tileSize;
        _maxColumnsPerStrip = maxColumnsPerStrip;
        _tileFormat = tileFormat;
        _ioExecutor = Executors.newFixedThreadPool(_ioThreads);
        _levelExecutor = Executors.newFixedThreadPool(_levelThreads);
        _zoomFactorStrategy = new DefaultZoomFactorStrategy(_tileSize);
    }

    public ImageTilerConfig(ExecutorService ioExecutor, ExecutorService levelExecutor, int tileSize, int maxColumnsPerStrip, TileFormat tileFormat) {
        _tileSize = tileSize;
        _maxColumnsPerStrip = maxColumnsPerStrip;
        _tileFormat = tileFormat;
        _ioExecutor = ioExecutor;
        _levelExecutor = levelExecutor;
    }

    public int getIOThreadCount() {
        return _ioThreads;
    }
    public void setIOThreadCount(int threadCount) { _ioThreads = threadCount; }

    public int getLevelThreadCount() {
        return _levelThreads;
    }
    public void setLevelThreadCount(int threadCount) { _levelThreads = threadCount; }

    public int getTileSize() {
        return _tileSize;
    }
    public void setTileSize(int tileSize) { _tileSize = tileSize; }

    public int getMaxColumnsPerStrip() {
        return _maxColumnsPerStrip;
    }
    public void setMaxColumnsPerString(int columns) { _maxColumnsPerStrip = columns; }

    public TileFormat getTileFormat() { return _tileFormat; }
    public void setTileFormat(TileFormat format) { _tileFormat = format; }

    public Color getTileBackgroundColor() { return _tileBackgroundColor; }
    public void setTileBackgroundColor(Color c) { _tileBackgroundColor = c; }

    public ZoomFactorStrategy getZoomFactorStrategy() { return _zoomFactorStrategy; }
    public void setZoomFactorStrategy(ZoomFactorStrategy strategy) { _zoomFactorStrategy = strategy; }

    public ExecutorService getIoExecutor() {
        return _ioExecutor;
    }

    public ExecutorService getLevelExecutor() {
        return _levelExecutor;
    }
}
