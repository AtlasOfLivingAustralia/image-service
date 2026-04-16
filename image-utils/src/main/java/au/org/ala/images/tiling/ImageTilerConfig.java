package au.org.ala.images.tiling;

import java.awt.*;
import java.util.concurrent.Executor;

public class ImageTilerConfig {

    private final Executor _ioExecutor;
    private final Executor _levelExecutor;
    private final int _tileSize;
    private final int _maxColumnsPerStrip;
    private final TileFormat _tileFormat;
    private final Color _tileBackgroundColor;
    private final ZoomFactorStrategy _zoomFactorStrategy;

    public ImageTilerConfig(Executor ioExecutor, Executor levelExecutor) {
        this(ioExecutor, levelExecutor, 256, 6, TileFormat.JPEG, new Color(221, 221, 221));
    }

    public ImageTilerConfig(Executor ioExecutor, Executor levelExecutor, int tileSize, int maxColumnsPerStrip, TileFormat tileFormat) {
        this(ioExecutor, levelExecutor, tileSize, maxColumnsPerStrip, tileFormat, new Color(221, 221, 221));
    }

    public ImageTilerConfig(Executor ioExecutor, Executor levelExecutor, int tileSize, int maxColumnsPerStrip, TileFormat tileFormat, Color tileBackgroundColor) {
        this._tileSize = tileSize;
        this._maxColumnsPerStrip = maxColumnsPerStrip;
        this._tileFormat = tileFormat;
        this._tileBackgroundColor = tileBackgroundColor;
        this._ioExecutor = ioExecutor;
        this._levelExecutor = levelExecutor;
        this._zoomFactorStrategy = new DefaultZoomFactorStrategy(_tileSize);
    }

    public int getTileSize() {
        return _tileSize;
    }

    public int getMaxColumnsPerStrip() {
        return _maxColumnsPerStrip;
    }

    public TileFormat getTileFormat() { return _tileFormat; }

    public Color getTileBackgroundColor() { return _tileBackgroundColor; }

    public ZoomFactorStrategy getZoomFactorStrategy() { return _zoomFactorStrategy; }

    public Executor getIoExecutor() {
        return _ioExecutor;
    }

    public Executor getLevelExecutor() {
        return _levelExecutor;
    }
}
