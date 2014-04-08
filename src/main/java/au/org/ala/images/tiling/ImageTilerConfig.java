package au.org.ala.images.tiling;

import java.awt.*;

public class ImageTilerConfig {

    private int _ioThreads = 2;
    private int _levelThreads = 2;
    private int _tileSize = 256;
    private int _maxColumnsPerStrip = 6;
    private TileFormat _tileFormat = TileFormat.JPEG;
    private Color _tileBackgroundColor = Color.gray;

    public ImageTilerConfig() {

    }

    public ImageTilerConfig(int ioThreads, int levelThreads, int tileSize, int maxColumnsPerStrip, TileFormat tileFormat) {
        _ioThreads = ioThreads;
        _levelThreads = levelThreads;
        _tileSize = tileSize;
        _maxColumnsPerStrip = maxColumnsPerStrip;
        _tileFormat = tileFormat;
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

}
