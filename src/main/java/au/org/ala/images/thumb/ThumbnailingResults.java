package au.org.ala.images.thumb;

import java.util.List;

public class ThumbnailingResults {

    private int _width;
    private int _height;
    private int _squareThumbSize;
    public List<String> _thumbnailNames;

    public ThumbnailingResults() {
    }

    public ThumbnailingResults(int width, int height, int squareThumbSize, List<String> thumbnailNames) {
        _height = height;
        _width = width;
        _squareThumbSize = squareThumbSize;
        _thumbnailNames = thumbnailNames;
    }

    public int getWidth() {
        return _width;
    }

    public int getHeight() {
        return _height;
    }

    public int getSquareThumbSize() {
        return _squareThumbSize;
    }

    public List<String> getThumbnailNames() {
        return _thumbnailNames;
    }
}
