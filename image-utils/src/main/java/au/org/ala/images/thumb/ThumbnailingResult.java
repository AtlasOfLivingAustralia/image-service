package au.org.ala.images.thumb;

public class ThumbnailingResult {

    private int _width;
    private int _height;
    private boolean _isSquare;
    String _thumbnailName;

    public ThumbnailingResult() {
    }

    public ThumbnailingResult(int width, int height, boolean isSquare, String thumbnailName) {
        _height = height;
        _width = width;
        _isSquare = isSquare;
        _thumbnailName = thumbnailName;
    }

    public int getWidth() {
        return _width;
    }

    public int getHeight() {
        return _height;
    }

    public boolean isSquare() {
        return _isSquare;
    }

    public String getThumbnailName() {
        return _thumbnailName;
    }
}
