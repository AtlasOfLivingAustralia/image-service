package au.org.ala.images.thumb;

import java.awt.*;
import java.util.List;

public class ThumbDefinition {

    public static final List<ThumbDefinition> DEFAULT_THUMBS = List.of(
            new ThumbDefinition(300, false, null, "thumbnail"),
            new ThumbDefinition(300, true, null, "thumbnail_square"),
            new ThumbDefinition(300, true, Color.black, "thumbnail_square_black"),
            new ThumbDefinition(300, true, Color.white, "thumbnail_square_white"),
            new ThumbDefinition(300, true, Color.darkGray, "thumbnail_square_darkGray"),
            ThumbDefinition.centreCrop(300, "thumbnail_centre_crop"),
            ThumbDefinition.centreCrop(650, "thumbnail_centre_crop_large"),
            new ThumbDefinition(650, false, null, "thumbnail_large"),
            new ThumbDefinition(1024, false, null, "thumbnail_xlarge")
    );
    private int _maximumDimension;
    private int _width;
    private boolean _square;
    private boolean _centreCrop;
    private Color _backgroundColor;
    private String _name;

    public ThumbDefinition(int maxDimension, boolean square, Color backgroundColor, String name) {
        _maximumDimension = maxDimension;
        _square = square;
        _backgroundColor = backgroundColor;
        _name = name;
        _width = -1;
        _centreCrop = false;
    }

    public ThumbDefinition(int maxDimension, int width, boolean square, boolean centreCrop, Color backgroundColor, String name) {
        _maximumDimension = maxDimension;
        _width = width;
        _square = square;
        _centreCrop = centreCrop;
        _backgroundColor = backgroundColor;
        _name = name;
    }

    public int getWidth() {
        return _width;
    }

    public int getMaximumDimension() {
        return _maximumDimension;
    }

    public boolean isSquare() {
        return _square;
    }

    public boolean isCentreCrop() {
        return _centreCrop;
    }

    public Color getBackgroundColor() {
        return _backgroundColor;
    }

    public String getName() {
        return _name;
    }

    static ThumbDefinition fixedWidth(int width, String name) {
        ThumbDefinition td = new ThumbDefinition(width, width, false, false, null, name);
        return td;
    }

    static ThumbDefinition centreCrop(int width, String name) {
        ThumbDefinition td = new ThumbDefinition(width, width, true, true, null, name);
        return td;
    }
}
