package au.org.ala.images.thumb;

import java.awt.*;

public class ThumbDefinition {

    private int _maximumDimension;
    private boolean _square;
    private Color _backgroundColor;
    private String _name;

    public ThumbDefinition(int maxDimension, boolean square, Color backgroundColor, String name) {
        _maximumDimension = maxDimension;
        _square = square;
        _backgroundColor = backgroundColor;
        _name = name;
    }

    public int getMaximumDimension() {
        return _maximumDimension;
    }

    public boolean isSquare() {
        return _square;
    }

    public Color getBackgroundColor() {
        return _backgroundColor;
    }

    public String getName() {
        return _name;
    }


}
