package au.org.ala.images.tiling;

public class ImageTilerResults {

    private boolean _success;
    private int _zoomLevels;

    public ImageTilerResults(boolean success, int zoomLevels) {
        _success = success;
        _zoomLevels = zoomLevels;
    }

    public void setZoomLevels(int zoomLevels) {
        _zoomLevels = zoomLevels;
    }

    public int getZoomLevels() {
        return _zoomLevels;
    }

    public void setSuccess(boolean success) {
        _success = success;
    }

    public boolean getSuccess() {
        return _success;
    }

    @Override
    public String toString() {
        return "ImageTilerResults{_success=" + _success +
                ", _zoomLevels=" + _zoomLevels +
                '}';
    }
}
