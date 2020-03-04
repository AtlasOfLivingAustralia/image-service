package au.org.ala.images.tiling;

public interface ZoomFactorStrategy {

    int[] getZoomFactors(byte[] imageBytes);

}
