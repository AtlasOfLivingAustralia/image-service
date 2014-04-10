package au.org.ala.images.tiling;

import java.io.File;

public interface ZoomFactorStrategy {

    public int[] getZoomFactors(File imageFile, byte[] imageBytes);

}
