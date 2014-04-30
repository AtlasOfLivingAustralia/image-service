package au.org.ala.images.util;

import javax.imageio.ImageReader;
import java.util.List;

public interface ImageReaderSelectionStrategy {

    public ImageReader selectImageReader(List<ImageReader> candidates);

}
