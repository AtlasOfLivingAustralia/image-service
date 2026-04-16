package au.org.ala.images.util;

import javax.imageio.ImageReader;
import java.util.Iterator;
import java.util.List;

public interface ImageReaderSelectionStrategy {

    default ImageReader selectImageReader(Iterable<ImageReader> candidates) {
        return selectImageReader(candidates.iterator());
    }
    ImageReader selectImageReader(Iterator<ImageReader> candidates);

}
