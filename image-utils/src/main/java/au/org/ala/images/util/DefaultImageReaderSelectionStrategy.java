package au.org.ala.images.util;

import javax.imageio.ImageReader;
import java.util.Iterator;
import java.util.List;

public class DefaultImageReaderSelectionStrategy implements ImageReaderSelectionStrategy {

    public static final DefaultImageReaderSelectionStrategy INSTANCE = new DefaultImageReaderSelectionStrategy();

    public ImageReader selectImageReader(Iterator<ImageReader> candidates) {

        if (candidates == null) {
            return null;
        }

        ImageReader first = null;


        ImageReader preferred = null;
        while (candidates.hasNext()) {
            ImageReader reader = candidates.next();
            if (first == null) {
                first = reader;
            }
            if (reader.getClass().getCanonicalName().contains("twelvemonkeys")) {
                preferred = reader;
                break;
            }
        }

        return preferred != null ? preferred : first;
    }

}
