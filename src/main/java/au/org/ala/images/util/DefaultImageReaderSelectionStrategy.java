package au.org.ala.images.util;

import javax.imageio.ImageReader;
import java.util.List;

public class DefaultImageReaderSelectionStrategy implements ImageReaderSelectionStrategy {

    public ImageReader selectImageReader(List<ImageReader> candidates) {

        if (candidates == null || candidates.size() == 0) {
            return null;
        }

        if (candidates.size() == 1) {
            return candidates.get(0);
        }

        ImageReader preferred = null;
        for (ImageReader reader : candidates) {
            if (reader.getClass().getCanonicalName().contains("twelvemonkeys")) {
                preferred = reader;
                break;
            }
        }
        if (preferred != null) {
            return preferred;
        }

        return candidates.get(0);

    }

}
