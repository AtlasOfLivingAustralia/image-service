package au.org.ala.images.util;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.util.Iterator;

public class ImageReaderUtils {

    public static ImageReader findCompatibleImageReader(ImageInputStream iis) {
        Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);
        while (iter.hasNext()) {
            ImageReader candidate = iter.next();
            try {
                candidate.setInput(iis);
                int height = candidate.getHeight(0);
                // if we get here, this reader will work
                candidate.reset();
                return candidate;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

}
