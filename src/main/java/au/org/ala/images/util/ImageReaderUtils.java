package au.org.ala.images.util;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.util.Iterator;

public class ImageReaderUtils {



    public static ImageReader findCompatibleImageReader(byte[] imageBytes) {
        try {
            FastByteArrayInputStream fbis = new FastByteArrayInputStream(imageBytes);
            ImageInputStream iis = ImageIO.createImageInputStream(fbis);
            iis.mark();
            Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);
            while (iter.hasNext()) {
                ImageReader candidate = iter.next();
                try {
                    iis.reset();
                    candidate.setInput(iis);
                    int height = candidate.getHeight(0);
                    // if we get here, this reader will work
                    return candidate;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }
        return null;
    }

    public static ImageReader findCompatibleImageReader(ImageInputStream iis) {
        iis.mark();
        Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);
        while (iter.hasNext()) {
            ImageReader candidate = iter.next();
            try {
                iis.reset();
                candidate.setInput(iis);
                int height = candidate.getHeight(0);
                // if we get here, this reader will work
                return candidate;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

}
