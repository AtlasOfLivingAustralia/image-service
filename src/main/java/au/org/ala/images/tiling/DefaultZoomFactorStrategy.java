package au.org.ala.images.tiling;

import au.org.ala.images.util.FastByteArrayInputStream;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.util.Iterator;

public class DefaultZoomFactorStrategy implements ZoomFactorStrategy {

    public int[] getZoomFactors(File imageFile, byte[] imageBytes) {
        int zoomLevels = 8;

        int height = 0;
        int width = 0;
        try {
            FastByteArrayInputStream bis = new FastByteArrayInputStream(imageBytes, imageBytes.length);
            ImageInputStream iis = ImageIO.createImageInputStream(bis);
            Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);

            if (iter.hasNext()) {
                ImageReader reader = iter.next();
                reader.setInput(iis);

                height = reader.getHeight(0);
                width = reader.getWidth(0);

                reader.dispose();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        if (height < 3000 && width < 3000) {
            zoomLevels = 5;
        }
        int[] pyramid = new int[zoomLevels];

        for (int i = 0; i < zoomLevels; ++i) {
            pyramid[zoomLevels - i - 1] = (int) Math.pow(2, i);
        }

        return pyramid;
    }
}
