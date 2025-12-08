package au.org.ala.images.tiling;

import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.util.Iterator;

public class DefaultZoomFactorStrategy implements ZoomFactorStrategy {

    static final Logger log = LoggerFactory.getLogger(DefaultZoomFactorStrategy.class);

    int tileSize = 256;

    public DefaultZoomFactorStrategy() {

    }

    public DefaultZoomFactorStrategy(int tileSize) {
        this.tileSize = tileSize;
    }

    public int[] getZoomFactors(byte[] imageBytes) {


        int height = 0;
        int width = 0;
        try {
            var bis = UnsynchronizedByteArrayInputStream.builder().setByteArray(imageBytes).setOffset(0).get();
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

        return getZoomFactorsOld(height, width);
    }

    public int[] getZoomFactorsOld(int height, int width) {
        int zoomLevels = 8;


//        double heightLevels = Math.log((double)height / (double)tileSize) * INV_LOG2;
//        double widthLevels = Math.log((double)width / (double)tileSize) * INV_LOG2;
//
//        double levels = Math.max(0.0, Math.ceil(Math.max(heightLevels, widthLevels)));
//        // only produce a maximum of 8 levels (enough for a ~32k pixels to be shrunk to a 256 pixel tile)
//        int zoomLevels = Math.min(MAX_LEVELS, (int) levels) + 1;

        if (height < 1000 && width < 1000) {
            zoomLevels = 4;
        } else if (height < 3000 && width < 3000) {
            zoomLevels = 5;
        }

        int[] pyramid = new int[zoomLevels];

        for (int i = 0; i < zoomLevels; ++i) {
            pyramid[zoomLevels - i - 1] = (int) Math.pow(2, i);
        }

        return pyramid;
    }

    static final int MAX_LEVELS = 7;
    static final double INV_LOG2 = 1.0 / Math.log(2.0);

    @Override
    public int[] getZoomFactors(int height, int width) {
//        int zoomLevels = 8;

        double heightLevels = Math.log((double)height / (double)tileSize) * INV_LOG2;
        double widthLevels = Math.log((double)width / (double)tileSize) * INV_LOG2;

        double levels = Math.max(0.0, Math.ceil(Math.max(heightLevels, widthLevels)));

        // only produce a maximum of 8 levels (enough for a ~32k pixels to be shrunk to a 256 pixel tile)
        int zoomLevels = Math.min(MAX_LEVELS, (int) levels) + 1;

//        if (height < 1000 && width < 1000) {
//            zoomLevels = 4;
//        } else if (height < 3000 && width < 3000) {
//            zoomLevels = 5;
//        }

        int[] pyramid = new int[zoomLevels];

        for (int i = 0; i < zoomLevels; ++i) {
            pyramid[zoomLevels - i - 1] = (int) Math.pow(2, i);
        }

        return pyramid;
    }

}
