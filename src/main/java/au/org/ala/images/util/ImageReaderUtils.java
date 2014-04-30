package au.org.ala.images.util;

import org.apache.commons.io.FileUtils;
import sun.misc.IOUtils;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ImageReaderUtils {

    public static ImageReader findCompatibleImageReader(byte[] imageBytes) {
        return findCompatibleImageReader(imageBytes, new DefaultImageReaderSelectionStrategy());
    }

    public static ImageReader findCompatibleImageReader(byte[] imageBytes, ImageReaderSelectionStrategy selectionStrategy) {

        try {
            FastByteArrayInputStream fbis = new FastByteArrayInputStream(imageBytes);
            ImageInputStream iis = ImageIO.createImageInputStream(fbis);
            Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);
            ArrayList<ImageReader> candidates = new ArrayList<ImageReader>();
            while (iter.hasNext()) {
                ImageReader candidate = iter.next();
                try {
                    fbis = new FastByteArrayInputStream(imageBytes);
                    iis = ImageIO.createImageInputStream(fbis);
                    iis.mark();
                    candidate.setInput(iis);
                    int height = candidate.getHeight(0);
                    // if we get here, this reader should work
                    candidates.add(candidate);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            ImageReader result = null;
            if (selectionStrategy != null) {
                result = selectionStrategy.selectImageReader(candidates);
            } else {
                if (candidates.size() > 0) {
                    // pick the first one
                    result = candidates.get(0);
                }
            }

            try {
                if (result != null) {
                    fbis = new FastByteArrayInputStream(imageBytes);
                    iis = ImageIO.createImageInputStream(fbis);
                    result.setInput(iis);
                    return result;
                }
            } catch (IOException ioex) {
                throw new RuntimeException(ioex);
            }

        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
        return null;
    }

}
