package au.org.ala.images.util;

import org.imgscalr.Scalr;
import java.awt.image.BufferedImage;

public class ImageUtils {

    public static BufferedImage scaleWidth(BufferedImage src, int destWidth) {
        return Scalr.resize(src, Scalr.Method.QUALITY, destWidth, Scalr.OP_ANTIALIAS);
    }

    public static BufferedImage scale(BufferedImage src, int destWidth, int destHeight) {
        return Scalr.resize(src, Scalr.Method.QUALITY, destWidth, destHeight, Scalr.OP_ANTIALIAS);
    }

}
