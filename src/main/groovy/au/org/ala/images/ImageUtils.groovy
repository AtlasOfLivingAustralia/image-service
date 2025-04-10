package au.org.ala.images

import com.google.common.hash.Hashing
import com.google.common.io.BaseEncoding
import com.google.common.io.Files
import groovy.util.logging.Slf4j

import java.awt.Color
import java.awt.Rectangle
import java.awt.geom.AffineTransform
import java.awt.image.AffineTransformOp
import java.awt.image.BufferedImage
import java.text.DecimalFormat
import java.util.regex.Pattern
import javax.imageio.ImageIO
import org.imgscalr.Scalr

@Slf4j
class ImageUtils {

    static int IMAGE_BUF_INIT_SIZE = 2 * 1024 * 1024
    static Pattern COLOR_PATTERN = Pattern.compile('^rgb[(]\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*[)]')

    static {
        ImageIO.useCache = false
    }

    static Rectangle getThumbBounds(int srcWidth, int srcHeight, int targetWidth, int targetHeight) {
        while (srcHeight > targetHeight || srcWidth > targetWidth) {
            if (srcHeight > targetHeight) {
                double ratio = (double) (targetHeight) / (double) srcHeight;
                srcHeight = targetHeight;
                srcWidth = (int) ((double) srcWidth * ratio);
            }

            if (srcWidth > targetWidth) {
                double ratio = (double) (targetWidth) / (double) srcWidth;
                srcWidth = targetWidth;
                srcHeight = (int) ((double) srcHeight * ratio);
            }
        }

        return new Rectangle(0, 0, srcWidth, srcHeight);
    }

    static getScaledHeight(int sourceWidth, int sourceHeight, int destWidth) {
        double ratio = (double) (destWidth) / (double) sourceWidth;
        return (int) (sourceHeight * ratio);
    }

    static BufferedImage bytesToImage(byte[] bytes) {
        ByteArrayInputStream bais = null
        try {
            bais = new ByteArrayInputStream(bytes)
            return ImageIO.read(new BufferedInputStream(bais))
        } finally {
            if (bais) {
                bais.close()
            }
        }
    }

    static byte[] imageToBytes(BufferedImage image) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(IMAGE_BUF_INIT_SIZE)
            ImageIO.write(image, "JPG", baos)
            return baos.toByteArray()
        } finally {
        }
    }

    static BufferedImage scaleWidth(BufferedImage src, int destWidth) {
        return Scalr.resize(src, Scalr.Method.QUALITY, destWidth, Scalr.OP_ANTIALIAS);
    }

    static BufferedImage scale(BufferedImage src, int destWidth, int destHeight) {
        return Scalr.resize(src, Scalr.Method.QUALITY, destWidth, destHeight, Scalr.OP_ANTIALIAS);
    }

    static Color parseColor(String colorString) {
        Color color;

        def m = COLOR_PATTERN.matcher(colorString)
        if (m.matches()) {
            int red, green, blue;
            red = Integer.parseInt(m.group(1))
            green = Integer.parseInt(m.group(2))
            blue = Integer.parseInt(m.group(3))
            return new Color(red, green, blue, 127 /* opacity todo: fix! */);
        }

        if (colorString.startsWith("#")) {
            colorString = colorString.substring(1);
        }
        if (colorString.endsWith(";")) {
            colorString = colorString.substring(0, colorString.length() - 1);
        }

        int red, green, blue, alpha;
        switch (colorString.length()) {
            case 8:
                red = Integer.parseInt(colorString.substring(0, 2), 16);
                green = Integer.parseInt(colorString.substring(2, 4), 16);
                blue = Integer.parseInt(colorString.substring(4, 6), 16);
                alpha = Integer.parseInt(colorString.substring(6, 8), 16);
                color = new Color(red, green, blue, alpha);
                break;

            case 6:
                red = Integer.parseInt(colorString.substring(0, 2), 16);
                green = Integer.parseInt(colorString.substring(2, 4), 16);
                blue = Integer.parseInt(colorString.substring(4, 6), 16);
                color = new Color(red, green, blue);
                break;
            case 3:
                red = Integer.parseInt(colorString.substring(0, 1), 16);
                green = Integer.parseInt(colorString.substring(1, 2), 16);
                blue = Integer.parseInt(colorString.substring(2, 3), 16);
                color = new Color(red, green, blue);
                break;
            case 1:
                red = green = blue = Integer.parseInt(colorString.substring(0, 1), 16);
                color = new Color(red, green, blue);
                break;
            default:
                throw new IllegalArgumentException("Invalid color: " + colorString);
        }
        return color;
    }

    static BufferedImage rotateImage(BufferedImage image, int degrees) {
        AffineTransform tx = new AffineTransform();
        tx.translate(image.getHeight() / 2, image.getWidth() / 2);
        tx.rotate(Math.toRadians(degrees));
        tx.translate(-image.getWidth() / 2, -image.getHeight() / 2);
        AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_BILINEAR);
        BufferedImage dest = new BufferedImage(image.getHeight(), image.getWidth(), image.getType());
        op.filter(image, dest);
        return dest
    }

    static String formatFileSize(double filesize) {
        def labels = [ ' bytes', 'KB', 'MB', 'GB' ]
        def label = labels.find { ( filesize < 1024 ) ? true : { filesize /= 1024 ; false }() } ?: 'TB'
        return "${new DecimalFormat( '0.#' ).format( filesize )} $label"
    }

    /**
     * Generates a lower case hex encoded MD5 hash of the given file's contents.  The resulting
     * string will always exactly 32 characters long.
     *
     * @param file The file to hash
     * @return The md5 hash of the file as a 32 character long hex string
     */
    static String generateMD5(final file) {
        return BaseEncoding.base16().lowerCase().encode(Files.asByteSource(file).hash(Hashing.md5()).asBytes()).padLeft(32, '0')
    }

    /**
     * Takes a String that could be a filename or a URI and returns the filename part if it is a URI or the original
     * string if it is not a URI.
     *
     * @param filenameOrUri
     * @return
     */
    static String getFilename(String filenameOrUri) {
        def filename
        try {
            def uri = filenameOrUri.toURI()
            if (uri.path) {
                filename = uri.path
                if (filename.contains('/')) {
                    def potential = filename.substring(filename.lastIndexOf('/') + 1)
                    if (potential) {
                        filename = potential
                    }
                }
            } else {
                filename = filenameOrUri
            }
        } catch (URISyntaxException ignored) {
            filename = filenameOrUri
        }
        return filename
    }
}
