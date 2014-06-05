package au.org.ala.images.thumb;

import au.org.ala.images.util.CodeTimer;
import au.org.ala.images.util.ImageReaderUtils;
import au.org.ala.images.util.ImageUtils;
import org.apache.commons.lang.StringUtils;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ImageThumbnailer {

    private File createThumbnailFile(File parent) {
        return new File(parent.getAbsolutePath() + "/thumbnail");
    }

    private File createSquareThumbnailFile(File parent, String backgroundColor) {
        if (StringUtils.isEmpty(backgroundColor)) {
            return new File(String.format("%s/thumbnail_square", parent.getAbsolutePath()));
        } else {
            return new File(String.format("%s/thumbnail_square_%s", parent.getAbsolutePath(), backgroundColor));
        }
    }

    public ThumbnailingResults generateThumbnails(byte[] imageBytes, File destinationDirectory, int size, String[] backgroundColors) throws IOException {
        CodeTimer t = new CodeTimer("Thumbnail generation");
        ImageReader reader = ImageReaderUtils.findCompatibleImageReader(imageBytes);
        int thumbHeight = 0, thumbWidth = 0;
        List<String> names = new ArrayList<String>();
        if (reader != null) {
            ImageReadParam imageParams = reader.getDefaultReadParam();
            int height = reader.getHeight(0);
            int width = reader.getWidth(0);

            BufferedImage thumbImage;

            // Big images need to be thumbed via ImageReader to maintain O(1) heap use
            if (height > 1024 || width > 1024) {

                // roughly scale (subsample) the image to a max dimension of 1024
                int ratio;
                if (height > width) {
                    ratio = height / 1024;
                } else {
                    ratio = width / 1024;
                }

                imageParams.setSourceSubsampling(ratio, ratio == 0 ? 1 : ratio, 0, 0);
                thumbImage = reader.read(0, imageParams);

                // then finely scale the sub sampled image to get the final thumbnail
                thumbImage = ImageUtils.scaleWidth(thumbImage, size);
            } else {
                // small images
                thumbImage = reader.read(0);
                thumbImage = ImageUtils.scaleWidth(thumbImage, size);
            }

            if (thumbImage != null) {
                thumbHeight = thumbImage.getHeight();
                thumbWidth = thumbImage.getWidth();

                File thumbFile = createThumbnailFile(destinationDirectory);

                BufferedImage temp = new BufferedImage(thumbImage.getWidth(), thumbImage.getHeight(), BufferedImage.TYPE_3BYTE_BGR);
                Graphics g = temp.getGraphics();
                try {
                    g.drawImage(thumbImage, 0, 0, null);
                    ImageIO.write(temp, "JPG", thumbFile);
                    names.add(thumbFile.getName());
                } finally {
                    g.dispose();
                }

                // for each background color (where empty string means transparent), create a squared thumb
                // If not transparent, keep as jpeg for speed/space!
                for (String colorName : backgroundColors) {
                    Color backgroundColor = null;
                    if (StringUtils.isNotEmpty(colorName)) {
                        try {
                            Field field = Color.class.getField(colorName);
                            backgroundColor = (Color)field.get(null);
                        } catch (Exception e) {
                            backgroundColor = null; // Not defined
                        }
                    }

                    if (backgroundColor == null) {
                        temp = new BufferedImage(size, size, BufferedImage.TYPE_4BYTE_ABGR);
                    } else {
                        temp = new BufferedImage(size, size, BufferedImage.TYPE_3BYTE_BGR);
                    }

                    g = temp.getGraphics();
                    try {
                        if (StringUtils.isNotEmpty(colorName) && backgroundColor != null) {
                            g.setColor(backgroundColor);
                            g.fillRect(0, 0, size, size);
                        }

                        if (thumbHeight < size) {
                            int top = (size / 2) - (thumbHeight / 2);
                            g.drawImage(thumbImage, 0, top, null);
                        } else if (thumbWidth < size) {
                            int left = (size / 2) - (thumbWidth / 2);
                            g.drawImage(thumbImage, left, 0, null);
                        } else {
                            g.drawImage(thumbImage, 0, 0, null);
                        }

                        thumbFile = createSquareThumbnailFile(destinationDirectory, colorName);
                        if (StringUtils.isNotEmpty(colorName) && backgroundColor != null) {
                            ImageIO.write(temp, "JPG", thumbFile);
                        } else {
                            ImageIO.write(temp, "PNG", thumbFile);
                        }
                        names.add(thumbFile.getName());
                    } finally {
                        g.dispose();
                    }
                }
            }
        } else {
            System.err.println("No image readers for image ${imageIdentifier}!");
        }
        t.stop(true);
        return new ThumbnailingResults(thumbWidth, thumbHeight, size, names);
    }

}
