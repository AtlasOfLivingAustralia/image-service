package au.org.ala.images.thumb;

import au.org.ala.images.util.CodeTimer;
import au.org.ala.images.util.ImageReaderUtils;
import au.org.ala.images.util.ImageUtils;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that can generate thumbnails of various sizes based on a {@link au.org.ala.images.thumb.ThumbDefinition} descriptor
 */
public class ImageThumbnailer {

    private static final int MAX_THUMB_SIZE = 1024;

    protected File createThumbnailFile(File parent, String name) {
        return new File(String.format("%s/%s", parent.getAbsolutePath(), name));
    }

    public List<ThumbnailingResult> generateThumbnails(byte[] imageBytes, File destinationDirectory, List<ThumbDefinition> thumbDefs) throws IOException {
        ImageReader reader = ImageReaderUtils.findCompatibleImageReader(imageBytes);
        List<ThumbnailingResult> results = new ArrayList<ThumbnailingResult>();
        if (reader != null) {
            ImageReadParam imageParams = reader.getDefaultReadParam();
            int height = reader.getHeight(0);
            int width = reader.getWidth(0);
            BufferedImage thumbSrc;
            // Big images need to be thumbed via ImageReader to maintain O(1) heap use
            if (height > MAX_THUMB_SIZE || width > MAX_THUMB_SIZE) {
                // roughly scale (subsample) the image to a max dimension of 1024
                int ratio;
                if (height > width) {
                    ratio = height / MAX_THUMB_SIZE;
                } else {
                    ratio = width / MAX_THUMB_SIZE;
                }

                imageParams.setSourceSubsampling(ratio, ratio == 0 ? 1 : ratio, 0, 0);
                thumbSrc = reader.read(0, imageParams);

                // then finely scale the sub sampled image to get the final thumbnail
                thumbSrc = ImageUtils.scaleWidth(thumbSrc, MAX_THUMB_SIZE);
            } else {
                // small images
                thumbSrc = reader.read(0);
                thumbSrc = ImageUtils.scaleWidth(thumbSrc, MAX_THUMB_SIZE);
            }

            if (thumbSrc != null) {
                for (ThumbDefinition thumbDef : thumbDefs) {
                    File thumbFile = createThumbnailFile(destinationDirectory, thumbDef.getName());
                    // workout if we need to be a transparent png or if jpg will do...
                    Color backgroundColor = thumbDef.getBackgroundColor();
                    int size = thumbDef.getMaximumDimension();
                    boolean isPNG = false;
                    BufferedImage thumbImage;

                    BufferedImage scaledThumb = ImageUtils.scaleWidth(thumbSrc, size);
                    int thumbHeight = scaledThumb.getHeight();
                    int thumbWidth = scaledThumb.getWidth();

                    Graphics g = null;
                    try {
                        if (!thumbDef.isSquare()) {
                            // Need to paint anyway, as the source bytes might contain an alpha channel,
                            // and this is going to JPG with no transparency - this avoids weird colouration effects.
                            thumbImage = new BufferedImage(scaledThumb.getWidth(), scaledThumb.getHeight(), BufferedImage.TYPE_3BYTE_BGR);
                            g = thumbImage.getGraphics();
                            g.drawImage(scaledThumb, 0, 0, null);
                        } else {
                            if (backgroundColor == null) {
                                isPNG = true;
                                thumbImage = new BufferedImage(size, size, BufferedImage.TYPE_4BYTE_ABGR);
                                g = thumbImage.getGraphics();
                            } else {
                                thumbImage = new BufferedImage(size, size, BufferedImage.TYPE_3BYTE_BGR);
                                g = thumbImage.getGraphics();
                                g.setColor(backgroundColor);
                                g.fillRect(0, 0, size, size);
                            }
                            // Draw the non-square image centered on the square target
                            if (thumbHeight < size) {
                                int top = (size / 2) - (thumbHeight / 2);
                                g.drawImage(scaledThumb, 0, top, null);
                            } else if (thumbWidth < size) {
                                int left = (size / 2) - (thumbWidth / 2);
                                g.drawImage(scaledThumb, left, 0, null);
                            } else {
                                g.drawImage(scaledThumb, 0, 0, null);
                            }
                        }
                    } finally {
                        if (g != null) {
                            g.dispose();
                        }
                    }

                    if (thumbImage != null) {
                        ImageIO.write(thumbImage, isPNG ? "PNG" : "JPG", thumbFile);
                        results.add(new ThumbnailingResult(thumbImage.getWidth(), thumbImage.getHeight(), thumbDef.isSquare(), thumbDef.getName()));
                    }
                }
            }
        } else {
            System.err.println("No image readers for image ${imageIdentifier}!");
        }
        return results;
    }

}
