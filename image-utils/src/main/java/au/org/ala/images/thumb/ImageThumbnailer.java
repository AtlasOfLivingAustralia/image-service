package au.org.ala.images.thumb;

import au.org.ala.images.util.ByteSinkFactory;
import au.org.ala.images.util.DefaultImageReaderSelectionStrategy;
import au.org.ala.images.util.FileByteSinkFactory;
import au.org.ala.images.util.ImageReaderUtils;
import au.org.ala.images.util.ImageUtils;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.twelvemonkeys.image.AffineTransformOp;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class that can generate thumbnails of various sizes based on a {@link au.org.ala.images.thumb.ThumbDefinition} descriptor
 */
public class ImageThumbnailer {

    private static final Logger log = LoggerFactory.getLogger(ImageThumbnailer.class);

    private static final int MAX_THUMB_SIZE = 1024;

    private final RenderingHints renderingHints;

    public ImageThumbnailer() {
        this(RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR);
    }

    public ImageThumbnailer(Object interpolation) {
        renderingHints = new RenderingHints(RenderingHints.KEY_INTERPOLATION, interpolation);
    }

    public List<ThumbnailingResult> generateThumbnails(byte[] imageBytes, File destinationDirectory, List<ThumbDefinition> thumbDefs) throws IOException {
        return generateThumbnails(imageBytes, new FileByteSinkFactory(destinationDirectory), thumbDefs);
    }
    public List<ThumbnailingResult> generateThumbnails(byte[] imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
        return generateThumbnails(ByteSource.wrap(imageBytes), byteSinkFactory, thumbDefs, false);
    }

    public List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs, boolean useFileCache) throws IOException {

        List<ThumbnailingResult> results = new ArrayList<ThumbnailingResult>();

        // Open stream once and create ImageReader
        try (InputStream is = imageBytes.openBufferedStream()) {
            ImageInputStream iis = ImageIO.createImageInputStream(is);
            if (iis == null) {
                log.error("Failed to create ImageInputStream");
                IOUtils.consume(is);
                return results;
            }

            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                log.error("No image readers for image!");
                IOUtils.consume(is);
                return results;
            }

            // Use selection strategy to prefer TwelveMonkeys readers
            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                log.error("No suitable image reader selected!");
                IOUtils.consume(is);
                return results;
            }
            reader.setInput(iis, true, false); // Set ignoreMetadata to false to allow reading metadata

            generateThumbnailsInternal(byteSinkFactory, thumbDefs, reader, results, null);
        }
        return results;
    }

    /**
     * Generate thumbnails without encoding the image to an intermediate image with exif orientation applied / removed.
     * In theory this will be faster for large images as it avoids the intermediate encode.
     * @param imageBytes
     * @param byteSinkFactory
     * @param thumbDefs
     * @return
     * @throws IOException
     */
    public List<ThumbnailingResult> generateThumbnailsNoIntermediateEncode(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {

        List<ThumbnailingResult> results = new ArrayList<ThumbnailingResult>();

        // Open stream once and create ImageReader
        try (InputStream is = imageBytes.openBufferedStream()) {
            // Mark the buffered stream with a reasonable limit (128KB is typically enough for EXIF metadata)
            if (!is.markSupported()) {
                throw new IOException("Stream does not support mark/reset");
            }
            is.mark(ImageReaderUtils.METADATA_BUFFER_SIZE);

            ImageInputStream iis = ImageIO.createImageInputStream(is);
            if (iis == null) {
                log.error("Failed to create ImageInputStream");
                IOUtils.consume(is);
                return results;
            }

            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                log.error("No image readers for image!");
                IOUtils.consume(is);
                return results;
            }

            // Use selection strategy to prefer TwelveMonkeys readers
            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                log.error("No suitable image reader selected!");
                IOUtils.consume(is);
                return results;
            }
            reader.setInput(iis, true, false); // Set ignoreMetadata to false to allow reading orientation metadata

            // Try to get orientation from ImageReader metadata first
            ImageReaderUtils.Orientation orientation = null;
            try {
                IIOMetadata metadata = reader.getImageMetadata(0);
                orientation = ImageReaderUtils.findImageOrientation(metadata);
            } catch (Exception e) {
                log.debug("Could not read orientation from ImageReader metadata", e);
            }

            // If orientation is Normal or couldn't be determined, try using metadata library
            if (orientation == null || orientation == ImageReaderUtils.Orientation.Normal) {
                try {
                    // Reset stream to beginning
                    is.reset();
                    is.mark(ImageReaderUtils.METADATA_BUFFER_SIZE);

                    // Try with metadata-extractor library
                    com.drew.metadata.Metadata metadata = com.drew.imaging.ImageMetadataReader.readMetadata(is);
                    var exifDirectories = metadata.getDirectoriesOfType(com.drew.metadata.exif.ExifIFD0Directory.class);
                    if (!exifDirectories.isEmpty()) {
                        for (var exif : exifDirectories) {
                            if (exif.containsTag(com.drew.metadata.exif.ExifIFD0Directory.TAG_ORIENTATION)) {
                                orientation = ImageReaderUtils.Orientation.fromExifOrientation(exif.getInt(com.drew.metadata.exif.ExifIFD0Directory.TAG_ORIENTATION));
                                break;
                            }
                        }
                    }

                    // Reset stream again for ImageReader
                    is.reset();

                    // Recreate ImageInputStream and reader with fresh stream position
                    iis = ImageIO.createImageInputStream(is);
                    reader.dispose();
                    readers = ImageIO.getImageReaders(iis);
                    reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
                    if (reader != null) {
                        reader.setInput(iis, true, false); // Set ignoreMetadata to false
                    }
                } catch (Exception e) {
                    log.debug("Could not read orientation from metadata library", e);
                }
            }

            generateThumbnailsInternal(byteSinkFactory, thumbDefs, reader, results, orientation);
        }
        return results;
    }

    private void generateThumbnailsInternal(ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs, ImageReader reader, List<ThumbnailingResult> results, ImageReaderUtils.Orientation orientation) throws IOException {
        BufferedImage thumbSrc;
        try {
            ImageReadParam imageParams = reader.getDefaultReadParam();
            int height = reader.getHeight(0);
            int width = reader.getWidth(0);

            // Big images need to be thumbed via ImageReader to maintain O(1) heap use
            if (height > MAX_THUMB_SIZE || width > MAX_THUMB_SIZE) {
                // roughly scale (subsample) the image to a max dimension of 1024
                int ratio;
                if (height >= width) {
                    ratio = height / MAX_THUMB_SIZE;
                } else {
                    ratio = width / MAX_THUMB_SIZE;
                }

                ratio = ratio == 0 ? 1 : ratio;

                imageParams.setSourceSubsampling(ratio, ratio, 0, 0);
                var inputSrc = reader.read(0, imageParams);
                // apply orientation if needed
                thumbSrc = applyOrientation(orientation, inputSrc);
                inputSrc.flush();

                // if we have multiple thumbnails to generate then prescaling the image to this max size
                // will make the whole operation faster but trade off is that the image will be lower quality
                if (thumbDefs.size() > 1) {
                    var scaledThumb = ImageUtils.scaleWidth(thumbSrc, MAX_THUMB_SIZE);
                    thumbSrc.flush();
                    thumbSrc = scaledThumb;
                }
            } else {
                // small images
                var inputSrc = reader.read(0);
                // apply orientation if needed
                thumbSrc = applyOrientation(orientation, inputSrc);
                inputSrc.flush();

                // if we have multiple thumbnails to generate then prescaling the image to this max size
                // will make the whole operation faster but trade off is that the image will be lower quality
                if (thumbDefs.size() > 1) {
                    // TODO to preserve quality we should probably find some happy medium that is a multiple of all
                    // the thumb sizes
                    var scaledThumb = ImageUtils.scaleWidth(thumbSrc, MAX_THUMB_SIZE);
                    thumbSrc.flush();
                    thumbSrc = scaledThumb;
                }
            }
        } finally {
            var input = reader.getInput();
            if (input instanceof Closeable) {
                try {
                    ((Closeable) input).close();
                } catch (IOException e) {
                    // ignore
                }
            }
            reader.dispose();
        }

        if (thumbSrc != null) {
            for (ThumbDefinition thumbDef : thumbDefs) {
                ByteSink destination = byteSinkFactory.getByteSinkForNames(thumbDef.getName());
                // workout if we need to be a transparent png or if jpg will do...
                Color backgroundColor = thumbDef.getBackgroundColor();
                int size = thumbDef.getMaximumDimension();
                boolean isPNG = false;
                BufferedImage thumbImage;

                Graphics g = null;
                try {
                    if (thumbDef.isSquare() && thumbDef.isCentreCrop()) {
                        // centre crop the image to a square
                        if (thumbSrc.getHeight() == thumbSrc.getWidth() && thumbSrc.getHeight() == size) {
                            // already a square of the right size
                            thumbImage = new BufferedImage(size, size, BufferedImage.TYPE_3BYTE_BGR);
                            g = thumbImage.getGraphics();
                            g.drawImage(thumbSrc, 0, 0, null);

                            thumbSrc.flush();
                        } else {
                            int cropSize = Math.min(thumbSrc.getHeight(), thumbSrc.getWidth());
                            int x = (thumbSrc.getWidth() - cropSize) / 2;
                            int y = (thumbSrc.getHeight() - cropSize) / 2;

                            BufferedImage croppedThumb = thumbSrc.getSubimage(x, y, cropSize, cropSize);
                            BufferedImage scaledThumb = ImageUtils.scaleWidth(croppedThumb, size);
                            thumbSrc.flush();
                            croppedThumb.flush();

                            thumbImage = new BufferedImage(size, size, BufferedImage.TYPE_3BYTE_BGR);
                            g = thumbImage.getGraphics();
                            g.drawImage(scaledThumb, 0, 0, null);

                            scaledThumb.flush();
                        }
                    } else if (size == -1 && thumbDef.getWidth() != -1) {
                        BufferedImage scaledThumb = ImageUtils.scaleWidth(thumbSrc, thumbDef.getWidth());
                        thumbSrc.flush();
                        thumbImage = new BufferedImage(scaledThumb.getWidth(), scaledThumb.getHeight(), BufferedImage.TYPE_3BYTE_BGR);
                        g = thumbImage.getGraphics();
                        g.drawImage(scaledThumb, 0, 0, null);
                        scaledThumb.flush();
                    } else {

                        BufferedImage scaledThumb = ImageUtils.scaleWidth(thumbSrc, size);
                        thumbSrc.flush();
                        int thumbHeight = scaledThumb.getHeight();
                        int thumbWidth = scaledThumb.getWidth();


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
                        scaledThumb.flush();
                    }
                } finally {
                    if (g != null) {
                        g.dispose();
                    }
                }

                if (thumbImage != null) {
                    boolean result = false;
                    try (OutputStream thumbOutputStream = destination.openStream()) {
                        result = ImageIO.write(thumbImage, isPNG ? "PNG" : "JPG", thumbOutputStream);
                    }
                    thumbImage.flush();
                    if (result) {
                        results.add(new ThumbnailingResult(thumbImage.getWidth(), thumbImage.getHeight(), thumbDef.isSquare(), thumbDef.getName()));
                    }
                }
            }
        }
    }

    private BufferedImage applyOrientation(ImageReaderUtils.Orientation orientation, BufferedImage inputSrc) {
        BufferedImage thumbSrc = inputSrc;
        if (orientation != null && orientation != ImageReaderUtils.Orientation.Normal) {
//            for (var rotation : orientation.getScalrRotations()) {
//                thumbSrc = Scalr.rotate(thumbSrc, rotation);
//            }
//            Scalr.apply(thumbSrc, Scalr.OP_ANTIALIAS);

            thumbSrc = new AffineTransformOp(
                    orientation.getAffineTransform(inputSrc.getWidth(), inputSrc.getHeight()).orElseThrow( () -> new IllegalArgumentException("No affine transform for orientation " + orientation)),
                    renderingHints
            ).filter(inputSrc, null);
        }
        return thumbSrc;
    }

}
