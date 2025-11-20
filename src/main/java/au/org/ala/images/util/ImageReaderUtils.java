package au.org.ala.images.util;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.MetadataException;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.drew.metadata.jpeg.JpegDirectory;
import com.google.common.base.MoreObjects;
import com.google.common.io.ByteSource;
import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata;
import org.apache.commons.imaging.formats.tiff.constants.TiffTagConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.metadata.IIOMetadataFormatImpl;
import javax.imageio.metadata.IIOMetadataNode;
import javax.imageio.stream.ImageInputStream;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class ImageReaderUtils {

    protected static Logger logger = LoggerFactory.getLogger(ImageReaderUtils.class);

    public static ImageReader findCompatibleImageReader(byte[] imageBytes) {
        return findCompatibleImageReader(imageBytes, DefaultImageReaderSelectionStrategy.INSTANCE);
    }

    public static ImageReader findCompatibleImageReader(ByteSource imageBytes, boolean useFileCache) {
        return findCompatibleImageReader(imageBytes, DefaultImageReaderSelectionStrategy.INSTANCE, useFileCache);
    }

    public static ImageReader findCompatibleImageReader(byte[] imageBytes, ImageReaderSelectionStrategy selectionStrategy) {
//        FastByteArrayInputStream fbis = new FastByteArrayInputStream(imageBytes);
        return findCompatibleImageReader(ByteSource.wrap(imageBytes), selectionStrategy, true);
    }

    static public class ImageReaderAndOrientation {
        public final ImageReader reader;
        public final Orientation orientation;
        public final Dimension dimensions;

        public ImageReaderAndOrientation(ImageReader reader, Orientation orientation, Dimension dimensions) {
            this.reader = reader;
            this.orientation = orientation;
            this.dimensions = dimensions;
        }
    }

    public static ImageReaderAndOrientation findCompatibleImageReaderAndGetOrientation(ByteSource byteSource, String filename) {
        return findCompatibleImageReaderAndGetOrientation(byteSource, filename, DefaultImageReaderSelectionStrategy.INSTANCE);
    }

    public static ImageReaderAndOrientation findCompatibleImageReaderAndGetOrientation(ByteSource byteSource, String filename, ImageReaderSelectionStrategy selectionStrategy) {

        try {
            ImageReader result = getImageReaderFromSelectionStrategy(byteSource, selectionStrategy);

            try {
                if (result != null) {
                    var dimensions = getImageDimensionsAndOrientation(byteSource, filename, selectionStrategy);
                    result.setInput(ImageIO.createImageInputStream(byteSource.openBufferedStream()));
                    return new ImageReaderAndOrientation(result, dimensions.getLeft(), dimensions.getRight());
                }
            } catch (Exception ioex) {
                throw new RuntimeException(ioex);
            }

        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
        return null;
    }

    public static ImageReader findCompatibleImageReader(ByteSource byteSource, ImageReaderSelectionStrategy selectionStrategy, boolean useFileCache) {

        try {
//            FastByteArrayInputStream fbis = new FastByteArrayInputStream(imageBytes);
            ImageReader result = getImageReaderFromSelectionStrategy(byteSource, selectionStrategy);

            try {
                if (result != null) {
//                    if (true){
                        result.setInput(rotate(byteSource, useFileCache));
                        return result;
//                    } else {
//                        FastByteArrayInputStream fbis2 = new FastByteArrayInputStream(imageBytes);
//                        ImageInputStream iis2 =  ImageIO.createImageInputStream(fbis2);
//                        result.setInput(iis2);
//                        return result;
//                    }
                }
            } catch (Exception ioex) {
                throw new RuntimeException(ioex);
            }

        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
        return null;
    }

    private static ImageReader getImageReaderFromSelectionStrategy(ByteSource byteSource, ImageReaderSelectionStrategy selectionStrategy) throws IOException {
        Iterator<ImageReader> iter;
        try (ImageInputStream iis = ImageIO.createImageInputStream(byteSource.openStream())) {
            iter = ImageIO.getImageReaders(iis);
        }
        ArrayList<ImageReader> candidates = new ArrayList<ImageReader>();
        while (iter.hasNext()) {
            ImageReader candidate = iter.next();
            try {
//                    fbis = new FastByteArrayInputStream(imageBytes);
                try (ImageInputStream iis = ImageIO.createImageInputStream(byteSource.openStream())) {
                    iis.mark();
                    candidate.setInput(iis);
                    int height = candidate.getHeight(0);
                    logger.trace("ImageReader: {} height: {}", candidate.getClass().getCanonicalName(), height);
                }
                // if we get here, this reader should work
                candidates.add(candidate);
            } catch (Exception ex) {
                logger.info("Exception evaluating ImageReader", ex);
            }
        }

        ImageReader result = null;
        if (selectionStrategy != null) {
            result = selectionStrategy.selectImageReader(candidates);
        } else {
            if (!candidates.isEmpty()) {
                // pick the first one
                result = candidates.get(0);
            }
        }

        return result;
    }

    public static ImageInputStream rotate(ByteSource imageBytes, boolean useFileCache) throws IOException {

        try {

            var dimensions = getDimensionsWithMetadataReader(imageBytes);

            if (dimensions == null) {
                return ImageIO.createImageInputStream(imageBytes.openBufferedStream());
            }

            var orientation = dimensions.getLeft();
            int width = dimensions.getRight().width;
            int height = dimensions.getRight().height;
            logger.debug("orientation= {}, width={}, height={}", orientation, width, height);


            if (orientation != Orientation.Normal) {

                BufferedImage originalImage;
                try (InputStream bis2 = imageBytes.openBufferedStream()) {
                    originalImage = ImageIO.read(bis2);
                }
                AffineTransform affineTransform = orientation.getAffineTransform(
                        originalImage.getWidth(),
                        originalImage.getHeight()
                ).orElseThrow(() -> new RuntimeException("No AffineTransform found for orientation " + orientation));
                AffineTransformOp affineTransformOp = new AffineTransformOp(affineTransform, AffineTransformOp.TYPE_NEAREST_NEIGHBOR); // TYPE_BILINEAR is slow and we should have a 1:1 copy
                BufferedImage destinationImage = new BufferedImage(originalImage.getHeight(), originalImage.getWidth(), originalImage.getType());
                destinationImage = affineTransformOp.filter(originalImage, destinationImage);

                logger.debug("Transformed - width={}, height={}", destinationImage.getWidth(), destinationImage.getHeight());
                if (useFileCache) {
                    Path tempFile = Files.createTempFile("image", ".jpg");
                    File file = tempFile.toFile();
                    ImageIO.write(destinationImage, "jpg", file);
                    file.deleteOnExit();
                    return ImageIO.createImageInputStream(new RandomAccessFile(file, "r"));
                } else {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ImageIO.write(destinationImage, "jpg", baos);
                    baos.flush();
                    byte[] imageInByte = baos.toByteArray();
                    baos.close();
                    return ImageIO.createImageInputStream(new FastByteArrayInputStream(imageInByte));
                }
            } else {
                return ImageIO.createImageInputStream(imageBytes.openBufferedStream());
            }
        } catch (Exception e){
            return ImageIO.createImageInputStream(imageBytes.openBufferedStream());
        }
    }

    /**
     * Get the image dimensions, respecting the orientation tag if present.
     * @param imageBytes A byte source for the image
     *                   (e.g. {@link com.google.common.io.Files#asByteSource(java.io.File)})
     *                   or {@link com.google.common.io.ByteSource#wrap(byte[])}
     * @param filename The filename of the image
     * @return The dimensions of the image or null if the dimensions could not be determined
     */
    public static Dimension getImageDimensions(ByteSource imageBytes, String filename) {
        return getImageDimensions(imageBytes, filename, DefaultImageReaderSelectionStrategy.INSTANCE);
    }

    public static Dimension getImageDimensions(ByteSource imageBytes, String filename, ImageReaderSelectionStrategy selectionStrategy) {
        var dimensions = getImageDimensionsAndOrientation(imageBytes, filename, selectionStrategy);
        return dimensions != null ? dimensions.getRight() : null;
    }

    public static Pair<Orientation, Dimension> getImageDimensionsAndOrientation(ByteSource imageBytes, String filename, ImageReaderSelectionStrategy selectionStrategy) {

        // prefer using the drew noakes metadata reader library as it gives better
        // results for the test images
        var dimensions = getDimensionsWithMetadataReader(imageBytes);
        if (dimensions != null) {
            return dimensions;
        }

        // this can throw if it can't process the image metadata
        try {
            dimensions = getDimensionsWithCommonsImaging(imageBytes, filename);
            if (dimensions != null) {
                return dimensions;
            }
        } catch (Exception e) {
            logger.info("Could not get image dimensions using commons imaging for {}, exception message {}", filename, e.getMessage());
        }


        dimensions = getDimensionsWithImageIOMetadata(imageBytes, selectionStrategy);
        return dimensions;
    }

    private static Pair<Orientation, Dimension> getDimensionsWithMetadataReader(ByteSource byteSource) {
        Metadata metadata;
        try (InputStream bis = byteSource.openBufferedStream()) {
            metadata = ImageMetadataReader.readMetadata(bis);
        } catch (ImageProcessingException | IOException e) {
            logger.error("Error reading image metadata", e);
            return null;
        }
        Collection<ExifIFD0Directory> exifIFD0 = metadata.getDirectoriesOfType(ExifIFD0Directory.class);
        JpegDirectory jpegDirectory = metadata.getFirstDirectoryOfType(JpegDirectory.class);

        if (jpegDirectory == null || exifIFD0.isEmpty()) {
            return null;
        }

        try {
            int width = jpegDirectory.getImageWidth();
            int height = jpegDirectory.getImageHeight();

            for (var exif : exifIFD0) {

                if (exif.containsTag(ExifIFD0Directory.TAG_ORIENTATION)) {
                    var orientation = Orientation.fromExifOrientation(exif.getInt(ExifIFD0Directory.TAG_ORIENTATION));
                    logger.debug("orientation= {}, width={}, height={}", orientation, width, height);
                    return Pair.of(
                            orientation,
                            orientation.isFlipDimensions() ? new Dimension(height, width) : new Dimension(width, height)
                    );
                }
            }
        } catch (MetadataException e) {
            logger.debug("Error reading image dimensions or orientation", e);
        }

        return null;
    }

    private static Pair<Orientation, Dimension> getDimensionsWithCommonsImaging(ByteSource imageBytes, String filename) {
        try (var stream = imageBytes.openStream()) {
            var metadata = Imaging.getMetadata(stream, filename);

            if (metadata instanceof JpegImageMetadata) {
                var jpegMetadata = (JpegImageMetadata) metadata;
                var orientation = jpegMetadata.findExifValueWithExactMatch(TiffTagConstants.TIFF_TAG_ORIENTATION);
                var widthTag = jpegMetadata.findExifValueWithExactMatch(TiffTagConstants.TIFF_TAG_IMAGE_WIDTH);
                var heightTag = jpegMetadata.findExifValueWithExactMatch(TiffTagConstants.TIFF_TAG_IMAGE_LENGTH);
                int height;
                int width;
                if (widthTag == null || heightTag == null) {
                    stream.reset();
                    var size = Imaging.getImageSize(stream, filename);
                    width = size.width;
                    height = size.height;
                } else {
                    width = widthTag.getIntValue();
                    height = heightTag.getIntValue();
                }
                Orientation orientationEnum;
                if (orientation == null) {
                    orientationEnum = Orientation.Normal;
                } else {
                    orientationEnum = Orientation.fromExifOrientation(orientation.getIntValue());
                }

                return Pair.of(
                    orientationEnum,
                    orientationEnum.isFlipDimensions() ? new Dimension(height, width) : new Dimension(width, height)
                );
            } else {
                stream.reset();
                var size = Imaging.getImageSize(stream, filename);
                return Pair.of(Orientation.Normal, new Dimension(size.width, size.height));
            }

        } catch (IOException e) {
            return null;
        }
    }

    private static Pair<Orientation, Dimension> getDimensionsWithImageIOMetadata(ByteSource imageBytes, ImageReaderSelectionStrategy selectionStrategy) {
        // theoretically this should work for all images, but it doesn't
        // work with the test images for some reason
        try (ImageInputStream iis = ImageIO.createImageInputStream(imageBytes.openStream())) {
            Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);

            selectionStrategy = selectionStrategy != null ? selectionStrategy : DefaultImageReaderSelectionStrategy.INSTANCE;
            var reader = selectionStrategy.selectImageReader(iter);

            reader.setInput(iis);
            var metadata = reader.getImageMetadata(0);

            var orientation = findImageOrientation(metadata);

            int height = reader.getHeight(0);
            int width = reader.getWidth(0);

            logger.trace("Image dimensions: width={}, height={}, orientation={}", width, height, orientation);

            reader.dispose();

            return Pair.of(
                    orientation,
                    orientation.isFlipDimensions() ? new Dimension(height, width) : new Dimension(width, height)
            );
        } catch (IOException e) {
            logger.error("Error reading image dimensions", e);
        }
        return null;
    }

    /**
     * Finds the {@code ImageOrientation} tag, if any, and returns an {@link Orientation} based on its
     * {@code value} attribute.
     * If no match is found or the tag is not present, {@code Normal} (the default orientation) is returned.
     *
     * @param metadata an {@code IIOMetadata} object
     * @return the {@code Orientation} matching the {@code value} attribute of the {@code ImageOrientation} tag,
     * or {@code Normal}, never {@code null}.
     * @see Orientation
     * @see <a href="https://docs.oracle.com/javase/7/docs/api/javax/imageio/metadata/doc-files/standard_metadata.html">Standard (Plug-in Neutral) Metadata Format Specification</a>
     */
    private static Orientation findImageOrientation(final IIOMetadata metadata) {
        if (metadata != null) {
            IIOMetadataNode root = (IIOMetadataNode) metadata.getAsTree(IIOMetadataFormatImpl.standardMetadataFormatName);
            NodeList imageOrientations = root.getElementsByTagName("ImageOrientation");

            if (imageOrientations != null && imageOrientations.getLength() > 0) {
                IIOMetadataNode imageOrientation = (IIOMetadataNode) imageOrientations.item(0);
                String orientationValue = imageOrientation.getAttribute("value");
                logger.trace("Found ImageOrientation tag with value: {}", orientationValue);
                return Orientation.fromMetadataOrientation(orientationValue);
            } else {
                logger.trace("No ImageOrientation tag found in metadata.");
            }
        } else {
            logger.trace("Metadata is null.");
        }

        return Orientation.Normal;
    }

    public static class Dimension {
        public final int width;
        public final int height;

        public Dimension(int width, int height) {
            this.width = width;
            this.height = height;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("width", width)
                    .add("height", height)
                    .toString();
        }
    }

    static final double RADIANS_90_DEGREES = Math.toRadians(90.0);
    static final double RADIANS_180_DEGREES = Math.PI;
    static final double RADIANS_270_DEGREES = Math.toRadians(-90.0);

    public enum Orientation {
        Normal(1, false, List.of()) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                return Optional.empty();
            }
        },
        FlipH(2, false, List.of(Scalr.Rotation.FLIP_HORZ)) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                var affineTransform = new AffineTransform();
                affineTransform.scale(-1.0, 1.0);
                affineTransform.translate(-width, 0);
                return Optional.of(affineTransform);
            }
        },
        Rotate180(3, false, List.of(Scalr.Rotation.CW_180)) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                var affineTransform = new AffineTransform();
                affineTransform.translate(width, height);
                affineTransform.rotate(RADIANS_180_DEGREES);
                return Optional.of(affineTransform);
            }
        },
        FlipV(4, false, List.of(Scalr.Rotation.FLIP_VERT)) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                var affineTransform = new AffineTransform();
                affineTransform.scale(1.0, -1.0);
                affineTransform.translate(0, -height);
                return Optional.of(affineTransform);
            }
        },
        FlipVRotate90(5, true, List.of(Scalr.Rotation.FLIP_VERT, Scalr.Rotation.CW_90)) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                var affineTransform = new AffineTransform();
                affineTransform.rotate(RADIANS_270_DEGREES);
                affineTransform.scale(-1.0, 1.0);
                return Optional.of(affineTransform);
            }
        },
        Rotate270(6, true, List.of(Scalr.Rotation.CW_90)) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                var affineTransform = new AffineTransform();
                affineTransform.translate(height, 0);
                affineTransform.rotate(RADIANS_90_DEGREES);
                return Optional.of(affineTransform);
            }
        },
        FlipHRotate90(7, true, List.of(Scalr.Rotation.FLIP_HORZ, Scalr.Rotation.CW_90)) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                var affineTransform = new AffineTransform();
                affineTransform.scale(-1.0, 1.0);
//                affineTransform.translate(-height, 0);
//                affineTransform.translate(0, width);
                affineTransform.translate(-height, width);
                affineTransform.rotate(RADIANS_270_DEGREES);
                return Optional.of(affineTransform);
            }
        },
        Rotate90(8, true, List.of(Scalr.Rotation.CW_270)) {
            @Override
            public Optional<AffineTransform> getAffineTransform(int width, int height) {
                var affineTransform = new AffineTransform();
                affineTransform.translate(0, width);
                affineTransform.rotate(RADIANS_270_DEGREES);
                return Optional.of(affineTransform);
            }
        };

        // name as defined in javax.imageio metadata
        private final int value; // value as defined in TIFF spec
        private final boolean flipDimensions; // cheat for whether the dimensions are swapped
        private final List<Scalr.Rotation> scalrRotations;

        Orientation(int value, boolean flipDimensions, List<Scalr.Rotation> scalrRotations) {
            this.value = value;
            this.flipDimensions = flipDimensions;
            this.scalrRotations = scalrRotations;
        }

        public abstract Optional<AffineTransform> getAffineTransform(int width, int height);

        public int value() {
            return value;
        }

        public boolean isFlipDimensions() {
            return flipDimensions;
        }

        public List<Scalr.Rotation> getScalrRotations() {
            return scalrRotations;
        }

        public static Orientation fromMetadataOrientation(final String orientationName) {
            if (orientationName != null) {
                try {
                    return valueOf(orientationName);
                }
                catch (IllegalArgumentException e) {
                    String lowerCaseName = orientationName.toLowerCase();

                    for (Orientation orientation : values()) {
                        if (orientation.name().toLowerCase().equals(lowerCaseName)) {
                            return orientation;
                        }
                    }
                }
            }

            return Normal;
        }

        public static Orientation fromExifOrientation(final int orientation) {
            for (Orientation o : values()) {
                if (o.value == orientation) {
                    return o;
                }
            }
            return Normal;
        }
    }
}
