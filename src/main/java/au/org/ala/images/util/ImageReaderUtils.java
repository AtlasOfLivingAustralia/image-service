package au.org.ala.images.util;

import au.org.ala.images.metadata.ImageMetadataExtractor;
import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.drew.metadata.jpeg.JpegDirectory;
import org.imgscalr.Scalr;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

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
                    result.setInput(rotate(imageBytes));
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

    public static ImageInputStream  rotate(byte[] imageBytes) throws IOException {

        try {
            BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(imageBytes));
            Metadata metadata = ImageMetadataReader.readMetadata(bis, imageBytes.length);
            Collection<ExifIFD0Directory> exifIFD0 = metadata.getDirectoriesOfType(ExifIFD0Directory.class);
            JpegDirectory jpegDirectory = metadata.getFirstDirectoryOfType(JpegDirectory.class);

            if(jpegDirectory == null || exifIFD0.isEmpty()){
                FastByteArrayInputStream fbis = new FastByteArrayInputStream(imageBytes);
                return ImageIO.createImageInputStream(fbis);
            }

            int orientation = exifIFD0.iterator().next().getInt(ExifIFD0Directory.TAG_ORIENTATION);

            int width = jpegDirectory.getImageWidth();
            int height = jpegDirectory.getImageHeight();

            AffineTransform affineTransform = new AffineTransform();

            switch (orientation) {
                case 1:
                    break;
                case 2: // Flip X
                    affineTransform.scale(-1.0, 1.0);
                    affineTransform.translate(-width, 0);
                    break;
                case 3: // PI rotation
                    affineTransform.translate(width, height);
                    affineTransform.rotate(Math.PI);
                    break;
                case 4: // Flip Y
                    affineTransform.scale(1.0, -1.0);
                    affineTransform.translate(0, -height);
                    break;
                case 5: // - PI/2 and Flip X
                    affineTransform.rotate(-Math.PI / 2);
                    affineTransform.scale(-1.0, 1.0);
                    break;
                case 6: // -PI/2 and -width
                    affineTransform.translate(height, 0);
                    affineTransform.rotate(Math.PI / 2);
                    break;
                case 7: // PI/2 and Flip
                    affineTransform.scale(-1.0, 1.0);
                    affineTransform.translate(-height, 0);
                    affineTransform.translate(0, width);
                    affineTransform.rotate(3 * Math.PI / 2);
                    break;
                case 8: // PI / 2
                    affineTransform.translate(0, width);
                    affineTransform.rotate(3 * Math.PI / 2);
                    break;
                default:
                    break;
            }

            BufferedInputStream bis2 = new BufferedInputStream(new ByteArrayInputStream(imageBytes));
            BufferedImage originalImage = ImageIO.read(bis2);


            AffineTransformOp affineTransformOp = new AffineTransformOp(affineTransform, AffineTransformOp.TYPE_BILINEAR);
            BufferedImage destinationImage = new BufferedImage(originalImage.getHeight(), originalImage.getWidth(), originalImage.getType());
            destinationImage = affineTransformOp.filter(originalImage, destinationImage);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            ImageIO.write(destinationImage, "jpg", baos);
            baos.flush();
            byte[] imageInByte = baos.toByteArray();
            baos.close();

            FastByteArrayInputStream fbis = new FastByteArrayInputStream(imageInByte);
            return ImageIO.createImageInputStream(fbis);
        } catch (Exception e){
            FastByteArrayInputStream fbis = new FastByteArrayInputStream(imageBytes);
            return ImageIO.createImageInputStream(fbis);
        }
    }
}
