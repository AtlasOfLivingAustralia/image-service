package au.org.ala.images.thumb;

import au.org.ala.images.TestBase;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ImageThumbnailerTest extends TestBase{

    @Test
    public void test1() throws Exception {
        createThumbs("CYMK_2078907.jpeg");
    }

    @Test
    public void test2() throws Exception {
        createThumbs("audio-icon.png");
    }

    /**
     * Test that orientation metadata is correctly applied for all portrait orientation test images.
     *
     * The orientation test images are designed so that after applying the orientation
     * transformation, they display in the correct orientation. All portrait images should
     * remain portrait (450x600) after applying their respective EXIF orientation values (1-8).
     */
    @Test
    public void testOrientationPortraitImages() throws Exception {
        // Expected dimensions for all portrait images after orientation correction
        final int PORTRAIT_WIDTH = 450;
        final int PORTRAIT_HEIGHT = 600;
        final int MAX_DIMENSION = 300;
        // Scaled dimensions: max dimension 600 -> 300 = 0.5x scale
        final int EXPECTED_THUMB_WIDTH = 225;
        final int EXPECTED_THUMB_HEIGHT = 300;

        for (int orientation = 1; orientation <= 8; orientation++) {
            String imageName = String.format("portrait_%d.jpg", orientation);
            String imagePath = "images/orientation/" + imageName;

            println("Testing %s (EXIF Orientation %d)", imageName, orientation);

            // Verify that ImageReaderUtils correctly reports the dimensions with orientation
            au.org.ala.images.util.ImageReaderUtils.Dimension dims =
                au.org.ala.images.util.ImageReaderUtils.getImageDimensions(
                    Resources.asByteSource(Resources.getResource(imagePath)), imageName);
            assertNotNull("Should have dimensions for " + imageName, dims);
            assertEquals(imageName + " should report portrait width", PORTRAIT_WIDTH, dims.width);
            assertEquals(imageName + " should report portrait height", PORTRAIT_HEIGHT, dims.height);

            Path tempDir = Files.createTempDirectory("orientation-test-" + orientation);
            try {
                // Create a simple thumbnail
                List<ThumbDefinition> thumbList = new ArrayList<>();
                thumbList.add(new ThumbDefinition(MAX_DIMENSION, false, null, imageName));

                // Generate thumbnail using generateThumbnailsNoIntermediateEncode which applies orientation
                ImageThumbnailer thumbnailer = new ImageThumbnailer();
                List<ThumbnailingResult> results = thumbnailer.generateThumbnailsNoIntermediateEncode(
                    Resources.asByteSource(Resources.getResource(imagePath)),
                    new au.org.ala.images.util.FileByteSinkFactory(tempDir.toFile()),
                    thumbList
                );

                // Verify we got a result
                assertNotNull(imageName + " should have thumbnailing results", results);
                assertEquals(imageName + " should have 1 thumbnail", 1, results.size());

                ThumbnailingResult result = results.get(0);

                // Verify the thumbnail is portrait (taller than wide)
                assertTrue(imageName + " thumbnail should be taller than wide (portrait) after orientation correction",
                    result.getHeight() > result.getWidth());

                // Verify the actual thumbnail file
                File thumbnailFile = new File(tempDir.toFile(), imageName);
                assertTrue(imageName + " thumbnail file should exist", thumbnailFile.exists());

                BufferedImage thumbnail = ImageIO.read(thumbnailFile);
                assertNotNull(imageName + " should be readable", thumbnail);

                // Verify the thumbnail is portrait (taller than wide)
                assertTrue(imageName + " thumbnail height should be greater than width",
                    thumbnail.getHeight() > thumbnail.getWidth());

                // Verify exact dimensions
                assertEquals(imageName + " thumbnail width should be " + EXPECTED_THUMB_WIDTH,
                    EXPECTED_THUMB_WIDTH, thumbnail.getWidth());
                assertEquals(imageName + " thumbnail height should be " + EXPECTED_THUMB_HEIGHT,
                    EXPECTED_THUMB_HEIGHT, thumbnail.getHeight());

                println("  ✓ %s: %dx%d -> %dx%d", imageName, dims.width, dims.height,
                    thumbnail.getWidth(), thumbnail.getHeight());

            } finally {
                FileUtils.deleteDirectory(tempDir.toFile());
            }
        }
    }

    /**
     * Test that orientation metadata is correctly applied for all landscape orientation test images.
     *
     * All landscape images should remain landscape (600x450) after applying their respective
     * EXIF orientation values (1-8).
     */
    @Test
    public void testOrientationLandscapeImages() throws Exception {
        // Expected dimensions for all landscape images after orientation correction
        final int LANDSCAPE_WIDTH = 600;
        final int LANDSCAPE_HEIGHT = 450;
        final int MAX_DIMENSION = 300;
        // Scaled dimensions: max dimension 600 -> 300 = 0.5x scale
        final int EXPECTED_THUMB_WIDTH = 300;
        final int EXPECTED_THUMB_HEIGHT = 225;

        for (int orientation = 1; orientation <= 8; orientation++) {
            String imageName = String.format("landscape_%d.jpg", orientation);
            String imagePath = "images/orientation/" + imageName;

            println("Testing %s (EXIF Orientation %d)", imageName, orientation);

            // Verify that ImageReaderUtils correctly reports the dimensions with orientation
            au.org.ala.images.util.ImageReaderUtils.Dimension dims =
                au.org.ala.images.util.ImageReaderUtils.getImageDimensions(
                    Resources.asByteSource(Resources.getResource(imagePath)), imageName);
            assertNotNull("Should have dimensions for " + imageName, dims);
            assertEquals(imageName + " should report landscape width", LANDSCAPE_WIDTH, dims.width);
            assertEquals(imageName + " should report landscape height", LANDSCAPE_HEIGHT, dims.height);

            Path tempDir = Files.createTempDirectory("orientation-test-" + orientation);
            try {
                // Create a simple thumbnail
                List<ThumbDefinition> thumbList = new ArrayList<>();
                thumbList.add(new ThumbDefinition(MAX_DIMENSION, false, null, imageName));

                // Generate thumbnail using generateThumbnailsNoIntermediateEncode which applies orientation
                ImageThumbnailer thumbnailer = new ImageThumbnailer();
                List<ThumbnailingResult> results = thumbnailer.generateThumbnailsNoIntermediateEncode(
                    Resources.asByteSource(Resources.getResource(imagePath)),
                    new au.org.ala.images.util.FileByteSinkFactory(tempDir.toFile()),
                    thumbList
                );

                // Verify we got a result
                assertNotNull(imageName + " should have thumbnailing results", results);
                assertEquals(imageName + " should have 1 thumbnail", 1, results.size());

                ThumbnailingResult result = results.get(0);

                // Verify the thumbnail is landscape (wider than tall)
                assertTrue(imageName + " thumbnail should be wider than tall (landscape) after orientation correction",
                    result.getWidth() > result.getHeight());

                // Verify the actual thumbnail file
                File thumbnailFile = new File(tempDir.toFile(), imageName);
                assertTrue(imageName + " thumbnail file should exist", thumbnailFile.exists());

                BufferedImage thumbnail = ImageIO.read(thumbnailFile);
                assertNotNull(imageName + " should be readable", thumbnail);

                // Verify the thumbnail is landscape (wider than tall)
                assertTrue(imageName + " thumbnail width should be greater than height",
                    thumbnail.getWidth() > thumbnail.getHeight());

                // Verify exact dimensions
                assertEquals(imageName + " thumbnail width should be " + EXPECTED_THUMB_WIDTH,
                    EXPECTED_THUMB_WIDTH, thumbnail.getWidth());
                assertEquals(imageName + " thumbnail height should be " + EXPECTED_THUMB_HEIGHT,
                    EXPECTED_THUMB_HEIGHT, thumbnail.getHeight());

                println("  ✓ %s: %dx%d -> %dx%d", imageName, dims.width, dims.height,
                    thumbnail.getWidth(), thumbnail.getHeight());

            } finally {
                FileUtils.deleteDirectory(tempDir.toFile());
            }
        }
    }

    private void createThumbs(String filename) throws Exception {
        URL url = ImageThumbnailerTest.class.getResource(String.format("/images/%s", filename));
        println("Thumbnailing: %s", url);
        File imageFile = new File(url.toURI());

        Path tempDir = Files.createTempDirectory("imagetests");
        try {
            byte[] imageBytes = FileUtils.readFileToByteArray(imageFile);

            List<ThumbDefinition> thumbList = new ArrayList<ThumbDefinition>();

            thumbList.add(new ThumbDefinition(300, false, null, "thumbnail.jpg"));
            thumbList.add(new ThumbDefinition(300, true, null, "thumbnail_square.png"));
            thumbList.add(new ThumbDefinition(300, true, Color.black, "thumbnail_square_black.jpg"));
            thumbList.add(new ThumbDefinition(300, true, Color.white, "thumbnail_square_white.jpg"));
            thumbList.add(new ThumbDefinition(300, true, Color.darkGray, "thumbnail_square_darkGray.jpg"));
            thumbList.add(new ThumbDefinition(650, false, Color.white, "thumbnail_large.jpg"));
            thumbList.add(new ThumbDefinition(800, false, null, "thumbnail_xlarge.jpg"));
            thumbList.add(new ThumbDefinition(300, true, null, "thumbnail_centre_crop.jpg"));
            thumbList.add(new ThumbDefinition(650, false, Color.white, "thumbnail_centre_crop_large.jpg"));

            List<ThumbnailingResult> results = new ImageThumbnailer().generateThumbnails(imageBytes, tempDir.toFile(), thumbList);
            for (ThumbnailingResult r : results) {
                System.out.println(r.getThumbnailName());
            }

        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }

    }


}
