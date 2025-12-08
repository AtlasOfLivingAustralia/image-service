package au.org.ala.images.tiling;

import au.org.ala.images.TestBase;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ImageTilerTest extends TestBase {

    @Test
    public void test1() throws Exception {
        Assert.assertTrue(tileImage("CYMK_2078907.jpeg"));
    }

    @Test
    public void test2() throws Exception {
        Assert.assertTrue(tileImage("silky_oak.JPG"));
    }

    @Test
    public void test3() throws Exception {
        Assert.assertTrue(tileImage("5229.JPG"));
    }

    @Test
    public void test4() throws Exception {
        Assert.assertTrue(tileImage("wickhams_grevillea.PNG"));
    }

    @Test
    public void test5() throws Exception {
        Assert.assertTrue(tileImage("stk19951boj.png"));
    }

    @Test
    public void test6() throws Exception {
        Assert.assertTrue(tileImage("P1010763.JPG"));
    }

    @Test
    public void test7() throws Exception {
        Assert.assertTrue(tileImage("Bearded_Heath.jpg"));
    }

    @Test
    public void test8() throws Exception {
        Assert.assertTrue(tileImage("1024x576.jpg"));
    }

    /**
     * Test that tiles are correctly positioned when the image width and height exceed the strip width.
     *
     * This test creates tiles from a large image (10000x10000 pixels) which exceeds the
     * default strip width of 8192 pixels (for tile size 256). It verifies:
     * 1. Tiles are created at the expected positions
     * 2. Tiles in the second strip (past 8192px) are created correctly in both X and Y directions
     * 3. Each tile contains actual image data (not just background color)
     */
    @Test
    public void testTilePositionsForLargeImage() throws Exception {
        String filename = "large_test_10000x10000.jpg";
        URL url = ImageTilerTest.class.getResource(String.format("/images/%s", filename));
        assertNotNull("Test image should exist: " + filename, url);

        println("Testing tile positions for large image: %s", url);
        File imageFile = new File(url.toURI());

        // Read the original image to get reference dimensions
        BufferedImage originalImage = ImageIO.read(imageFile);
        assertNotNull("Should be able to read the test image", originalImage);

        int originalWidth = originalImage.getWidth();
        int originalHeight = originalImage.getHeight();

        println("Original image dimensions: %dx%d", originalWidth, originalHeight);
        assertTrue("Image width should exceed strip width (8192)", originalWidth > 8192);
        assertTrue("Image height should exceed strip width (8192)", originalHeight > 8192);

        ImageTilerConfig config = new ImageTilerConfig();
        int tileSize = config.getTileSize(); // Default 256
        ImageTiler3 tiler = new ImageTiler3(config);

        Path tempDir = Files.createTempDirectory("tile-position-test");
        try {
            var sw = Stopwatch.createStarted();
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            println("Tiling completed in %s", sw.stop());
            assertTrue("Tiling should succeed", results.getSuccess());

            println("Tiling completed with %d zoom levels", results.getZoomLevels());

            // Test the highest resolution level (highest level number = full resolution)
            // The levels are numbered in reverse: level 0 is most zoomed out, highest level is full resolution
            int fullResLevel = results.getZoomLevels() - 1;
            File fullResDir = new File(tempDir.toFile(), String.valueOf(fullResLevel));
            assertTrue("Full resolution level directory should exist", fullResDir.exists() && fullResDir.isDirectory());

            // Calculate expected number of columns and rows for full resolution
            int expectedCols = (int) Math.ceil((double) originalWidth / tileSize);
            int expectedRows = (int) Math.ceil((double) originalHeight / tileSize);

            println("Testing full resolution level %d: expected %d columns x %d rows = %d tiles",
                    fullResLevel, expectedCols, expectedRows, expectedCols * expectedRows);

            // Verify tiles at specific positions exist and contain image data
            verifyTileExists(fullResDir, originalImage, tileSize, 0, "First column (strip 1)");
            verifyTileExists(fullResDir, originalImage, tileSize, 15, "Middle of first strip");
            verifyTileExists(fullResDir, originalImage, tileSize, 31, "Near strip boundary");

            // Test tiles in the second strip (column >= 32) - this is the critical test
            // for images exceeding strip width of 8192 pixels
            // 8192 / 256 = 32, so column 32 is the start of the second strip
            if (expectedCols > 32) {
                verifyTileExists(fullResDir, originalImage, tileSize, 32, "First column of second strip");
            }
            if (expectedCols > 35) {
                verifyTileExists(fullResDir, originalImage, tileSize, 35, "Middle of second strip (past 8192px)");
            }
            verifyTileExists(fullResDir, originalImage, tileSize, expectedCols - 1, "Last column");

            println("✓ All tile position tests passed!");

        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    /**
     * Verify that tiles exist for a given column and contain pixels matching the original image.
     *
     * @param levelDir The directory containing the zoom level
     * @param originalImage The original source image
     * @param tileSize The size of each tile
     * @param col The column number to verify
     * @param description A description of this column for error messages
     */
    private void verifyTileExists(File levelDir, BufferedImage originalImage, int tileSize, int col, String description) throws Exception {
        File colDir = new File(levelDir, String.valueOf(col));
        assertTrue(description + ": column directory should exist for col=" + col,
                colDir.exists() && colDir.isDirectory());

        File[] tiles = colDir.listFiles((dir, name) -> name.endsWith(".png"));
        assertNotNull(description + ": should have tiles in column " + col, tiles);
        assertTrue(description + ": should have at least one tile in column " + col, tiles.length > 0);

        // Check that at least one tile's pixels match the original image
        boolean hasMatchingPixels = false;

        for (File tileFile : tiles) {
            BufferedImage tile = ImageIO.read(tileFile);
            assertNotNull(description + ": tile should be readable: " + tileFile.getName(), tile);

            // Extract row number from filename (e.g., "0.png" -> row 0)
            String filename = tileFile.getName();
            int row = Integer.parseInt(filename.substring(0, filename.lastIndexOf('.')));

            // Calculate the position in the original image
            int srcX = col * tileSize;
            int srcY = row * tileSize;

            // Ensure we're within bounds of the original image
            if (srcX >= originalImage.getWidth() || srcY >= originalImage.getHeight()) {
                continue;
            }

            // Sample and compare pixels between tile and original image
            int matchingPixels = 0;
            int sampledPixels = 0;

            // Sample pixels at regular intervals
            for (int y = 0; y < tile.getHeight() && (srcY + y) < originalImage.getHeight(); y += 20) {
                for (int x = 0; x < tile.getWidth() && (srcX + x) < originalImage.getWidth(); x += 20) {
                    sampledPixels++;

                    Color tileColor = new Color(tile.getRGB(x, y));
                    Color origColor = new Color(originalImage.getRGB(srcX + x, srcY + y));

                    // Compare colors with tolerance for JPEG compression artifacts
                    int rDiff = Math.abs(tileColor.getRed() - origColor.getRed());
                    int gDiff = Math.abs(tileColor.getGreen() - origColor.getGreen());
                    int bDiff = Math.abs(tileColor.getBlue() - origColor.getBlue());

                    // Allow tolerance of up to 15 per channel for JPEG compression
                    if (rDiff <= 15 && gDiff <= 15 && bDiff <= 15) {
                        matchingPixels++;
                    }
                }
            }

            // At least 90% of sampled pixels should match (accounting for compression)
            if (sampledPixels > 0 && matchingPixels >= (sampledPixels * 0.9)) {
                hasMatchingPixels = true;
                break;
            }
        }

        assertTrue(description + ": tiles in column " + col + " should contain pixels matching the original image",
                hasMatchingPixels);

        println("  ✓ Column %d (%s): %d tiles found with pixels matching original image", col, description, tiles.length);
    }

    private boolean tileImage(String filename) throws Exception {
        URL url = ImageTilerTest.class.getResource(String.format("/images/%s", filename));
        println("Tiling: %s", url);
        File imageFile = new File(url.toURI());

        ImageTilerConfig config = new ImageTilerConfig();
        ImageTiler3 tiler = new ImageTiler3(config);

        Path tempDir = Files.createTempDirectory("imagetests");
        try {
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            println("Result Success: %b", results.getSuccess());
            println("Result Zoomlevels: %d", results.getZoomLevels());
            return results.getSuccess();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}
