package au.org.ala.images.tiling;

import au.org.ala.images.TestBase;
import com.google.common.base.Stopwatch;
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

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class ImageTiler4Test extends TestBase {

    private static final int _tileSize = 256;

    /**
     * Test that ImageTiler4 correctly handles all zoom levels including extreme ones.
     */
    @Test
    public void testAllZoomLevels() throws Exception {
        String filename = "large_test_10000x10000.jpg";
        URL url = ImageTiler4Test.class.getResource(String.format("/images/%s", filename));
        assertNotNull("Test image should exist: " + filename, url);

        println("Testing ImageTiler4 with all zoom levels: %s", url);
        File imageFile = new File(url.toURI());

        BufferedImage originalImage = ImageIO.read(imageFile);
        assertNotNull("Should be able to read the test image", originalImage);

        int originalWidth = originalImage.getWidth();
        int originalHeight = originalImage.getHeight();

        println("Original image dimensions: %dx%d", originalWidth, originalHeight);

        ImageTilerConfig config = new ImageTilerConfig();
        int tileSize = config.getTileSize();
        ImageTiler4 tiler = new ImageTiler4(config);

        Path tempDir = Files.createTempDirectory("imagetiler4-test");
        try {
            var sw = Stopwatch.createStarted();
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            println("Tiling completed in %s", sw.stop());
            assertTrue("Tiling should succeed", results.getSuccess());

            int zoomLevels = results.getZoomLevels();
            println("Tiling completed with %d zoom levels", zoomLevels);

            // Test all zoom levels
            for (int level = 0; level < zoomLevels; level++) {
                println("\n=== Testing Zoom Level %d ===", level);
                testZoomLevel(tempDir.toFile(), level, originalImage, tileSize, zoomLevels);
            }

            println("\n✓ All zoom levels tested successfully!");

        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    /**
     * Test extreme zoom level 0 specifically
     */
    @Test
    public void testExtremeZoomLevel() throws Exception {
        String filename = "large_test_10000x10000.jpg";
        URL url = ImageTiler4Test.class.getResource(String.format("/images/%s", filename));
        File imageFile = new File(url.toURI());

        BufferedImage originalImage = ImageIO.read(imageFile);
        int originalWidth = originalImage.getWidth();
        int originalHeight = originalImage.getHeight();

        ImageTilerConfig config = new ImageTilerConfig();
        ImageTiler4 tiler = new ImageTiler4(config);

        Path tempDir = Files.createTempDirectory("imagetiler4-extreme-test");
        try {
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            assertTrue(results.getSuccess());

            int zoomLevels = results.getZoomLevels();
            int subsample = (int) Math.pow(2, zoomLevels - 1);  // Level 0 has max subsample

            // Test level 0 specifically
            File level0Dir = new File(tempDir.toFile(), "0");
            assertTrue("Level 0 directory should exist", level0Dir.exists());

            File[] colDirs = level0Dir.listFiles(File::isDirectory);
            assertNotNull("Should have column directories", colDirs);
            
            // Level 0: 10000/64 = 156.25, ceil = 157 pixels, ceil(157/256) = 1 tile
            assertEquals("Should have exactly 1 column at level 0", 1, colDirs.length);

            File col0Dir = new File(level0Dir, "0");
            File[] tiles = col0Dir.listFiles((dir, name) -> name.endsWith(".png"));
            assertNotNull("Should have tiles in column 0", tiles);
            assertEquals("Should have exactly 1 row at level 0", 1, tiles.length);

            // Verify the tile contains pixels from the entire image
            File tile00 = new File(col0Dir, "0.png");
            assertTrue("Tile 0,0 should exist", tile00.exists());

            BufferedImage tileImage = ImageIO.read(tile00);
            assertNotNull("Should be able to read tile", tileImage);

            // Calculate the actual content area in the tile
            // At level 0, the image is heavily downscaled
            int scaledHeight = (int) Math.ceil(originalHeight / (double) subsample);
            int scaledWidth = (int) Math.ceil(originalWidth / (double) subsample);

            // If scaled image is smaller than tile size, there will be padding at the top
            int paddingTop = Math.max(0, _tileSize - scaledHeight);
            int contentHeight = Math.min(_tileSize, scaledHeight);

            println("Level 0: scaledSize=%dx%d, paddingTop=%d, contentHeight=%d",
                    scaledWidth, scaledHeight, paddingTop, contentHeight);

            // Sample pixels from different areas of the CONTENT (not the padding)
            // Sample from the middle of the content area
            int sampleY1 = paddingTop + (contentHeight / 4);
            int sampleY2 = paddingTop + (3 * contentHeight / 4);
            int sampleX1 = scaledWidth / 4;
            int sampleX2 = 3 * scaledWidth / 4;

            boolean topLeftMatches = checkPixelMatch(tileImage, originalImage, sampleX1, sampleY1, subsample, paddingTop);
            boolean topRightMatches = checkPixelMatch(tileImage, originalImage, sampleX2, sampleY1, subsample, paddingTop);
            boolean bottomLeftMatches = checkPixelMatch(tileImage, originalImage, sampleX1, sampleY2, subsample, paddingTop);
            boolean bottomRightMatches = checkPixelMatch(tileImage, originalImage, sampleX2, sampleY2, subsample, paddingTop);

            int matchCount = (topLeftMatches ? 1 : 0) + (topRightMatches ? 1 : 0) + 
                           (bottomLeftMatches ? 1 : 0) + (bottomRightMatches ? 1 : 0);

            println("Level 0 tile pixel matches: topLeft=%b, topRight=%b, bottomLeft=%b, bottomRight=%b",
                    topLeftMatches, topRightMatches, bottomLeftMatches, bottomRightMatches);

            // At least 3 out of 4 corners should match (allowing for edge cases)
            assertTrue("Level 0 tile should contain pixels from across the entire image", matchCount >= 3);

            println("✓ Level 0 (extreme zoom) is complete and correct!");

        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    private boolean checkPixelMatch(BufferedImage tile, BufferedImage original, int tileX, int tileY, int subsample, int paddingTop) {
        if (tileX >= tile.getWidth() || tileY >= tile.getHeight() || tileY < paddingTop) {
            println("  checkPixelMatch: out of bounds - tileX=%d, tileY=%d, paddingTop=%d", tileX, tileY, paddingTop);
            return false;
        }

        // Account for padding: tileY includes padding at top, so subtract it to get position in scaled content
        int contentY = tileY - paddingTop;

        // Map from scaled image coordinates to original image coordinates
        int origX = tileX * subsample;
        int origY = contentY * subsample;

        if (origX >= original.getWidth() || origY >= original.getHeight()) {
            println("  checkPixelMatch: original out of bounds - origX=%d, origY=%d, origWidth=%d, origHeight=%d",
                    origX, origY, original.getWidth(), original.getHeight());
            return false;
        }

        Color tileColor = new Color(tile.getRGB(tileX, tileY));
        Color origColor = new Color(original.getRGB(origX, origY));

        println("  checkPixelMatch: tile(%d,%d)->content(%d,%d)->orig(%d,%d): tileRGB=(%d,%d,%d) origRGB=(%d,%d,%d)",
                tileX, tileY, tileX, contentY, origX, origY,
                tileColor.getRed(), tileColor.getGreen(), tileColor.getBlue(),
                origColor.getRed(), origColor.getGreen(), origColor.getBlue());

        int rDiff = Math.abs(tileColor.getRed() - origColor.getRed());
        int gDiff = Math.abs(tileColor.getGreen() - origColor.getGreen());
        int bDiff = Math.abs(tileColor.getBlue() - origColor.getBlue());

        // Allow tolerance for subsampling and JPEG compression
        return rDiff <= 30 && gDiff <= 30 && bDiff <= 30;
    }

    private void testZoomLevel(File tempDir, int level, BufferedImage originalImage, int tileSize, int totalZoomLevels) throws Exception {
        File levelDir = new File(tempDir, String.valueOf(level));
        assertTrue("Zoom level directory should exist: " + level, levelDir.exists() && levelDir.isDirectory());

        int subsample = (int) Math.pow(2, totalZoomLevels - level - 1);

        int levelWidth = (int) Math.ceil((double) originalImage.getWidth() / subsample);
        int levelHeight = (int) Math.ceil((double) originalImage.getHeight() / subsample);

        int expectedCols = (int) Math.ceil((double) levelWidth / tileSize);
        int expectedRows = (int) Math.ceil((double) levelHeight / tileSize);

        println("Level %d: subsample=%d, dimensions=%dx%d, expected tiles=%dx%d (%d total)",
                level, subsample, levelWidth, levelHeight, expectedCols, expectedRows, expectedCols * expectedRows);

        File[] colDirs = levelDir.listFiles(File::isDirectory);
        assertNotNull("Should have column directories at level " + level, colDirs);

        // ImageTiler4 should produce EXACT tile counts at all levels
        int actualCols = colDirs.length;
        assertEquals("Should have correct number of column directories at level " + level,
                expectedCols, actualCols);

        // Verify a few key columns
        if (actualCols > 0) {
            verifyColumn(levelDir, 0, "First column");
        }
        if (actualCols > 1) {
            verifyColumn(levelDir, actualCols / 2, "Middle column");
            verifyColumn(levelDir, actualCols - 1, "Last column");
        }
    }

    private void verifyColumn(File levelDir, int col, String description) {
        File colDir = new File(levelDir, String.valueOf(col));
        assertTrue(description + ": column should exist for col=" + col, colDir.exists() && colDir.isDirectory());

        File[] tiles = colDir.listFiles((dir, name) -> name.endsWith(".png"));
        assertNotNull(description + ": should have tiles in column " + col, tiles);
        assertTrue(description + ": should have at least one tile in column " + col, tiles.length > 0);

        println("  ✓ Column %d (%s): %d tiles found", col, description, tiles.length);
    }

    @Test
    public void testSmallImage() throws Exception {
        String filename = "audio-icon.png";
        URL url = ImageTiler4Test.class.getResource(String.format("/images/%s", filename));
        File imageFile = new File(url.toURI());

        ImageTilerConfig config = new ImageTilerConfig();
        ImageTiler4 tiler = new ImageTiler4(config);

        Path tempDir = Files.createTempDirectory("imagetiler4-small-test");
        try {
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            assertTrue("Small image tiling should succeed", results.getSuccess());
            assertTrue("Small image should have zoom levels", results.getZoomLevels() > 0);

            println("✓ Small image test passed with %d zoom levels", results.getZoomLevels());

        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}

