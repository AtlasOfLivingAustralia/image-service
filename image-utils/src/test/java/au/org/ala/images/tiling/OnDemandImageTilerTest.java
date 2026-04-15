package au.org.ala.images.tiling;

import au.org.ala.images.TestBase;
import au.org.ala.images.util.FileByteSinkFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

import javax.imageio.ImageIO;

/**
 * Performance tests for OnDemandImageTiler.
 */
public class OnDemandImageTilerTest extends TestBase {

    private ExecutorService ioExecutor;
    private ExecutorService levelExecutor;

    @Before
    public void setup() {
        ioExecutor = Executors.newFixedThreadPool(2);
        levelExecutor = Executors.newFixedThreadPool(2);
    }

    @After
    public void tearDown() {
        if (ioExecutor != null) ioExecutor.shutdown();
        if (levelExecutor != null) levelExecutor.shutdown();
    }

    @Test
    public void testSingleTileGeneration() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        // Get pyramid info
        try (FileInputStream fis = new FileInputStream(imageFile)) {
            TilePyramidInfo info = tiler.getPyramidInfo(fis);
            System.out.println("Pyramid info for 10000x10000 image:");
            System.out.println(info);
        }

        // Generate a single tile at maximum zoom (level 66)
        long startTime = System.nanoTime();
        byte[] tileBytes;
        File outputDir = Files.createTempDirectory("testSingleTile").toFile();
        TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
        try (FileInputStream fis = new FileInputStream(imageFile)) {
            TileGenerationResult result = tiler.generateTile(fis, sink, 6, 0, 0);
            assertTrue("Tile generation should be successful", result.isSuccess());
            File tileFile = new File(outputDir, "6/0/0.png");
            assertTrue("Tile file should exist", tileFile.exists());
            tileBytes = Files.readAllBytes(tileFile.toPath());
        }
        long endTime = System.nanoTime();

        assertNotNull("Tile should be generated", tileBytes);
        BufferedImage tile = ImageIO.read(new ByteArrayInputStream(tileBytes));
        assertEquals("Tile width should be 256", 256, tile.getWidth());
        assertEquals("Tile height should be 256", 256, tile.getHeight());

        double timeMs = (endTime - startTime) / 1_000_000.0;
        System.out.printf("✓ Generated single tile (6,0,0) in %.2f ms\n", timeMs);
    }

    @Test
    public void testMultipleTilesAtDifferentLevels() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        // Test tiles at different zoom levels
        int[][] testCases = {
            {0, 0, 0},  // Level 0 (most zoomed out) - tile (0,0)
            {3, 0, 0},  // Level 3 - tile (0,0)
            {5, 5, 5},  // Level 5 - tile (5,5)
            {6, 10, 10}, // Level 6 (most zoomed in) - tile (10,10)
            {6, 20, 20}, // Level 6 - tile (20,20)
        };

        System.out.println("\nTile generation performance for 10000x10000 image:");
        System.out.println("Level | X  | Y  | Time (ms) | Notes");
        System.out.println("------|----|----|-----------|------");

        for (int[] testCase : testCases) {
            int level = testCase[0];
            int x = testCase[1];
            int y = testCase[2];

            long startTime = System.nanoTime();
            byte[] tileBytes;
            File outputDir = Files.createTempDirectory("testMultipleTiles").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            try (FileInputStream fis = new FileInputStream(imageFile)) {
                TileGenerationResult result = tiler.generateTile(fis, sink, level, x, y);
                assertTrue("Tile generation should be successful", result.isSuccess());
                File tileFile = new File(outputDir, level + "/" + x + "/" + y + ".png");
                assertTrue("Tile file should exist", tileFile.exists());
                tileBytes = Files.readAllBytes(tileFile.toPath());
            }
            long endTime = System.nanoTime();

            assertNotNull("Tile should be generated", tileBytes);
            double timeMs = (endTime - startTime) / 1_000_000.0;

            String notes = "";
            if (level == 0) {
                notes = "Entire image subsampled";
            } else if (level == 6) {
                notes = "Full resolution region";
            }

            System.out.printf("  %2d  | %2d | %2d | %9.2f | %s\n", level, x, y, timeMs, notes);
        }
    }

    @Test
    public void testBurstTileGeneration() throws IOException {
        // Simulate a user panning/zooming - generating multiple adjacent tiles rapidly
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        // Generate a 3x3 grid of tiles at level 6 (typical viewport)
        int level = 6;
        int startX = 10;
        int startY = 10;
        int gridSize = 3;

        System.out.println("\nBurst generation (3x3 viewport at level 6):");

        long overallStart = System.nanoTime();
        List<Long> individualTimes = new ArrayList<>();

        for (int x = startX; x < startX + gridSize; x++) {
            for (int y = startY; y < startY + gridSize; y++) {
                long tileStart = System.nanoTime();
                File outputDir = Files.createTempDirectory("testBurstTile").toFile();
                TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
                try (FileInputStream fis = new FileInputStream(imageFile)) {
                    TileGenerationResult result = tiler.generateTile(fis, sink, level, x, y);
                    assertTrue("Tile generation should be successful", result.isSuccess());
                    File tileFile = new File(outputDir, level + "/" + x + "/" + y + ".png");
                    assertTrue("Tile file should exist", tileFile.exists());
                }
                long tileEnd = System.nanoTime();
                individualTimes.add(tileEnd - tileStart);
            }
        }
        long overallEnd = System.nanoTime();

        double totalTime = (overallEnd - overallStart) / 1_000_000.0;
        double avgTime = individualTimes.stream().mapToLong(t -> t).average().orElse(0) / 1_000_000.0;
        double minTime = individualTimes.stream().mapToLong(t -> t).min().orElse(0) / 1_000_000.0;
        double maxTime = individualTimes.stream().mapToLong(t -> t).max().orElse(0) / 1_000_000.0;

        System.out.printf("Total time: %.2f ms\n", totalTime);
        System.out.printf("Tiles generated: %d\n", individualTimes.size());
        System.out.printf("Average per tile: %.2f ms\n", avgTime);
        System.out.printf("Min time: %.2f ms\n", minTime);
        System.out.printf("Max time: %.2f ms\n", maxTime);
        System.out.printf("Throughput: %.2f tiles/second\n", (individualTimes.size() * 1000.0) / totalTime);
    }

    @Test
    public void testLargeImageOnDemand() throws IOException {
        File imageFile = getImageFile("large_test_20000x20000.jpg");
        if (!imageFile.exists()) {
            System.out.println("Skipping test - 20000x20000 image not available");
            return;
        }

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        // Get pyramid info
        try (FileInputStream fis = new FileInputStream(imageFile)) {
            TilePyramidInfo info = tiler.getPyramidInfo(fis);
            System.out.println("\nPyramid info for 20000x20000 image:");
            System.out.println(info);
        }

        System.out.println("\nTile generation performance for 20000x20000 image:");
        System.out.println("Level | X  | Y  | Time (ms)");
        System.out.println("------|----|----|----------");

        int[][] testCases = {
            {0, 0, 0},   // Most zoomed out
            {4, 5, 5},   // Mid level
            {8, 20, 20}, // Max zoom
        };

        for (int[] testCase : testCases) {
            int level = testCase[0];
            int x = testCase[1];
            int y = testCase[2];

            long startTime = System.nanoTime();
            File outputDir = Files.createTempDirectory("testLargeImage").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            TileGenerationResult result;
            try (FileInputStream fis = new FileInputStream(imageFile)) {
                result = tiler.generateTile(fis, sink, level, x, y);
            }
            long endTime = System.nanoTime();

            if (result.isSuccess()) {
                double timeMs = (endTime - startTime) / 1_000_000.0;
                System.out.printf("  %2d  | %2d | %2d | %9.2f\n", level, x, y, timeMs);
            }
        }
    }

    @Test
    public void testInvalidCoordinates() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        // Out of bounds coordinates should return error
        File outputDir = Files.createTempDirectory("testInvalidCoord").toFile();
        TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
        try (FileInputStream fis = new FileInputStream(imageFile)) {
            TileGenerationResult result = tiler.generateTile(fis, sink, 6, 1000, 1000);
            assertFalse("Result should not be successful", result.isSuccess());
            assertEquals(TileGenerationResult.Status.OUT_OF_BOUNDS, result.getStatus());
        }

        // Invalid level should return error
        try (FileInputStream fis = new FileInputStream(imageFile)) {
            TileGenerationResult result = tiler.generateTile(fis, sink, 7, 0, 0);
            assertFalse("Result should not be successful", result.isSuccess());
            assertEquals(TileGenerationResult.Status.INVALID_LEVEL, result.getStatus());
        }
    }

    @Test
    public void testComparisonWithBatchTiler() throws IOException {
        // Compare on-demand generation time vs batch generation time per tile
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        System.out.println("\n=== On-Demand vs Batch Tiler Comparison (10000x10000 image) ===\n");

        // Test on-demand: generate 10 random tiles
        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler onDemandTiler = new OnDemandImageTiler(config);

        int[][] sampleTiles = {
            {6, 0, 0}, {6, 5, 5}, {6, 10, 10}, {6, 15, 15}, {6, 20, 20},
            {5, 2, 2}, {5, 4, 4}, {3, 1, 1}, {1, 0, 0}, {0, 0, 0}
        };

        long onDemandTotal = 0;
        System.out.println("On-Demand Generation (10 tiles):");
        for (int[] coords : sampleTiles) {
            long start = System.nanoTime();
            File outputDir = Files.createTempDirectory("testComparison").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            try (FileInputStream fis = new FileInputStream(imageFile)) {
                TileGenerationResult result = onDemandTiler.generateTile(fis, sink, coords[0], coords[1], coords[2]);
                assertTrue(result.isSuccess());
            }
            long end = System.nanoTime();
            long timeMs = (end - start) / 1_000_000;
            onDemandTotal += timeMs;
            System.out.printf("  Tile (%d,%d,%d): %d ms\n", coords[0], coords[1], coords[2], timeMs);
        }

        System.out.printf("\nOn-Demand Total: %d ms for 10 tiles (avg: %.1f ms/tile)\n",
                onDemandTotal, onDemandTotal / 10.0);
        System.out.println("\nNote: Batch tilers (ImageTiler4/5) generate ALL tiles,");
        System.out.println("so they're faster per-tile but must generate the full pyramid.");
        System.out.println("On-demand is ideal for sparse access patterns or real-time serving.");
    }

    @Test
    public void testGenerateTileToSink() throws Exception {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        // Create a temporary directory for output
        File outputDir = Files.createTempDirectory("ondemand-tile-test").toFile();
        outputDir.deleteOnExit();

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        // Create a TilerSink
        TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));

        // Generate a single tile at level 6, position (10, 10)
        try (FileInputStream fis = new FileInputStream(imageFile)) {
            tiler.generateTile(fis, sink, 6, 10, 10);
        }

        // Verify the tile was written
        File tileFile = new File(outputDir, "6/10/10.png");
        assertTrue("Tile file should exist", tileFile.exists());
        assertTrue("Tile file should not be empty", tileFile.length() > 0);

        System.out.println("✓ Successfully generated tile to sink: " + tileFile.getAbsolutePath());
        System.out.println("  Tile size: " + tileFile.length() + " bytes");
    }

    @Test
    public void testGenerateMultipleTilesToSink() throws Exception {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        File outputDir = Files.createTempDirectory("ondemand-tiles-test").toFile();
        outputDir.deleteOnExit();

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);
        TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));

        // Generate a 3x3 grid of tiles
        int level = 6;
        int[][] tiles = {{10, 10}, {10, 11}, {10, 12}, {11, 10}, {11, 11}, {11, 12}, {12, 10}, {12, 11}, {12, 12}};

        long startTime = System.nanoTime();
        for (int[] tile : tiles) {
            try (FileInputStream fis = new FileInputStream(imageFile)) {
                tiler.generateTile(fis, sink, level, tile[0], tile[1]);
            }
        }
        long endTime = System.nanoTime();

        // Verify all tiles were written
        int count = 0;
        for (int[] tile : tiles) {
            File tileFile = new File(outputDir, level + "/" + tile[0] + "/" + tile[1] + ".png");
            if (tileFile.exists() && tileFile.length() > 0) {
                count++;
            }
        }

        assertEquals("All tiles should be generated", tiles.length, count);

        double totalTime = (endTime - startTime) / 1_000_000.0;
        System.out.println("✓ Generated " + count + " tiles to sink in " + String.format("%.2f", totalTime) + " ms");
        System.out.println("  Average: " + String.format("%.2f", totalTime / count) + " ms/tile");
    }

    @Test
    public void testSuccessfulTileGeneration() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        try (FileInputStream fis = new FileInputStream(imageFile)) {
            File outputDir = Files.createTempDirectory("testSuccess").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            TileGenerationResult result = tiler.generateTile(fis, sink, 6, 10, 10);

            assertTrue("Result should be successful", result.isSuccess());
            assertEquals("Status should be SUCCESS", TileGenerationResult.Status.SUCCESS, result.getStatus());
            assertNull("Message should be null for success", result.getMessage());

            File tileFile = new File(outputDir, "6/10/10.png");
            assertTrue("Tile file should exist", tileFile.exists());
            BufferedImage tile = ImageIO.read(tileFile);
            assertEquals("Tile width should be 256", 256, tile.getWidth());
            assertEquals("Tile height should be 256", 256, tile.getHeight());

            System.out.println("✓ " + result);
        }
    }

    @Test
    public void testOutOfBoundsTileCoordinates() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        try (FileInputStream fis = new FileInputStream(imageFile)) {
            File outputDir = Files.createTempDirectory("testOutOfBounds").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            TileGenerationResult result = tiler.generateTile(fis, sink, 6, 1000, 1000);

            assertFalse("Result should not be successful", result.isSuccess());
            assertEquals("Status should be OUT_OF_BOUNDS",
                    TileGenerationResult.Status.OUT_OF_BOUNDS, result.getStatus());
            assertNotNull("Message should be present", result.getMessage());
            assertTrue("Should be coordinate error", result.isCoordinateError());
            assertFalse("Should not be image error", result.isImageError());

            System.out.println("✓ " + result);
            System.out.println("  Message: " + result.getMessage());
        }
    }

    @Test
    public void testInvalidZoomLevel() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        try (FileInputStream fis = new FileInputStream(imageFile)) {
            File outputDir = Files.createTempDirectory("testInvalidLevel").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            TileGenerationResult result = tiler.generateTile(fis, sink, 999, 0, 0);

            assertFalse("Result should not be successful", result.isSuccess());
            assertEquals("Status should be INVALID_LEVEL",
                    TileGenerationResult.Status.INVALID_LEVEL, result.getStatus());
            assertNotNull("Message should be present", result.getMessage());
            assertTrue("Should be coordinate error", result.isCoordinateError());

            System.out.println("✓ " + result);
            System.out.println("  Message: " + result.getMessage());
        }
    }

    @Test
    public void testNegativeZoomLevel() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        try (FileInputStream fis = new FileInputStream(imageFile)) {
            File outputDir = Files.createTempDirectory("testNegativeLevel").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            TileGenerationResult result = tiler.generateTile(fis, sink, -1, 0, 0);

            assertFalse("Result should not be successful", result.isSuccess());
            assertEquals("Status should be INVALID_LEVEL",
                    TileGenerationResult.Status.INVALID_LEVEL, result.getStatus());

            System.out.println("✓ " + result);
        }
    }

    @Test
    public void testNotAnImage() throws IOException {
        // Feed non-image data
        byte[] notAnImage = "This is not an image file".getBytes();
        ByteArrayInputStream bais = new ByteArrayInputStream(notAnImage);

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        File outputDir = Files.createTempDirectory("testNotAnImage").toFile();
        TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
        TileGenerationResult result = tiler.generateTile(bais, sink, 0, 0, 0);

        assertFalse("Result should not be successful", result.isSuccess());
        assertEquals("Status should be NOT_AN_IMAGE",
                TileGenerationResult.Status.NOT_AN_IMAGE, result.getStatus());
        assertNotNull("Message should be present", result.getMessage());
        assertTrue("Should be image error", result.isImageError());
        assertFalse("Should not be coordinate error", result.isCoordinateError());

        System.out.println("✓ " + result);
        System.out.println("  Message: " + result.getMessage());
    }

    @Test
    public void testResultErrorClassification() throws IOException {
        File imageFile = getImageFile("large_test_10000x10000.jpg");
        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler tiler = new OnDemandImageTiler(config);

        // Test coordinate errors
        try (FileInputStream fis = new FileInputStream(imageFile)) {
            File outputDir = Files.createTempDirectory("testErrorClassification1").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            TileGenerationResult outOfBounds = tiler.generateTile(fis, sink, 6, 1000, 1000);
            assertTrue("Out of bounds should be coordinate error", outOfBounds.isCoordinateError());
            assertFalse("Out of bounds should not be image error", outOfBounds.isImageError());
        }

        try (FileInputStream fis = new FileInputStream(imageFile)) {
            File outputDir = Files.createTempDirectory("testErrorClassification2").toFile();
            TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
            TileGenerationResult invalidLevel = tiler.generateTile(fis, sink, 999, 0, 0);
            assertTrue("Invalid level should be coordinate error", invalidLevel.isCoordinateError());
            assertFalse("Invalid level should not be image error", invalidLevel.isImageError());
        }

        // Test image errors
        byte[] notAnImage = "Not an image".getBytes();
        File outputDir = Files.createTempDirectory("testErrorClassification3").toFile();
        TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDir, true));
        TileGenerationResult notImage = tiler.generateTile(
                new ByteArrayInputStream(notAnImage), sink, 0, 0, 0);
        assertTrue("Not an image should be image error", notImage.isImageError());
        assertFalse("Not an image should not be coordinate error", notImage.isCoordinateError());

        System.out.println("✓ Error classification works correctly");
    }
}

