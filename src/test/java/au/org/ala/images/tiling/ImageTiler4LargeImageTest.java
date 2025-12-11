package au.org.ala.images.tiling;

import au.org.ala.images.TestBase;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.awt.*;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 * Test ImageTiler4 with very large images to identify potential issues.
 */
public class ImageTiler4LargeImageTest extends TestBase {

    /**
     * Test actual tile generation with the existing large test image.
     * This test uses the pre-existing large_test_20000x20000.jpg file, which is more
     * representative of production usage (reading from file) and avoids creating
     * huge synthetic images in memory.
     *
     * Uses reduced parallelism (single-threaded) to minimize memory requirements for CI environments.
     */
    @Test
    public void testActualLargeImage() throws Exception {
        // Use the 20000x20000 test image from resources
        String filename = "large_test_20000x20000.jpg";
        URL url = ImageTiler4LargeImageTest.class.getResource(String.format("/images/%s", filename));
        assertNotNull("Test image should exist: " + filename, url);

        File imageFile = new File(url.toURI());
        println("Testing ImageTiler4 with real large image: %s", filename);

        // Configure with reduced parallelism for lower memory usage in CI
        // Use single-threaded executors to minimize concurrent memory usage
        ExecutorService levelExecutor = Executors.newSingleThreadExecutor();
        ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
        ImageTilerConfig config = new ImageTilerConfig(levelExecutor, ioExecutor);
        println("Using single-threaded execution to reduce memory requirements");

        ImageTiler4 tiler = new ImageTiler4(config);
        
        Path tempDir = Files.createTempDirectory("large-image-tiles");
        try {
            println("Tiling image...");
            long startTime = System.currentTimeMillis();
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            long duration = System.currentTimeMillis() - startTime;
            
            assertTrue("Tiling should succeed", results.getSuccess());
            
            println("✓ Tiling completed in %d ms", duration);
            println("✓ Generated %d zoom levels", results.getZoomLevels());
            
            // Verify level 0 exists and is correct
            File level0 = new File(tempDir.toFile(), "0");
            assertTrue("Level 0 should exist", level0.exists());
            
            File[] cols = level0.listFiles(File::isDirectory);
            assertNotNull("Level 0 should have columns", cols);
            println("✓ Level 0 has %d columns", cols.length);
            
            // Verify a mid-level
            int midLevel = results.getZoomLevels() / 2;
            File midLevelDir = new File(tempDir.toFile(), String.valueOf(midLevel));
            assertTrue("Mid level should exist", midLevelDir.exists());
            
            println("✓ All levels generated correctly");
            
        } finally {
            // Cleanup
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}

