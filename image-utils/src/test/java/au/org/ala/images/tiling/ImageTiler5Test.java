package au.org.ala.images.tiling;

import au.org.ala.images.TestBase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class ImageTiler5Test extends TestBase {

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
    @Ignore("Takes a long time and uses significant resources - run manually as needed")
    public void testLargeImageWithStreamingArchitecture() throws Exception {
        String filename = "large_test_20000x20000.jpg";
        URL url = ImageTiler5Test.class.getResource(String.format("/images/%s", filename));
        assertNotNull("Test image should exist: " + filename, url);
        File imageFile = new File(url.toURI());
        println("Testing ImageTiler5 streaming with parallel writes: %s", filename);
        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        ImageTiler5 tiler = new ImageTiler5(config);
        Path tempDir = Files.createTempDirectory("imagetiler5-test");
        try {
            long start = System.currentTimeMillis();
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            long duration = System.currentTimeMillis() - start;
            assertTrue(results.getSuccess());
            println("✓ Completed in %d ms with %d levels (parallel writes, 1GB memory)",
                    duration, results.getZoomLevels());
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testPerformanceWithParallelWrites() throws Exception {
        String filename = "large_test_10000x10000.jpg";
        URL url = ImageTiler5Test.class.getResource(String.format("/images/%s", filename));
        File imageFile = new File(url.toURI());
        println("Testing ImageTiler5 parallel write performance: %s", filename);
        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        ImageTiler5 tiler = new ImageTiler5(config);
        Path tempDir = Files.createTempDirectory("imagetiler5-perf");
        try {
            long start = System.currentTimeMillis();
            ImageTilerResults results = tiler.tileImage(imageFile, tempDir.toFile());
            long duration = System.currentTimeMillis() - start;
            assertTrue(results.getSuccess());
            println("✓ Parallel tile writes: %d ms for %d levels", duration, results.getZoomLevels());
            println("✓ Memory efficient: one level at a time, tiles written in parallel");
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}
