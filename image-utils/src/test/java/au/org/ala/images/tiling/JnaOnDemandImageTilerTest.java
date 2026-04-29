package au.org.ala.images.tiling;

import au.org.ala.images.jna.NativeLibraryDetector;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

import au.org.ala.images.util.FileByteSinkFactory;

/**
 * Tests for the JNA-based on-demand tiler.
 */
public class JnaOnDemandImageTilerTest {

    private static File tempDir;
    private static File testImage;

    private ExecutorService ioExecutor;
    private ExecutorService levelExecutor;

    @Before
    public void setupTest() {
        ioExecutor = Executors.newFixedThreadPool(2);
        levelExecutor = Executors.newFixedThreadPool(2);
    }

    @After
    public void tearDown() {
        if (ioExecutor != null) ioExecutor.shutdown();
        if (levelExecutor != null) levelExecutor.shutdown();
    }

    @BeforeClass
    public static void setup() throws IOException {
        tempDir = new File("build/tmp/jna-tiling-test");
        tempDir.mkdirs();
        testImage = new File(tempDir, "test-500x500.jpg");
        
        BufferedImage img = new BufferedImage(500, 500, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setColor(Color.RED);
        g.fillRect(0, 0, 250, 250);
        g.setColor(Color.GREEN);
        g.fillRect(250, 0, 250, 250);
        g.setColor(Color.BLUE);
        g.fillRect(0, 250, 250, 250);
        g.setColor(Color.YELLOW);
        g.fillRect(250, 250, 250, 250);
        g.dispose();
        
        ImageIO.write(img, "jpg", testImage);
    }

    @Test
    public void testJnaMatchesJava() throws IOException {
        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        OnDemandImageTiler javaTiler = new OnDemandImageTiler(config);
        JnaOnDemandImageTiler jnaTiler = new JnaOnDemandImageTiler(config, javaTiler);

        if (!NativeLibraryDetector.isVipsAvailable()) {
            System.out.println("VIPS not available, skipping JNA specific checks");
            return;
        }

        File outputDirJava = new File(tempDir, "java");
        File outputDirJna = new File(tempDir, "jna");
        outputDirJava.mkdirs();
        outputDirJna.mkdirs();

        TilerSink sinkJava = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDirJava, true));
        TilerSink sinkJna = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(outputDirJna, true));

        try (FileInputStream fis1 = new FileInputStream(testImage); 
             FileInputStream fis2 = new FileInputStream(testImage)) {
            TileGenerationResult resultJava = javaTiler.generateTile(fis1, sinkJava, 1, 0, 0); 
            TileGenerationResult resultJna = jnaTiler.generateTile(fis2, sinkJna, 1, 0, 0); 
            
            assertTrue(resultJava.isSuccess());
            assertTrue(resultJna.isSuccess());
            
            BufferedImage imgJava = ImageIO.read(new File(outputDirJava, "1/0/0.png"));
            BufferedImage imgJna = ImageIO.read(new File(outputDirJna, "1/0/0.png"));

            Color cJava = new Color(imgJava.getRGB(10, 10));
            Color cJna = new Color(imgJna.getRGB(10, 10));
            
            // Allow for small differences due to different encoders or minor subsampling differences
            assertTrue("Red should match within 10", Math.abs(cJava.getRed() - cJna.getRed()) < 10);
            assertTrue("Green should match within 10", Math.abs(cJava.getGreen() - cJna.getGreen()) < 10);
            assertTrue("Blue should match within 10", Math.abs(cJava.getBlue() - cJna.getBlue()) < 10);
        }
    }

    @Test
    public void testNullFallbackThrowsUsefulErrorWhenVipsUnavailable() throws IOException {
        if (NativeLibraryDetector.isVipsAvailable()) {
            System.out.println("VIPS available, skipping null-fallback guard test");
            return;
        }

        ImageTilerConfig config = new ImageTilerConfig(ioExecutor, levelExecutor);
        JnaOnDemandImageTiler jnaTiler = new JnaOnDemandImageTiler(config, null);
        TilerSink sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(new File(tempDir, "null-fallback"), true));

        IllegalStateException error;
        try (InputStream inputStream = new FileInputStream(testImage)) {
            error = assertThrows(IllegalStateException.class,
                () -> jnaTiler.generateTile(inputStream, sink, 1, 0, 0));
        }

        assertEquals("JNA tiler fallback is not configured and libvips is unavailable", error.getMessage());
    }
}
