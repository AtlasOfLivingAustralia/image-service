package au.org.ala.images.iiif;

import au.org.ala.images.TestBase;
import com.google.common.io.ByteSource;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class IiifImageProcessorTest extends TestBase {

    private BufferedImage readImage(String filename) throws Exception {
        URL url = IiifImageProcessorTest.class.getResource(String.format("/images/%s", filename));
        assertNotNull("Test image not found in resources: " + filename, url);
        return ImageIO.read(new File(url.toURI()));
    }

    private byte[] readImageBytes(String filename) throws Exception {
        URL url = IiifImageProcessorTest.class.getResource(String.format("/images/%s", filename));
        assertNotNull("Test image not found in resources: " + filename, url);
        return FileUtils.readFileToByteArray(new File(url.toURI()));
    }

    @Test
    public void testRegionSizeRotationQualityFormat_pipeline() throws Exception {
        // Use a small PNG from test resources
        String filename = "audio-icon.png";
        BufferedImage src = readImage(filename);
        assertNotNull(src);
        int srcW = src.getWidth();
        int srcH = src.getHeight();

        // IIIF-style parameters: left half region → width=100 → mirror+rotate 90 → grayscale → PNG
        IiifImageProcessor.Region region = IiifImageProcessor.Region.percent(0, 0, 50, 100);
        IiifImageProcessor.Size size = IiifImageProcessor.Size.width(100, false);
        IiifImageProcessor.Rotation rotation = new IiifImageProcessor.Rotation(true, 90);
        IiifImageProcessor.Quality quality = IiifImageProcessor.Quality.GRAY;
        IiifImageProcessor.Format format = IiifImageProcessor.Format.PNG;

        // Compute expected dimensions independently
        // Region (left half)
        int regW = Math.max(1, (int) Math.round(srcW * 50 / 100.0));
        int regH = Math.max(1, (int) Math.round(srcH * 100 / 100.0));
        // Size: width=100, maintain aspect
        int sizedW = 100;
        int sizedH = (int) Math.round(regH * (sizedW / (double) regW));
        if (sizedH <= 0) sizedH = 1;
        // Rotation: 90 degrees (mirror first does not change dimensions), swap w/h
        int expectedW = sizedH;
        int expectedH = sizedW;

        byte[] bytes = readImageBytes(filename);
        var proc = new IiifImageProcessor();
        try (var out = new ByteArrayOutputStream()) {
            IiifImageProcessor.Result result = proc.process(ByteSource.wrap(bytes), region, size, rotation, quality, format, out);
            assertNotNull(result);
            assertEquals("image/png", result.mimeType);
            assertEquals("width mismatch after pipeline", expectedW, result.width);
            assertEquals("height mismatch after pipeline", expectedH, result.height);
            assertTrue("Output should not be empty", out.size() > 0);
        }
    }

    @Test
    public void testBestFitAndJpegEncoding() throws Exception {
        // Use a JPEG from test resources
        String filename = "CYMK_2078907.jpeg";
        BufferedImage src = readImage(filename);
        assertNotNull(src);
        int srcW = src.getWidth();
        int srcH = src.getHeight();

        // Region: full → Size: best-fit within 200x150 → Rotation: none → Quality: default → Format: JPG
        IiifImageProcessor.Region region = IiifImageProcessor.Region.full();
        IiifImageProcessor.Size size = IiifImageProcessor.Size.bestFit(200, 150, false);
        IiifImageProcessor.Rotation rotation = IiifImageProcessor.Rotation.none();
        IiifImageProcessor.Quality quality = IiifImageProcessor.Quality.DEFAULT;
        IiifImageProcessor.Format format = IiifImageProcessor.Format.JPG;

        // Compute expected best-fit independently
        double scale = Math.min(200 / (double) srcW, 150 / (double) srcH);
        int expectedW = Math.max(1, (int) Math.round(srcW * scale));
        int expectedH = Math.max(1, (int) Math.round(srcH * scale));

        byte[] bytes = readImageBytes(filename);
        var proc = new IiifImageProcessor();
        try (var out = new ByteArrayOutputStream()) {
            IiifImageProcessor.Result result = proc.process(ByteSource.wrap(bytes), region, size, rotation, quality, format, out);
            assertNotNull(result);
            assertEquals("image/jpeg", result.mimeType);
            assertEquals(expectedW, result.width);
            assertEquals(expectedH, result.height);
            assertTrue(out.size() > 0);
        }
    }

    @Test
    public void testAspectRegionLargestCrop() throws Exception {
        String filename = "audio-icon.png"; // 300x300, AR 16:9 crop should be 300x168
        BufferedImage src = readImage(filename);
        assertNotNull(src);
        int srcW = src.getWidth();
        int srcH = src.getHeight();

        // Request a 16:9 aspect crop
        IiifImageProcessor.Region region = IiifImageProcessor.Region.parse("ar:16,9");
        IiifImageProcessor.Size size = IiifImageProcessor.Size.max(false);
        IiifImageProcessor.Rotation rotation = IiifImageProcessor.Rotation.none();
        IiifImageProcessor.Quality quality = IiifImageProcessor.Quality.DEFAULT;
        IiifImageProcessor.Format format = IiifImageProcessor.Format.PNG;

        // Expected maximal-area centered crop dimensions maintaining exact 16:9 integer ratio
        int expectedW = 300;
        int expectedH = 168;
        double aspect = 16.0 / 9.0;

        byte[] bytes = readImageBytes(filename);
        var proc = new IiifImageProcessor();
        try (var out = new ByteArrayOutputStream()) {
            IiifImageProcessor.Result result = proc.process(ByteSource.wrap(bytes), region, size, rotation, quality, format, out);
            assertNotNull(result);
            assertEquals(expectedW, result.width);
            assertEquals(expectedH, result.height);
            // Verify aspect ratio is exact within 1px rounding
            double actualAspect = result.width / (double) result.height;
            assertEquals(aspect, actualAspect, 1e-2);
            assertTrue(out.size() > 0);
        }
    }
}
