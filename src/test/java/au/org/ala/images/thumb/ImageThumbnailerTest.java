package au.org.ala.images.thumb;

import au.org.ala.images.TestBase;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

@RunWith(JUnit4.class)
public class ImageThumbnailerTest extends TestBase{

    @Test
    public void test1() throws Exception {
        createThumbs("CYMK_2078907.jpeg");
    }

    private void createThumbs(String filename) throws Exception {
        URL url = ImageThumbnailerTest.class.getResource(String.format("/images/%s", filename));
        println("Thumbnailing: %s", url);
        File imageFile = new File(url.toURI());

        Path tempDir = Files.createTempDirectory("imagetests");
        try {
            byte[] imageBytes = FileUtils.readFileToByteArray(imageFile);
            ThumbnailingResults results = new ImageThumbnailer().generateThumbnails(imageBytes, tempDir.toFile(), 300, new String[] {"","black", "white", "darkGray"} );
            for (String s : results.getThumbnailNames()) {
                System.out.println(s);
            }

        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }

    }


}
