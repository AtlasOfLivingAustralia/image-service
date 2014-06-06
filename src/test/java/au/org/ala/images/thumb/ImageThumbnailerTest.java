package au.org.ala.images.thumb;

import au.org.ala.images.TestBase;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.awt.*;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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

            List<ThumbnailingResult> results = new ImageThumbnailer().generateThumbnails(imageBytes, tempDir.toFile(), thumbList);
            for (ThumbnailingResult r : results) {
                System.out.println(r.getThumbnailName());
            }

        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }

    }


}
