package au.org.ala.images.util;

import com.google.common.io.Resources;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class ImageReaderUtilsTest {

    public static final int LANDSCAPE_WIDTH = 600;
    public static final int LANDSCAPE_HEIGHT = 450;
    public static final int PORTRAIT_WIDTH = 450;
    public static final int PORTRAIT_HEIGHT = 600;

    @Test
    public void testImageDimensions() {
        var landScapeImages = List.of(
                "images/orientation/landscape_1.jpg",
                "images/orientation/landscape_2.jpg",
                "images/orientation/landscape_3.jpg",
                "images/orientation/landscape_4.jpg",
                "images/orientation/landscape_5.jpg",
                "images/orientation/landscape_6.jpg",
                "images/orientation/landscape_7.jpg",
                "images/orientation/landscape_8.jpg"
        );

        for (var image : landScapeImages) {
            var dimensions = ImageReaderUtils.getImageDimensions(Resources.asByteSource(Resources.getResource(image)), FilenameUtils.getName(image));
            assertNotNull(dimensions);
            assertEquals(LANDSCAPE_WIDTH, dimensions.width);
            assertEquals(LANDSCAPE_HEIGHT, dimensions.height);
        }

        var portraitImages = List.of(
                "images/orientation/portrait_1.jpg",
                "images/orientation/portrait_2.jpg",
                "images/orientation/portrait_3.jpg",
                "images/orientation/portrait_4.jpg",
                "images/orientation/portrait_5.jpg",
                "images/orientation/portrait_6.jpg",
                "images/orientation/portrait_7.jpg",
                "images/orientation/portrait_8.jpg"
        );

        for (var image : portraitImages) {
            var dimensions = ImageReaderUtils.getImageDimensions(Resources.asByteSource(Resources.getResource(image)), FilenameUtils.getName(image));
            assertNotNull(dimensions);
            assertEquals(PORTRAIT_WIDTH, dimensions.width);
            assertEquals(PORTRAIT_HEIGHT, dimensions.height);
        }
    }

}
