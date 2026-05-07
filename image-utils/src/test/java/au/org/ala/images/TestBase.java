package au.org.ala.images;

import java.io.File;
import java.net.URL;

public class TestBase {

    protected void println(String fmt, Object...args) {
        System.out.println(String.format(fmt, args));
    }

    protected File getImageFile(String filename) {
        try {
            URL url = TestBase.class.getResource(String.format("/images/%s", filename));
            File imageFile = new File(url.toURI());
            return imageFile;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
