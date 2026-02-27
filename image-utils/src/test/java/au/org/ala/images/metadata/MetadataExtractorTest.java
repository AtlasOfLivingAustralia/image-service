package au.org.ala.images.metadata;


import au.org.ala.images.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

@RunWith(JUnit4.class)
public class MetadataExtractorTest extends TestBase {

    @Test
    public void test1() throws Exception {
        dumpMetadata("silky_oak.JPG");
    }

    @Test
    public void test2() throws Exception {
        dumpMetadata("x06446.mp3");
    }

    private void dumpMetadata(String filename) throws Exception {
        MetadataExtractor e = new MetadataExtractor();
        Map<String, String> md = e.readMetadata(getImageFile(filename));
        dumpMap(md);

    }

    private void dumpMap(Map<String, String> map) {
        for (String key : map.keySet()) {
            String value = map.get(key);
            println("%s: %s", key, value);
        }
    }


}
