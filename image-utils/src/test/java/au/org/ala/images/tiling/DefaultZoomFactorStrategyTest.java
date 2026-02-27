package au.org.ala.images.tiling;

import au.org.ala.images.TestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DefaultZoomFactorStrategyTest extends TestBase {

    @Test
    public void test() {
        var zfs = new DefaultZoomFactorStrategy(256);
        Assert.assertEquals(1, zfs.getZoomFactors(128, 128).length);
        Assert.assertEquals(1, zfs.getZoomFactors(256, 256).length);
        Assert.assertEquals(2, zfs.getZoomFactors(512, 512).length);
        Assert.assertEquals(3, zfs.getZoomFactors(1024, 1024).length);
        Assert.assertEquals(4, zfs.getZoomFactors(2048, 2048).length);
        Assert.assertEquals(5, zfs.getZoomFactors(4096, 4096).length);
        Assert.assertEquals(6, zfs.getZoomFactors(8192, 8192).length);
        Assert.assertEquals(7, zfs.getZoomFactors(16384, 16384).length);
        Assert.assertEquals(8, zfs.getZoomFactors(32768, 32768).length);
        Assert.assertEquals(8, zfs.getZoomFactors(65536, 65536).length);

        Assert.assertEquals(2, zfs.getZoomFactors(384, 192).length);
        Assert.assertEquals(3, zfs.getZoomFactors(768, 384).length);
        Assert.assertEquals(4, zfs.getZoomFactors(1536, 768).length);
        Assert.assertEquals(5, zfs.getZoomFactors(3072, 1536).length);
        Assert.assertEquals(6, zfs.getZoomFactors(6144, 3072).length);
        Assert.assertEquals(7, zfs.getZoomFactors(12288, 6144).length);
        Assert.assertEquals(8, zfs.getZoomFactors(24576, 12288).length);

        Assert.assertEquals(2, zfs.getZoomFactors(192, 384).length);
        Assert.assertEquals(3, zfs.getZoomFactors(384, 768).length);
        Assert.assertEquals(4, zfs.getZoomFactors(768, 1536).length);
        Assert.assertEquals(5, zfs.getZoomFactors(1536, 3072).length);
        Assert.assertEquals(6, zfs.getZoomFactors(3072, 6144).length);
        Assert.assertEquals(7, zfs.getZoomFactors(6144, 12288).length);
        Assert.assertEquals(8, zfs.getZoomFactors(12288, 24576).length);

    }
}
