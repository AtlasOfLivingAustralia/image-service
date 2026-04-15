package au.org.ala.images.jna

import au.org.ala.images.thumb.JnaStreamingImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.tiling.JnaStreamingImageTiler
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.Files
import com.google.common.io.Resources
import spock.lang.Specification
import spock.lang.TempDir
import groovy.util.logging.Slf4j
import au.org.ala.images.tiling.ImageTilerConfig
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Slf4j
class JnaStreamingImageSpec extends Specification {

    @TempDir
    File tempDir

    ExecutorService ioExecutor
    ExecutorService levelExecutor

    def setup() {
        ioExecutor = Executors.newFixedThreadPool(2)
        levelExecutor = Executors.newFixedThreadPool(2)
    }

    def cleanup() {
        ioExecutor?.shutdown()
        levelExecutor?.shutdown()
    }

    def "test JnaStreamingImageThumbnailer"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            log.warn "libvips not available, skipping JnaStreamingImageThumbnailer test"
            return
        }

        def thumbnailer = new JnaStreamingImageThumbnailer(null)
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        def thumbDef = new ThumbDefinition(100, true, null, "test-thumb.jpg")
        def byteSinkFactory = new TestByteSinkFactory(tempDir)

        when:
        def results = thumbnailer.generateThumbnails(imageBytes, byteSinkFactory, [thumbDef])

        then:
        results.size() == 1
        results[0].thumbnailName == "test-thumb.jpg"
        new File(tempDir, "test-thumb.jpg").exists()
        results[0].width <= 100
        results[0].height <= 100
    }

    def "test JnaStreamingImageTiler"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            log.warn "libvips not available, skipping JnaStreamingImageTiler test"
            return
        }

        def config = new ImageTilerConfig(ioExecutor, levelExecutor)
        def tiler = new JnaStreamingImageTiler(null, config)
        def imageResource = "test.jpg"
        def inputStream = Resources.getResource(imageResource).openStream()
        def outputDir = new File(tempDir, "tiles")
        outputDir.mkdirs()
        def tilerSink = new TilerSink.PathBasedTilerSink(new TestByteSinkFactory(outputDir))

        when:
        def results = tiler.tileImage(inputStream, tilerSink, 0, 2)

        then:
        results.success
        results.zoomLevels > 0
        new File(outputDir, "0/0/0.png").exists()
    }

    static class TestByteSinkFactory implements ByteSinkFactory {
        File dir
        TestByteSinkFactory(File dir) { this.dir = dir }
        @Override void prepare() {}
        @Override ByteSink getByteSinkForNames(String... names) {
            File f = dir
            for (String name : names) {
                f = new File(f, name)
            }
            f.parentFile.mkdirs()
            return Files.asByteSink(f)
        }
    }
}
