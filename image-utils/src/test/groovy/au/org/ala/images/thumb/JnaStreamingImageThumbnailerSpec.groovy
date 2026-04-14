package au.org.ala.images.thumb

import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.ByteSource
import com.google.common.io.Resources
import spock.lang.Specification
import spock.lang.TempDir

class JnaStreamingImageThumbnailerSpec extends Specification {

    @TempDir
    File tempDir

    def "test streaming thumbnail generation with JNA"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            println "[DEBUG_LOG] libvips not available, skipping test"
            return
        }

        def fallback = Mock(IImageThumbnailer)
        def thumbnailer = new JnaStreamingImageThumbnailer(fallback)
        
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        
        def thumbDef = new ThumbDefinition(100, true, null, "test-thumb.jpg")
        def byteSinkFactory = new FileByteSinkFactory(tempDir)
        
        when:
        def results = thumbnailer.generateThumbnails(imageBytes, byteSinkFactory, [thumbDef])
        
        then:
        results.size() == 1
        results[0].thumbnailName == "test-thumb.jpg"
        results[0].width <= 100
        results[0].height <= 100
        
        def thumbFile = new File(tempDir, "test-thumb.jpg")
        thumbFile.exists()
        thumbFile.length() > 0
        0 * fallback.generateThumbnails(_, _, _)
    }

    // Minimal implementation of ByteSinkFactory for testing
    static class FileByteSinkFactory implements ByteSinkFactory {
        File dir
        FileByteSinkFactory(File dir) { this.dir = dir }
        @Override void prepare() {}
        @Override ByteSink getByteSinkForNames(String... names) {
            return com.google.common.io.Files.asByteSink(new File(dir, names[0]))
        }
    }
}
