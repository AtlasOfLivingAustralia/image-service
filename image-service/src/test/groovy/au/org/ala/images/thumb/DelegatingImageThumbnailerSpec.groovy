package au.org.ala.images.thumb

import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.optimisation.ProcessCommandExecutor
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.Resources
import spock.lang.Specification
import spock.lang.TempDir

class DelegatingImageThumbnailerSpec extends Specification {

    @TempDir
    File tempDir

    def "test DelegatingImageThumbnailer finds and uses JNA thumbnailer"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            println "[DEBUG_LOG] libvips not available, skipping test"
            return
        }

        def commandExecutor = new ProcessCommandExecutor()
        def thumbnailer = new DelegatingImageThumbnailer(commandExecutor, 'vips', true)
        
        expect:
        thumbnailer.implementationNames.any { it.contains("JNA") }
        
        when:
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        def thumbDef = new ThumbDefinition(100, true, null, "test-thumb.jpg")
        def byteSinkFactory = new FileByteSinkFactory(tempDir)
        def results = thumbnailer.generateThumbnails(imageBytes, byteSinkFactory, [thumbDef])
        
        then:
        results.size() == 1
        results[0].thumbnailName == "test-thumb.jpg"
        new File(tempDir, "test-thumb.jpg").exists()
    }

    def "test DelegatingImageThumbnailer falls back to Java thumbnailer"() {
        given:
        def commandExecutor = new ProcessCommandExecutor()
        // We can't easily disable JNA/FFM here without mocking ServiceLoader or the factories,
        // but we can check that Java is at least in the list of discovered implementations.
        def thumbnailer = new DelegatingImageThumbnailer(commandExecutor, 'non-existent-tool', false)
        
        expect:
        thumbnailer.implementationNames.any { it.contains("Pure Java") }
        
        when:
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        def thumbDef = new ThumbDefinition(100, true, null, "java-thumb.jpg")
        def byteSinkFactory = new FileByteSinkFactory(tempDir)
        def results = thumbnailer.generateThumbnails(imageBytes, byteSinkFactory, [thumbDef])
        
        then:
        results.size() == 1
        results[0].thumbnailName == "java-thumb.jpg"
        new File(tempDir, "java-thumb.jpg").exists()
    }

    static class FileByteSinkFactory implements ByteSinkFactory {
        File dir
        FileByteSinkFactory(File dir) { this.dir = dir }
        @Override void prepare() {}
        @Override ByteSink getByteSinkForNames(String... names) {
            return com.google.common.io.Files.asByteSink(new File(dir, names[0]))
        }
    }
}
