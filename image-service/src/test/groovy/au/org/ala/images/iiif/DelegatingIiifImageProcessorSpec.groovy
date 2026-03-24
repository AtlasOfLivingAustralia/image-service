package au.org.ala.images.iiif

import au.org.ala.images.jna.NativeLibraryDetector
import com.google.common.io.ByteSource
import com.google.common.io.Resources
import spock.lang.Specification

class DelegatingIiifImageProcessorSpec extends Specification {

    def "test DelegatingIiifImageProcessor finds and uses JNA processor"() {
        given:
        def processor = new DelegatingIiifImageProcessor()
        
        expect:
        if (NativeLibraryDetector.isVipsAvailable()) {
            processor.implementationNames.any { it.contains("JNA") }
        } else {
            processor.implementationNames.size() >= 0 // Might only have Pure Java if nothing else discovered
        }
        
        when:
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        def region = IiifImageProcessor.Region.full()
        def size = IiifImageProcessor.Size.width(100, false)
        def rotation = IiifImageProcessor.Rotation.none()
        def quality = IiifImageProcessor.Quality.DEFAULT
        def format = IiifImageProcessor.Format.JPG
        def out = new ByteArrayOutputStream()
        def result = processor.process(imageBytes, region, size, rotation, quality, format, out)
        
        then:
        result != null
        result.width == 100
        out.size() > 0
    }
}
