package au.org.ala.images.iiif

import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.jna.VipsLibrary
import com.google.common.io.ByteSource
import com.google.common.io.Resources
import com.sun.jna.Memory
import com.sun.jna.Pointer
import spock.lang.Specification
import spock.lang.IgnoreIf

class JnaIiifImageProcessorSpec extends Specification {

    @IgnoreIf({ !NativeLibraryDetector.isVipsAvailable() })
    def "test arbitrary rotation"() {
        given:
        def vips = NativeLibraryDetector.getVipsLibrary()
        def javaFallback = new JavaIiifImageProcessor()
        def processor = new JnaIiifImageProcessor(javaFallback)
        
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        
        def region = IiifImageProcessor.Region.full()
        def size = IiifImageProcessor.Size.max(false)
        def rotation = new IiifImageProcessor.Rotation(false, 45.0)
        def quality = IiifImageProcessor.Quality.DEFAULT
        def format = IiifImageProcessor.Format.JPG
        def out = new ByteArrayOutputStream()
        
        when:
        def result = processor.process(imageBytes, region, size, rotation, quality, format, out)
        
        then:
        result != null
        result.width > 640
        result.height > 480
        Math.abs(result.width - 792) <= 2
        Math.abs(result.height - 792) <= 2
    }
    @IgnoreIf({ !NativeLibraryDetector.isVipsAvailable() })
    def "test mirror and arbitrary rotation"() {
        given:
        def javaFallback = new JavaIiifImageProcessor()
        def processor = new JnaIiifImageProcessor(javaFallback)
        
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        
        def region = IiifImageProcessor.Region.full()
        def size = IiifImageProcessor.Size.max(false)
        def rotation = new IiifImageProcessor.Rotation(true, 45.0)
        def quality = IiifImageProcessor.Quality.DEFAULT
        def format = IiifImageProcessor.Format.JPG
        def out = new ByteArrayOutputStream()
        
        when:
        def result = processor.process(imageBytes, region, size, rotation, quality, format, out)
        
        then:
        result != null
        result.width > 640
        result.height > 480
        Math.abs(result.width - 792) <= 2
        Math.abs(result.height - 792) <= 2
    }
    @IgnoreIf({ !NativeLibraryDetector.isVipsAvailable() })
    def "test orthogonal rotations"() {
        given:
        def javaFallback = new JavaIiifImageProcessor()
        def processor = new JnaIiifImageProcessor(javaFallback)
        def imageResource = "test.jpg"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        
        when:
        def res90 = processor.process(imageBytes, IiifImageProcessor.Region.full(), IiifImageProcessor.Size.max(false), new IiifImageProcessor.Rotation(false, 90.0), IiifImageProcessor.Quality.DEFAULT, IiifImageProcessor.Format.JPG, new ByteArrayOutputStream())
        def res180 = processor.process(imageBytes, IiifImageProcessor.Region.full(), IiifImageProcessor.Size.max(false), new IiifImageProcessor.Rotation(false, 180.0), IiifImageProcessor.Quality.DEFAULT, IiifImageProcessor.Format.JPG, new ByteArrayOutputStream())
        def res270 = processor.process(imageBytes, IiifImageProcessor.Region.full(), IiifImageProcessor.Size.max(false), new IiifImageProcessor.Rotation(false, 270.0), IiifImageProcessor.Quality.DEFAULT, IiifImageProcessor.Format.JPG, new ByteArrayOutputStream())

        then:
        res90.width == 480
        res90.height == 640
        res180.width == 640
        res180.height == 480
        res270.width == 480
        res270.height == 640
    }

    def "should use fallback when vips is unavailable"() {
        given:
        def fallback = Mock(IiifImageProcessor)
        def nullProcessor = new JnaIiifImageProcessor(null, fallback)
        def bytes = ByteSource.wrap(new byte[0])
        def out = new ByteArrayOutputStream()
        def result = new IiifImageProcessor.Result(10, 10, "image/jpeg")

        when:
        def actual = nullProcessor.process(bytes, null, null, null, null, IiifImageProcessor.Format.JPG, out)

        then:
        1 * fallback.process(bytes, null, null, null, null, IiifImageProcessor.Format.JPG, out) >> result
        actual == result
    }

    def "should fallback on native failure if no bytes were written"() {
        given:
        def vips = Mock(VipsLibrary)
        def fallback = Mock(IiifImageProcessor)
        def processor = new JnaIiifImageProcessor(vips, fallback)
        def bytes = ByteSource.wrap(new byte[0])
        def out = new ByteArrayOutputStream()
        def result = new IiifImageProcessor.Result(10, 10, "image/jpeg")

        // Mock vips_image_new_from_source to fail
        vips.vips_image_new_from_source(_, _, _) >> Pointer.NULL
        vips.vips_error_buffer() >> "Could not load image"

        when:
        def actual = processor.process(bytes, null, null, null, null, IiifImageProcessor.Format.JPG, out)

        then:
        1 * fallback.process(bytes, null, null, null, null, IiifImageProcessor.Format.JPG, out) >> result
        actual == result
    }

    def "should throw exception and NOT fallback if partial bytes were written"() {
        given:
        def vips = Mock(VipsLibrary)
        def fallback = Mock(IiifImageProcessor)
        def processor = new JnaIiifImageProcessor(vips, fallback)
        def bytes = ByteSource.wrap(new byte[100])
        def out = new ByteArrayOutputStream()

        Pointer image = Mock(Pointer)
        Pointer source = Mock(Pointer)
        Pointer target = Mock(Pointer)
        VipsLibrary.WriteCallback capturedCallback = null

        // Mock success until write
        vips.vips_source_custom_new() >> source
        vips.vips_image_new_from_source(source, _, _) >> image
        vips.vips_image_get_width(image) >> 100
        vips.vips_image_get_height(image) >> 100
        vips.vips_target_custom_new() >> target
        
        vips.g_signal_connect_data(*_) >> { args ->
            if (args[1] == "write") {
                capturedCallback = (VipsLibrary.WriteCallback) args[2]
            }
            return 1L
        }

        // Mock write failure but only after calling the callback to simulate some progress
        vips.vips_image_write_to_target(*_) >> { args ->
            Pointer t = (Pointer) args[2]
            if (capturedCallback) {
                capturedCallback.invoke(t, new Memory(10), 10, Pointer.NULL)
            }
            return -1
        }
        vips.vips_error_buffer() >> "Write failed after 10 bytes"

        when:
        processor.process(bytes, null, null, null, null, IiifImageProcessor.Format.JPG, out)

        then:
        def e = thrown(IOException)
        e.message.contains("partial bytes already written to output")
        0 * fallback.process(*_)
    }
}
