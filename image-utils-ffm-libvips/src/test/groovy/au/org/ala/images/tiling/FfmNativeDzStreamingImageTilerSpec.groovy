package au.org.ala.images.tiling

import au.org.ala.images.ffm.NativeDzTilerBridgeLoaderFFM
import au.org.ala.images.ffm.NativeLibraryDetectorFFM
import au.org.ala.images.util.FileByteSinkFactory
import spock.lang.Specification
import spock.lang.IgnoreIf

import java.nio.file.Files

class FfmNativeDzStreamingImageTilerSpec extends Specification {

    @IgnoreIf({ !NativeLibraryDetectorFFM.vipsAvailable || !NativeDzTilerBridgeLoaderFFM.available })
    def "uses source-based native bridge call when stream is seekable"() {
        given:
        def fallback = Mock(IImageTiler)
        def config = new ImageTilerConfig(Runnable::run, Runnable::run, 256, 6, TileFormat.JPEG)
        def tiler = new FfmNativeDzStreamingImageTiler(fallback, config)

        def imageFile = new File("../image-utils/src/test/resources/images/orientation/portrait_1.jpg")
        if (!imageFile.exists()) {
            imageFile = new File("image-utils/src/test/resources/images/orientation/portrait_1.jpg")
        }
        def imageStream = imageFile.newInputStream()

        def tempDir = Files.createTempDirectory("ffm-native-dz-tiler-spec")
        def sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(tempDir.toFile()))

        when:
        def result = tiler.tileImage(imageStream, sink, 0, 0)

        then:
        result.success
        result.zoomLevels > 0
        0 * fallback.tileImage(_, _, _, _)

        and:
        def level0Dir = new File(tempDir.toFile(), "0")
        level0Dir.exists()
        level0Dir.isDirectory()

        cleanup:
        imageStream?.close()
        tempDir?.toFile()?.deleteDir()
    }

    @IgnoreIf({ !NativeLibraryDetectorFFM.vipsAvailable || !NativeDzTilerBridgeLoaderFFM.available })
    def "falls back when stream is not seekable"() {
        given:
        def fallback = Mock(IImageTiler)
        def config = new ImageTilerConfig(Runnable::run, Runnable::run, 256, 6, TileFormat.JPEG)
        def tiler = new FfmNativeDzStreamingImageTiler(fallback, config)

        def imageFile = new File("../image-utils/src/test/resources/images/orientation/portrait_1.jpg")
        if (!imageFile.exists()) {
            imageFile = new File("image-utils/src/test/resources/images/orientation/portrait_1.jpg")
        }
        def rawInput = imageFile.newInputStream()

        def input = new FilterInputStream(rawInput) {
            @Override
            boolean markSupported() {
                return false
            }
        }
        def sink = Mock(TilerSink)
        def fallbackResult = new ImageTilerResults(true, 3)

        when:
        def result = tiler.tileImage(input, sink, 0, 1)

        then:
        result.is(fallbackResult)
        1 * fallback.tileImage(input, sink, 0, 1) >> fallbackResult

        cleanup:
        input?.close()
    }
}


