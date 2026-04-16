package au.org.ala.images.vipsffm

import app.photofox.vipsffm.Vips
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TileFormat
import au.org.ala.images.util.FileByteSinkFactory
import spock.lang.Specification
import spock.lang.IgnoreIf

import java.nio.file.Files
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class VipsFfmStreamingImageTilerSpec extends Specification {

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

    def setupSpec() {
        try {
            Vips.init()
        } catch (Throwable e) {
            // Ignore init failure
        }
    }

    def "test tileImage for a single level"() {
        given:
        def fallback = Mock(IImageTiler)
        def config = new ImageTilerConfig(ioExecutor, levelExecutor, 256, 6, TileFormat.JPEG)
        def tiler = new VipsFfmStreamingImageTiler(fallback, config)
        
        def imageFile = new File("../image-utils/src/test/resources/images/orientation/portrait_1.jpg")
        if (!imageFile.exists()) {
            imageFile = new File("image-utils/src/test/resources/images/orientation/portrait_1.jpg")
        }
        def imageStream = imageFile.newInputStream()
        
        def tempDir = Files.createTempDirectory("photofox-tiler-spec")
        def sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(tempDir.toFile()))
        
        when:
        def results = tiler.tileImage(imageStream, sink, 0)
        
        then:
        results.success
        results.zoomLevels > 0
        
        // Check if level 0 directory exists and has tiles
        def level0Dir = new File(tempDir.toFile(), "0")
        level0Dir.exists()
        level0Dir.isDirectory()
        
        cleanup:
        tempDir?.toFile()?.deleteDir()
        imageStream?.close()
    }
}
