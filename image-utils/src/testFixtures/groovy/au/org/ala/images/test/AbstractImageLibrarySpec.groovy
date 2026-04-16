package au.org.ala.images.test

import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.ByteSource
import com.google.common.io.Files
import com.google.common.io.Resources
import spock.lang.Specification
import spock.lang.TempDir

import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

abstract class AbstractImageLibrarySpec extends Specification {

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

    abstract ImageLibraryFactory getFactory()

    abstract Map<String, String> getCommands()

    CommandExecutor getCommandExecutor() {
        return Mock(CommandExecutor)
    }

    def "thumbnailer generates a thumbnail"() {
        given:
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        def thumbnailer = factory.createThumbnailer(getCommandExecutor(), getCommands(), null)
        if (thumbnailer == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support thumbnailing, skipping"
            return
        }

        def imageResource = "images/audio-icon.png"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        def thumbDef = new ThumbDefinition(100, true, null, "test-thumb.png")
        def sinkFactory = new SimpleByteSinkFactory(tempDir)

        when:
        def results = thumbnailer.generateThumbnails(imageBytes, sinkFactory, [thumbDef])

        then:
        results.size() == 1
        results[0].thumbnailName == "test-thumb.png"
        results[0].width > 0
        new File(tempDir, "test-thumb.png").exists()
    }

    def "tiler generates tiles"() {
        given:
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        def config = new ImageTilerConfig(ioExecutor, levelExecutor)
        def tiler = factory.createTiler(getCommandExecutor(), config, getCommands(), null)
        if (tiler == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support tiling, skipping"
            return
        }

        def imageResource = "images/audio-icon.png"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        def imageFile = new File(tempDir, "source-image.png")
        imageBytes.copyTo(Files.asByteSink(imageFile))
        def outputDir = new File(tempDir, "tiles")
        outputDir.mkdirs()

        when:
        def results = tiler.tileImage(imageFile, outputDir)

        then:
        results.success
        results.zoomLevels > 0
        new File(outputDir, "0").exists()
    }

    def "iiif processor processes an image"() {
        given:
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        def processor = factory.createIiifProcessor(getCommandExecutor(), getCommands(), null)
        if (processor == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support IIIF, skipping"
            return
        }

        def imageResource = "images/audio-icon.png"
        def imageBytes = Resources.asByteSource(Resources.getResource(imageResource))
        def out = new ByteArrayOutputStream()

        when:
        def result = processor.process(
                imageBytes,
                IiifImageProcessor.Region.full(),
                IiifImageProcessor.Size.width(100, false),
                IiifImageProcessor.Rotation.none(),
                IiifImageProcessor.Quality.DEFAULT,
                IiifImageProcessor.Format.PNG,
                out
        )

        then:
        result != null
        result.width == 100
        out.size() > 0
    }

    def "on-demand tiler generates a tile"() {
        given:
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        def config = new ImageTilerConfig(ioExecutor, levelExecutor)
        def tiler = factory.createOnDemandTiler(getCommandExecutor(), config, getCommands(), null)
        if (tiler == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support on-demand tiling, skipping"
            return
        }

        def imageResource = "images/audio-icon.png"
        def inputStream = Resources.getResource(imageResource).openStream()
        def sink = new TilerSink.PathBasedTilerSink(new SimpleByteSinkFactory(tempDir))

        when:
        def result = tiler.generateTile(inputStream, sink, 0, 0, 0)

        then:
        result != null
        result.success
        new File(tempDir, "0/0/0.png").exists()
    }

    static class SimpleByteSinkFactory implements ByteSinkFactory {
        File dir
        SimpleByteSinkFactory(File dir) { this.dir = dir }
        @Override void prepare() {}
        @Override ByteSink getByteSinkForNames(String... names) {
            File file = dir
            for (String name : names) {
                file = new File(file, name)
            }
            file.parentFile.mkdirs()
            return Files.asByteSink(file)
        }
    }

}
