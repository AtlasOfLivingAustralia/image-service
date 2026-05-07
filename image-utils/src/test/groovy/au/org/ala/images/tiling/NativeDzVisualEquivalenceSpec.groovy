package au.org.ala.images.tiling

import au.org.ala.images.jna.NativeDzTilerBridgeLoader
import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.test.benchmark.TileVisualEquivalenceSupport
import au.org.ala.images.test.benchmark.TilerBenchmarkSupport
import spock.lang.Specification

import java.util.concurrent.Executor

/**
 * Visual-equivalence harness for comparing NativeDzStreamingImageTiler output against
 * the current JNA streaming tiler.
 */
class NativeDzVisualEquivalenceSpec extends Specification {

    static final int TILE_SIZE = Integer.getInteger("tiler.compare.tileSize", 256)
    static final int MIN_LEVEL = Integer.getInteger("tiler.compare.minLevel", 0)
    static final int MAX_LEVEL = Integer.getInteger("tiler.compare.maxLevel", Integer.MAX_VALUE)
    static final String FORMAT = System.getProperty("tiler.compare.format", "png").toLowerCase(Locale.ROOT)

    static final double MAX_RMSE = Double.parseDouble(System.getProperty("tiler.compare.maxRmse", FORMAT == "png" ? "0.0" : "6.0"))
    static final int MAX_CHANNEL_DELTA = Integer.getInteger("tiler.compare.maxChannelDelta", FORMAT == "png" ? 0 : 40)
    static final int MAX_REPORTED_MISMATCHES = Integer.getInteger("tiler.compare.maxReportedMismatches", 25)

    def "native dz tiler is visually equivalent to jna streaming tiler"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            println "[COMPARE] Skipping: libvips unavailable via JNA"
            return
        }
        if (!NativeDzTilerBridgeLoader.isAvailable()) {
            println "[COMPARE] Skipping: native dz bridge library unavailable"
            return
        }

        File imageFile = resolveImage()
        if (!imageFile.exists()) {
            println "[COMPARE] Skipping: test image not found at ${imageFile.absolutePath}"
            return
        }

        TileFormat tileFormat = FORMAT == "png" ? TileFormat.PNG : TileFormat.JPEG
        Executor sameThread = Runnable::run
        def config = new ImageTilerConfig(sameThread, sameThread, TILE_SIZE, 6, tileFormat)

        def referenceTiler = new JnaStreamingImageTiler(null, config)
        def candidateTiler = new NativeDzStreamingImageTiler(referenceTiler, config)

        def referenceCapture = TileVisualEquivalenceSupport.captureTiles()
        def candidateCapture = TileVisualEquivalenceSupport.captureTiles()

        when:
        imageFile.withInputStream { InputStream input ->
            referenceTiler.tileImage(input, referenceCapture.sink(), MIN_LEVEL, MAX_LEVEL)
        }
        imageFile.withInputStream { InputStream input ->
            candidateTiler.tileImage(input, candidateCapture.sink(), MIN_LEVEL, MAX_LEVEL)
        }

        def report = TileVisualEquivalenceSupport.compare(
                referenceCapture,
                candidateCapture,
                MAX_RMSE,
                MAX_CHANNEL_DELTA,
                MAX_REPORTED_MISMATCHES
        )

        println "[COMPARE] image=${imageFile.name} format=${tileFormat} levels=${MIN_LEVEL}-${MAX_LEVEL}"
        println "[COMPARE] thresholds rmse<=${MAX_RMSE}, maxChannelDelta<=${MAX_CHANNEL_DELTA}"
        println "[COMPARE] ${TileVisualEquivalenceSupport.renderSummary(report, 10)}"

        then:
        report.equivalent()
    }

    private static File resolveImage() {
        String explicitPath = System.getProperty("tiler.compare.image", "").trim()
        if (!explicitPath.isEmpty()) {
            return new File(explicitPath)
        }

        def images = TilerBenchmarkSupport.resolveDefaultImages()
        return images.mediumImage
    }
}

