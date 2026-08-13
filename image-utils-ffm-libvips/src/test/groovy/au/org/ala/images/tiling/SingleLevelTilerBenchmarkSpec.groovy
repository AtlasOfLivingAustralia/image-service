package au.org.ala.images.tiling

import au.org.ala.images.ffm.NativeDzTilerBridgeLoaderFFM
import au.org.ala.images.jna.NativeDzTilerBridgeLoader
import au.org.ala.images.test.benchmark.TilerBenchmarkSupport
import com.google.common.io.ByteSink
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.Executor

/**
 * Single-level zoom benchmark comparing all five tiler implementations:
 * - Pure Java (ImageTiler5)
 * - JNA Streaming
 * - JNA Native-DZ
 * - FFM Streaming
 * - FFM Native-DZ
 */
class SingleLevelTilerBenchmarkSpec extends Specification {

    static final int WARMUP_ROUNDS = Integer.getInteger("tiler.bench.warmupRounds", 0)
    static final int MEASURE_ROUNDS = Integer.getInteger("tiler.bench.measureRounds", 1)
    static final int TILE_SIZE = 256
    static final int TARGET_ZOOM_LEVEL = 2  // Mid-level zoom for reasonable data volume
    static final int VIPS_CONCURRENCY = Integer.getInteger("tiler.bench.vips.concurrency", 1)
    static final String IMAGE_MODE = System.getProperty("tiler.bench.image", "all").toLowerCase(Locale.ROOT)

    @Shared File mediumImage
    @Shared File largeImage

    def setupSpec() {
        def images = TilerBenchmarkSupport.resolveDefaultImages()
        mediumImage = images.mediumImage
        largeImage = images.largeImage
    }

    def "benchmark single-level on medium image"() {
        given:
        if (IMAGE_MODE == "large") {
            println "[BENCHMARK] Skipping single-level medium benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!mediumImage?.exists()) {
            println "[BENCHMARK] Skipping single-level medium benchmark - image not found: ${mediumImage}"
            return
        }

        Executor sameThread = Runnable::run
        def pureJavaConfig = buildImageTilerConfig(sameThread)
        def pureJavaTiler = new ImageTiler5(pureJavaConfig)

        // JNA streaming tiler
        def jnaStreamingTiler = new JnaStreamingImageTiler(null, pureJavaConfig)

        // JNA native-dz tiler (if available)
        def jnaNativeDzTiler = null
        if (NativeDzTilerBridgeLoader.isAvailable()) {
            jnaNativeDzTiler = new NativeDzStreamingImageTiler(jnaStreamingTiler, pureJavaConfig)
        }

        // FFM streaming tiler
        def ffmStreamingTiler = new FfmStreamingImageTiler(null, pureJavaConfig)

        // FFM native-dz tiler (if available)
        def ffmNativeDzTiler = null
        if (NativeDzTilerBridgeLoaderFFM.isAvailable()) {
            ffmNativeDzTiler = new FfmNativeDzStreamingImageTiler(ffmStreamingTiler, pureJavaConfig)
        }

        when:
        // Warmup
        TilerBenchmarkSupport.warmup(mediumImage, WARMUP_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            pureJavaTiler.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), TARGET_ZOOM_LEVEL, TARGET_ZOOM_LEVEL)
            null
        }

        // Verify all tilers actually generate tiles
        println "[VERIFY] --- Verifying all implementations generate tiles ---"
        verifyTilerWorks(mediumImage, pureJavaTiler, "Pure Java (ImageTiler5)")
        verifyTilerWorks(mediumImage, jnaStreamingTiler, "JNA Streaming")
        if (jnaNativeDzTiler != null) {
            verifyTilerWorks(mediumImage, jnaNativeDzTiler, "JNA Native-DZ")
        }
        verifyTilerWorks(mediumImage, ffmStreamingTiler, "FFM Streaming")
        if (ffmNativeDzTiler != null) {
            verifyTilerWorks(mediumImage, ffmNativeDzTiler, "FFM Native-DZ")
        }
        println "[VERIFY] --- All implementations verified ---"

        // Benchmark each tiler for the single level
        def pureJavaResult = benchmarkSingleLevel(
            mediumImage,
            pureJavaTiler,
            "Pure Java (ImageTiler5)",
            MEASURE_ROUNDS
        )

        def jnaStreamingResult = benchmarkSingleLevel(
            mediumImage,
            jnaStreamingTiler,
            "JNA Streaming",
            MEASURE_ROUNDS
        )

        def jnaNativeDzResult = null
        if (jnaNativeDzTiler != null) {
            jnaNativeDzResult = benchmarkSingleLevel(
                mediumImage,
                jnaNativeDzTiler,
                "JNA Native-DZ",
                MEASURE_ROUNDS
            )
        }

        def ffmStreamingResult = benchmarkSingleLevel(
            mediumImage,
            ffmStreamingTiler,
            "FFM Streaming",
            MEASURE_ROUNDS
        )

        def ffmNativeDzResult = null
        if (ffmNativeDzTiler != null) {
            ffmNativeDzResult = benchmarkSingleLevel(
                mediumImage,
                ffmNativeDzTiler,
                "FFM Native-DZ",
                MEASURE_ROUNDS
            )
        }

        List<TilerBenchmarkSupport.VariantResult> results = [
            pureJavaResult,
            jnaStreamingResult,
            ffmStreamingResult
        ]

        if (jnaNativeDzResult != null) {
            results.add(2, jnaNativeDzResult)  // Insert between JNA variants
        }
        if (ffmNativeDzResult != null) {
            results.add(ffmNativeDzResult)     // Add at end after FFM streaming
        }

        TilerBenchmarkSupport.printReport(
            "Single-Level (Z${TARGET_ZOOM_LEVEL}) Tiler Comparison",
            "MEDIUM (${mediumImage.name})",
            WARMUP_ROUNDS,
            MEASURE_ROUNDS,
            results
        )

        println "[DEBUG] All implementations tiled single level ${TARGET_ZOOM_LEVEL} successfully"

        then:
        pureJavaResult.timing.meanMs > 0
    }

    def "benchmark single-level on large image"() {
        given:
        if (IMAGE_MODE == "medium") {
            println "[BENCHMARK] Skipping single-level large benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!largeImage?.exists()) {
            println "[BENCHMARK] Skipping single-level large benchmark - image not found: ${largeImage}"
            return
        }

        Executor sameThread = Runnable::run
        def pureJavaConfig = buildImageTilerConfig(sameThread)
        def pureJavaTiler = new ImageTiler5(pureJavaConfig)

        // JNA streaming tiler
        def jnaStreamingTiler = new JnaStreamingImageTiler(null, pureJavaConfig)

        // JNA native-dz tiler (if available)
        def jnaNativeDzTiler = null
        if (NativeDzTilerBridgeLoader.isAvailable()) {
            jnaNativeDzTiler = new NativeDzStreamingImageTiler(jnaStreamingTiler, pureJavaConfig)
        }

        // FFM streaming tiler
        def ffmStreamingTiler = new FfmStreamingImageTiler(null, pureJavaConfig)

        // FFM native-dz tiler (if available)
        def ffmNativeDzTiler = null
        if (NativeDzTilerBridgeLoaderFFM.isAvailable()) {
            ffmNativeDzTiler = new FfmNativeDzStreamingImageTiler(ffmStreamingTiler, pureJavaConfig)
        }

        when:
        // Warmup
        TilerBenchmarkSupport.warmup(largeImage, WARMUP_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            pureJavaTiler.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), TARGET_ZOOM_LEVEL, TARGET_ZOOM_LEVEL)
            null
        }

        // Verify all tilers actually generate tiles
        println "[VERIFY] --- Verifying all implementations generate tiles ---"
        verifyTilerWorks(largeImage, pureJavaTiler, "Pure Java (ImageTiler5)")
        verifyTilerWorks(largeImage, jnaStreamingTiler, "JNA Streaming")
        if (jnaNativeDzTiler != null) {
            verifyTilerWorks(largeImage, jnaNativeDzTiler, "JNA Native-DZ")
        }
        verifyTilerWorks(largeImage, ffmStreamingTiler, "FFM Streaming")
        if (ffmNativeDzTiler != null) {
            verifyTilerWorks(largeImage, ffmNativeDzTiler, "FFM Native-DZ")
        }
        println "[VERIFY] --- All implementations verified ---"

        // Benchmark each tiler for the single level
        def pureJavaResult = benchmarkSingleLevel(
            largeImage,
            pureJavaTiler,
            "Pure Java (ImageTiler5)",
            MEASURE_ROUNDS
        )

        def jnaStreamingResult = benchmarkSingleLevel(
            largeImage,
            jnaStreamingTiler,
            "JNA Streaming",
            MEASURE_ROUNDS
        )

        def jnaNativeDzResult = null
        if (jnaNativeDzTiler != null) {
            jnaNativeDzResult = benchmarkSingleLevel(
                largeImage,
                jnaNativeDzTiler,
                "JNA Native-DZ",
                MEASURE_ROUNDS
            )
        }

        def ffmStreamingResult = benchmarkSingleLevel(
            largeImage,
            ffmStreamingTiler,
            "FFM Streaming",
            MEASURE_ROUNDS
        )

        def ffmNativeDzResult = null
        if (ffmNativeDzTiler != null) {
            ffmNativeDzResult = benchmarkSingleLevel(
                largeImage,
                ffmNativeDzTiler,
                "FFM Native-DZ",
                MEASURE_ROUNDS
            )
        }

        List<TilerBenchmarkSupport.VariantResult> results = [
            pureJavaResult,
            jnaStreamingResult,
            ffmStreamingResult
        ]

        if (jnaNativeDzResult != null) {
            results.add(2, jnaNativeDzResult)  // Insert between JNA variants
        }
        if (ffmNativeDzResult != null) {
            results.add(ffmNativeDzResult)     // Add at end after FFM streaming
        }

        TilerBenchmarkSupport.printReport(
            "Single-Level (Z${TARGET_ZOOM_LEVEL}) Tiler Comparison",
            "LARGE (${largeImage.name})",
            WARMUP_ROUNDS,
            MEASURE_ROUNDS,
            results
        )

        println "[DEBUG] All implementations tiled single level ${TARGET_ZOOM_LEVEL} successfully"

        then:
        pureJavaResult.timing.meanMs > 0
    }

    // ── Helper methods ──────────────────────────────────────────────────────────

    private TilerBenchmarkSupport.VariantResult benchmarkSingleLevel(
            File imageFile,
            IImageTiler tiler,
            String label,
            int rounds
    ) {
        def timing = TilerBenchmarkSupport.benchmark(imageFile, rounds) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            def result = tiler.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), TARGET_ZOOM_LEVEL, TARGET_ZOOM_LEVEL)
            if (!result.success) {
                throw new IllegalStateException("${label} failed during benchmark run: ${result}")
            }
            null
        }
        return new TilerBenchmarkSupport.VariantResult(label, timing)
    }

    private void verifyTilerWorks(File imageFile, IImageTiler tiler, String label) {
        imageFile.withInputStream { InputStream stream ->
            def (sink, tileCount, byteCount) = buildVerifyingTilerSinkWithCounters()
            def result = tiler.tileImage(stream, sink, TARGET_ZOOM_LEVEL, TARGET_ZOOM_LEVEL)
            if (!result.success) {
                throw new IllegalStateException("[VERIFY] ${label}: FAILED - ${result}")
            }
            if (tileCount[0] <= 0 || byteCount[0] <= 0L) {
                throw new IllegalStateException("[VERIFY] ${label}: FAILED - no tile output generated (tiles=${tileCount[0]}, bytes=${byteCount[0]})")
            }
            println "[VERIFY] ${label}: SUCCESS - ${result.zoomLevels} zoom levels, ${tileCount[0]} tiles, ${byteCount[0]} bytes"
        }
    }

    private List buildVerifyingTilerSinkWithCounters() {
        def tileCount = [0]
        def byteCount = [0L]

        def sink = new TilerSink() {
            @Override
            TilerSink.LevelSink getLevelSink(int level) {
                return new TilerSink.LevelSink() {
                    @Override
                    TilerSink.ColumnSink getColumnSink(int col, int stripIndex, int maxColsPerStrip) {
                        return new TilerSink.ColumnSink() {
                            @Override
                            ByteSink getTileSink(int row) {
                                tileCount[0]++
                                return new ByteSink() {
                                    @Override
                                    OutputStream openStream() {
                                        return new OutputStream() {
                                            @Override
                                            void write(int b) {
                                                byteCount[0]++
                                            }

                                            @Override
                                            void write(byte[] b, int off, int len) {
                                                byteCount[0] += len
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return [sink, tileCount, byteCount]
    }

    private static ImageTilerConfig buildImageTilerConfig(Executor sameThread) {
        return new ImageTilerConfig(
            sameThread,
            sameThread,
            TILE_SIZE,
            6,
            TileFormat.JPEG,
            null,
            VIPS_CONCURRENCY
        )
    }
}




























