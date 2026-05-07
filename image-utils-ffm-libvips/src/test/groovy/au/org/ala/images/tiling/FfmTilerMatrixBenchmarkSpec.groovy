package au.org.ala.images.tiling

import au.org.ala.images.ffm.NativeDzTilerBridgeLoaderFFM
import au.org.ala.images.ffm.NativeLibraryDetectorFFM
import au.org.ala.images.test.benchmark.TilerBenchmarkSupport
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Matrix benchmark comparing FFM streaming tiler vs FFM native dz tiler
 * across multiple executor configurations.
 */
class FfmTilerMatrixBenchmarkSpec extends Specification {

    static final int WARMUP_ROUNDS = Integer.getInteger("tiler.bench.warmupRounds", 0)
    static final int MEASURE_ROUNDS = Integer.getInteger("tiler.bench.measureRounds", 1)
    static final int TILE_SIZE = 256
    static final int FIXED_POOL_SIZE = Math.max(2, Runtime.getRuntime().availableProcessors())
    static final int VIPS_CONCURRENCY = Integer.getInteger("tiler.bench.vips.concurrency", 1)
    static final String IMAGE_MODE = System.getProperty("tiler.bench.image", "all").toLowerCase(Locale.ROOT)

    @Shared File mediumImage
    @Shared File largeImage

    def setupSpec() {
        def images = TilerBenchmarkSupport.resolveDefaultImages()
        mediumImage = images.mediumImage
        largeImage = images.largeImage
    }

    def "benchmark FFM tiler matrix on medium image"() {
        given:
        if (IMAGE_MODE == "large") {
            println "[BENCHMARK] Skipping FFM matrix medium benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            println "[BENCHMARK] Skipping FFM matrix benchmark - libvips not available"
            return
        }
        if (!mediumImage?.exists()) {
            println "[BENCHMARK] Skipping FFM matrix medium benchmark - image not found: ${mediumImage}"
            return
        }

        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = Executors.newVirtualThreadPerTaskExecutor()

        // Streaming tiler variants
        def streamingSeq = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def streamingPool = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def streamingVt = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))

        // Native-dz tiler variants (only if bridge available)
        def nativeDzSeq = null
        def nativeDzPool = null
        def nativeDzVt = null
        if (NativeDzTilerBridgeLoaderFFM.isAvailable()) {
            nativeDzSeq = new FfmNativeDzStreamingImageTiler(streamingSeq, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
            nativeDzPool = new FfmNativeDzStreamingImageTiler(streamingPool, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
            nativeDzVt = new FfmNativeDzStreamingImageTiler(streamingVt, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        }

        when:
        TilerBenchmarkSupport.warmup(mediumImage, WARMUP_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingSeq.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def streamSeq = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingSeq.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def streamPool = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingPool.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def streamVt = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingVt.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        List<TilerBenchmarkSupport.VariantResult> results = [
            new TilerBenchmarkSupport.VariantResult("FFM Streaming [Sequential]", streamSeq),
            new TilerBenchmarkSupport.VariantResult("FFM Streaming [Fixed pool]", streamPool),
            new TilerBenchmarkSupport.VariantResult("FFM Streaming [Virtual threads]", streamVt)
        ]

        if (nativeDzSeq != null) {
            def ndzSeq = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
                def buffered = new BufferedInputStream(stream)
                buffered.mark(Integer.MAX_VALUE)
                nativeDzSeq.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            def ndzPool = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
                def buffered = new BufferedInputStream(stream)
                buffered.mark(Integer.MAX_VALUE)
                nativeDzPool.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            def ndzVt = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
                def buffered = new BufferedInputStream(stream)
                buffered.mark(Integer.MAX_VALUE)
                nativeDzVt.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            results.addAll([
                new TilerBenchmarkSupport.VariantResult("FFM NativeDz [Sequential]", ndzSeq),
                new TilerBenchmarkSupport.VariantResult("FFM NativeDz [Fixed pool]", ndzPool),
                new TilerBenchmarkSupport.VariantResult("FFM NativeDz [Virtual threads]", ndzVt)
            ])
        }

        TilerBenchmarkSupport.printReport(
                "FFM Tiler Matrix",
                "MEDIUM (${mediumImage.name})",
                WARMUP_ROUNDS,
                MEASURE_ROUNDS,
                results
        )

        then:
        streamSeq.meanMs > 0

        cleanup:
        fixedPool?.shutdown()
        virtualPool?.shutdown()
    }

    def "benchmark FFM tiler matrix on large image"() {
        given:
        if (IMAGE_MODE == "medium") {
            println "[BENCHMARK] Skipping FFM matrix large benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            println "[BENCHMARK] Skipping FFM matrix benchmark - libvips not available"
            return
        }
        if (!largeImage?.exists()) {
            println "[BENCHMARK] Skipping FFM matrix large benchmark - image not found: ${largeImage}"
            return
        }

        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = Executors.newVirtualThreadPerTaskExecutor()

        // Streaming tiler variants
        def streamingSeq = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def streamingPool = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def streamingVt = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))

        // Native-dz tiler variants (only if bridge available)
        def nativeDzSeq = null
        def nativeDzPool = null
        def nativeDzVt = null
        if (NativeDzTilerBridgeLoaderFFM.isAvailable()) {
            nativeDzSeq = new FfmNativeDzStreamingImageTiler(streamingSeq, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
            nativeDzPool = new FfmNativeDzStreamingImageTiler(streamingPool, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
            nativeDzVt = new FfmNativeDzStreamingImageTiler(streamingVt, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        }

        when:
        TilerBenchmarkSupport.warmup(largeImage, WARMUP_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingSeq.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def streamSeq = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingSeq.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def streamPool = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingPool.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def streamVt = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            streamingVt.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        List<TilerBenchmarkSupport.VariantResult> results = [
            new TilerBenchmarkSupport.VariantResult("FFM Streaming [Sequential]", streamSeq),
            new TilerBenchmarkSupport.VariantResult("FFM Streaming [Fixed pool]", streamPool),
            new TilerBenchmarkSupport.VariantResult("FFM Streaming [Virtual threads]", streamVt)
        ]

        if (nativeDzSeq != null) {
            def ndzSeq = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
                def buffered = new BufferedInputStream(stream)
                buffered.mark(Integer.MAX_VALUE)
                nativeDzSeq.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            def ndzPool = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
                def buffered = new BufferedInputStream(stream)
                buffered.mark(Integer.MAX_VALUE)
                nativeDzPool.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            def ndzVt = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
                def buffered = new BufferedInputStream(stream)
                buffered.mark(Integer.MAX_VALUE)
                nativeDzVt.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            results.addAll([
                new TilerBenchmarkSupport.VariantResult("FFM NativeDz [Sequential]", ndzSeq),
                new TilerBenchmarkSupport.VariantResult("FFM NativeDz [Fixed pool]", ndzPool),
                new TilerBenchmarkSupport.VariantResult("FFM NativeDz [Virtual threads]", ndzVt)
            ])
        }

        TilerBenchmarkSupport.printReport(
                "FFM Tiler Matrix",
                "LARGE (${largeImage.name})",
                WARMUP_ROUNDS,
                MEASURE_ROUNDS,
                results
        )

        then:
        streamSeq.meanMs > 0

        cleanup:
        fixedPool?.shutdown()
        virtualPool?.shutdown()
    }
}


