package au.org.ala.images.tiling

import au.org.ala.images.ffm.NativeDzTilerBridgeLoaderFFM
import au.org.ala.images.ffm.NativeLibraryDetectorFFM
import au.org.ala.images.test.benchmark.TilerBenchmarkSupport
import spock.lang.Shared
import spock.lang.Specification

import java.util.Locale
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class FfmNativeDzStreamingImageTilerBenchmarkSpec extends Specification {

    static final int WARMUP_ROUNDS = TilerBenchmarkSupport.DEFAULT_WARMUP_ROUNDS
    static final int MEASURE_ROUNDS = TilerBenchmarkSupport.DEFAULT_MEASURE_ROUNDS
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

    def "benchmark FFM native dz tiler variants on medium image"() {
        given:
        if (IMAGE_MODE == "large") {
            println "[BENCHMARK] Skipping FFM native-dz medium benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            println "[BENCHMARK] Skipping FFM native-dz benchmark - libvips not available"
            return
        }
        if (!NativeDzTilerBridgeLoaderFFM.isAvailable()) {
            println "[BENCHMARK] Skipping FFM native-dz benchmark - native bridge not available"
            return
        }
        if (!mediumImage?.exists()) {
            println "[BENCHMARK] Skipping FFM native-dz medium benchmark - image not found: ${mediumImage}"
            return
        }

        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = Executors.newVirtualThreadPerTaskExecutor()

        // Use FFM streaming tilers as fallback chain to keep benchmark resilient if native path fails.
        def fallbackSequential = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fallbackFixed = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fallbackVirtual = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))

        def sequential = new FfmNativeDzStreamingImageTiler(fallbackSequential, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fixedPoolTiler = new FfmNativeDzStreamingImageTiler(fallbackFixed, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def virtualThreadTiler = new FfmNativeDzStreamingImageTiler(fallbackVirtual, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))

        when:
        TilerBenchmarkSupport.warmup(mediumImage, WARMUP_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            sequential.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def seq = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            sequential.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def pool = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            fixedPoolTiler.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def vt = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            virtualThreadTiler.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        TilerBenchmarkSupport.printReport(
                "FfmNativeDzStreamingImageTiler",
                "MEDIUM (${mediumImage.name})",
                WARMUP_ROUNDS,
                MEASURE_ROUNDS,
                [
                        new TilerBenchmarkSupport.VariantResult("Sequential (same-thread)", seq),
                        new TilerBenchmarkSupport.VariantResult("Fixed pool (${FIXED_POOL_SIZE} threads)", pool),
                        new TilerBenchmarkSupport.VariantResult("Virtual threads", vt)
                ]
        )

        then:
        seq.meanMs > 0

        cleanup:
        fixedPool?.shutdown()
        virtualPool?.shutdown()
    }

    def "benchmark FFM native dz tiler variants on large image"() {
        given:
        if (IMAGE_MODE == "medium") {
            println "[BENCHMARK] Skipping FFM native-dz large benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!NativeLibraryDetectorFFM.isVipsAvailable()) {
            println "[BENCHMARK] Skipping FFM native-dz benchmark - libvips not available"
            return
        }
        if (!NativeDzTilerBridgeLoaderFFM.isAvailable()) {
            println "[BENCHMARK] Skipping FFM native-dz benchmark - native bridge not available"
            return
        }
        if (!largeImage?.exists()) {
            println "[BENCHMARK] Skipping FFM native-dz large benchmark - image not found: ${largeImage}"
            return
        }

        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = Executors.newVirtualThreadPerTaskExecutor()

        def fallbackSequential = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fallbackFixed = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fallbackVirtual = new FfmStreamingImageTiler(null, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))

        def sequential = new FfmNativeDzStreamingImageTiler(fallbackSequential, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fixedPoolTiler = new FfmNativeDzStreamingImageTiler(fallbackFixed, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def virtualThreadTiler = new FfmNativeDzStreamingImageTiler(fallbackVirtual, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))

        when:
        TilerBenchmarkSupport.warmup(largeImage, WARMUP_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            sequential.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def seq = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            sequential.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def pool = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            fixedPoolTiler.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def vt = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            def buffered = new BufferedInputStream(stream)
            buffered.mark(Integer.MAX_VALUE)
            virtualThreadTiler.tileImage(buffered, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        TilerBenchmarkSupport.printReport(
                "FfmNativeDzStreamingImageTiler",
                "LARGE (${largeImage.name})",
                WARMUP_ROUNDS,
                MEASURE_ROUNDS,
                [
                        new TilerBenchmarkSupport.VariantResult("Sequential (same-thread)", seq),
                        new TilerBenchmarkSupport.VariantResult("Fixed pool (${FIXED_POOL_SIZE} threads)", pool),
                        new TilerBenchmarkSupport.VariantResult("Virtual threads", vt)
                ]
        )

        then:
        seq.meanMs > 0

        cleanup:
        fixedPool?.shutdown()
        virtualPool?.shutdown()
    }
}

