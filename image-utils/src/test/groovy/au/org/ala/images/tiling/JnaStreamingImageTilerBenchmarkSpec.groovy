package au.org.ala.images.tiling
import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.test.benchmark.TilerBenchmarkSupport
import spock.lang.Shared
import spock.lang.Specification

import java.util.Locale
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
class JnaStreamingImageTilerBenchmarkSpec extends Specification {
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
    def "benchmark JNA tiler variants on medium image"() {
        given:
        if (IMAGE_MODE == "large") {
            println "[BENCHMARK] Skipping JNA medium benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!NativeLibraryDetector.isVipsAvailable()) {
            println "[BENCHMARK] Skipping JNA benchmark - libvips not available"
            return
        }
        if (!mediumImage?.exists()) {
            println "[BENCHMARK] Skipping JNA medium benchmark - image not found: ${mediumImage}"
            return
        }
        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = maybeVirtualThreadExecutor()
        def sequential = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fixedPoolTiler = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def virtualThreadTiler = virtualPool != null ?
            new JnaStreamingImageTiler(null, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY)) : null
        when:
        TilerBenchmarkSupport.warmup(mediumImage, WARMUP_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def seq = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def pool = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            fixedPoolTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def results = [
            new TilerBenchmarkSupport.VariantResult("Sequential (same-thread)", seq),
            new TilerBenchmarkSupport.VariantResult("Fixed pool (${FIXED_POOL_SIZE} threads)", pool)
        ]
        if (virtualThreadTiler != null) {
            def vt = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
                virtualThreadTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            results << new TilerBenchmarkSupport.VariantResult("Virtual threads", vt)
        } else {
            println "[BENCHMARK] Virtual threads unavailable on this runtime - skipping that variant"
        }

        TilerBenchmarkSupport.printReport("JnaStreamingImageTiler", "MEDIUM (${mediumImage.name})", WARMUP_ROUNDS, MEASURE_ROUNDS, results)
        then:
        seq.meanMs > 0
        cleanup:
        fixedPool?.shutdown()
        virtualPool?.shutdown()
    }
    def "benchmark JNA tiler variants on large image"() {
        given:
        if (IMAGE_MODE == "medium") {
            println "[BENCHMARK] Skipping JNA large benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!NativeLibraryDetector.isVipsAvailable()) {
            println "[BENCHMARK] Skipping JNA benchmark - libvips not available"
            return
        }
        if (!largeImage?.exists()) {
            println "[BENCHMARK] Skipping JNA large benchmark - image not found: ${largeImage}"
            return
        }
        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = maybeVirtualThreadExecutor()
        def sequential = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fixedPoolTiler = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def virtualThreadTiler = virtualPool != null ?
            new JnaStreamingImageTiler(null, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY)) : null
        when:
        TilerBenchmarkSupport.warmup(largeImage, WARMUP_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def seq = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def pool = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            fixedPoolTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def results = [
            new TilerBenchmarkSupport.VariantResult("Sequential (same-thread)", seq),
            new TilerBenchmarkSupport.VariantResult("Fixed pool (${FIXED_POOL_SIZE} threads)", pool)
        ]
        if (virtualThreadTiler != null) {
            def vt = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
                virtualThreadTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            results << new TilerBenchmarkSupport.VariantResult("Virtual threads", vt)
        } else {
            println "[BENCHMARK] Virtual threads unavailable on this runtime - skipping that variant"
        }

        TilerBenchmarkSupport.printReport("JnaStreamingImageTiler", "LARGE (${largeImage.name})", WARMUP_ROUNDS, MEASURE_ROUNDS, results)
        then:
        seq.meanMs > 0
        cleanup:
        fixedPool?.shutdown()
        virtualPool?.shutdown()
    }

    private static ExecutorService maybeVirtualThreadExecutor() {
        try {
            return Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null) as ExecutorService
        } catch (Throwable ignored) {
            return null
        }
    }
}
