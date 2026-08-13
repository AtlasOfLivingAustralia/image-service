package au.org.ala.images.vipsffm
import app.photofox.vipsffm.Vips
import au.org.ala.images.test.benchmark.TilerBenchmarkSupport
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TileFormat
import spock.lang.Shared
import spock.lang.Specification

import java.util.Locale
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
class VipsFfmTilerBenchmarkSpec extends Specification {
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
        try {
            Vips.init()
        } catch (Throwable ignored) {
        }
    }
    private static IImageTiler makeTiler(Executor ioExecutor, int vipsConcurrency = 0) {
        def config = new ImageTilerConfig(ioExecutor, null, TILE_SIZE, 6, TileFormat.JPEG, null, vipsConcurrency)
        return new VipsFfmStreamingImageTiler(null, config)
    }
    def "benchmark photofox tiler variants on medium image"() {
        given:
        if (IMAGE_MODE == "large") {
            println "[BENCHMARK] Skipping photofox medium benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!mediumImage?.exists()) {
            println "[BENCHMARK] Skipping photofox medium benchmark - image not found: ${mediumImage}"
            return
        }
        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = Executors.newVirtualThreadPerTaskExecutor()
        def sequential = makeTiler(sameThread, VIPS_CONCURRENCY)
        def fixedPoolTiler = makeTiler(fixedPool, VIPS_CONCURRENCY)
        def virtualThreadTiler = makeTiler(virtualPool, VIPS_CONCURRENCY)
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
        def vt = TilerBenchmarkSupport.benchmark(mediumImage, MEASURE_ROUNDS) { InputStream stream ->
            virtualThreadTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        TilerBenchmarkSupport.printReport(
            "VipsFfmStreamingImageTiler",
            "MEDIUM (${mediumImage.name})",
            WARMUP_ROUNDS,
            MEASURE_ROUNDS,
            [
                new TilerBenchmarkSupport.VariantResult("Sequential", seq),
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
    def "benchmark photofox tiler variants on large image"() {
        given:
        if (IMAGE_MODE == "medium") {
            println "[BENCHMARK] Skipping photofox large benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!largeImage?.exists()) {
            println "[BENCHMARK] Skipping photofox large benchmark - image not found: ${largeImage}"
            return
        }
        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = Executors.newVirtualThreadPerTaskExecutor()
        def sequential = makeTiler(sameThread, VIPS_CONCURRENCY)
        def fixedPoolTiler = makeTiler(fixedPool, VIPS_CONCURRENCY)
        def virtualThreadTiler = makeTiler(virtualPool, VIPS_CONCURRENCY)
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
        def vt = TilerBenchmarkSupport.benchmark(largeImage, MEASURE_ROUNDS) { InputStream stream ->
            virtualThreadTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        TilerBenchmarkSupport.printReport(
            "VipsFfmStreamingImageTiler",
            "LARGE (${largeImage.name})",
            WARMUP_ROUNDS,
            MEASURE_ROUNDS,
            [
                new TilerBenchmarkSupport.VariantResult("Sequential", seq),
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

