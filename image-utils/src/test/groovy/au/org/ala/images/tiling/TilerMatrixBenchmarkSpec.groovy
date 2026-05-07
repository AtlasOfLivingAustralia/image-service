package au.org.ala.images.tiling

import au.org.ala.images.jna.NativeDzTilerBridgeLoader
import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.optimisation.ProcessCommandExecutor
import au.org.ala.images.test.benchmark.TilerBenchmarkSupport
import spock.lang.Shared
import spock.lang.Specification

import java.util.Locale
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Cross-implementation benchmark matrix for image-utils tilers.
 *
 * Includes:
 * - JnaStreamingImageTiler (full pyramid)
 * - ImageTiler5 (pure Java, full pyramid)
 * - VipsCliOnDemandImageTiler (single representative tile)
 * - MagickCliOnDemandImageTiler (single representative tile)
 *
 * NOTE: StreamingImageTiler (vips CLI full-pyramid) has been removed from the fallback chain
 * and from this matrix.  The fallback path is now JNA(10) → Pure Java(0).  The CLI tiler
 * remains in the codebase for explicit opt-in use but is not auto-discovered via SPI.
 */
class TilerMatrixBenchmarkSpec extends Specification {

    static final int WARMUP_ROUNDS = TilerBenchmarkSupport.DEFAULT_WARMUP_ROUNDS
    static final int MEASURE_ROUNDS = TilerBenchmarkSupport.DEFAULT_MEASURE_ROUNDS
    static final int TILE_SIZE = 256
    static final int FIXED_POOL_SIZE = Math.max(2, Runtime.getRuntime().availableProcessors())
    static final int VIPS_CONCURRENCY = Integer.getInteger("tiler.bench.vips.concurrency", 1)
    static final String IMAGE_MODE = System.getProperty("tiler.bench.image", "all").toLowerCase(Locale.ROOT)

    @Shared File mediumImage
    @Shared File largeImage

    static class TileRequest {
        final int level
        final int x
        final int y

        TileRequest(int level, int x, int y) {
            this.level = level
            this.x = x
            this.y = y
        }
    }

    def setupSpec() {
        def images = TilerBenchmarkSupport.resolveDefaultImages()
        mediumImage = images.mediumImage
        largeImage = images.largeImage
    }

    def "benchmark matrix on medium image"() {
        given:
        if (IMAGE_MODE == "large") {
            println "[BENCHMARK] Skipping matrix medium benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!mediumImage?.exists()) {
            println "[BENCHMARK] Skipping matrix medium benchmark - image not found: ${mediumImage}"
            return
        }

        runMatrixForImage(mediumImage, "MEDIUM")

        expect:
        true
    }

    def "benchmark matrix on large image"() {
        given:
        if (IMAGE_MODE == "medium") {
            println "[BENCHMARK] Skipping matrix large benchmark due to tiler.bench.image=${IMAGE_MODE}"
            return
        }
        if (!largeImage?.exists()) {
            println "[BENCHMARK] Skipping matrix large benchmark - image not found: ${largeImage}"
            return
        }

        runMatrixForImage(largeImage, "LARGE")

        expect:
        true
    }

    private void runMatrixForImage(File image, String label) {
        Executor sameThread = Runnable::run
        ExecutorService fixedPool = Executors.newFixedThreadPool(FIXED_POOL_SIZE)
        ExecutorService virtualPool = maybeVirtualThreadExecutor()

        try {
            benchmarkJnaStreaming(image, label, sameThread, fixedPool, virtualPool)
            benchmarkNativeDzStreaming(image, label, sameThread, fixedPool, virtualPool)
            benchmarkImageTiler5(image, label, sameThread, fixedPool, virtualPool)
            benchmarkCliOnDemand(image, label)
        } finally {
            fixedPool?.shutdown()
            virtualPool?.shutdown()
        }
    }

    private void benchmarkJnaStreaming(File image, String label, Executor sameThread, ExecutorService fixedPool, ExecutorService virtualPool) {
        if (!NativeLibraryDetector.isVipsAvailable()) {
            println "[BENCHMARK] Skipping JnaStreamingImageTiler - libvips not available"
            return
        }

        def sequential = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, sameThread, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fixedPoolTiler = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def virtualThreadTiler = virtualPool != null ?
            new JnaStreamingImageTiler(null, new ImageTilerConfig(null, virtualPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY)) : null

        TilerBenchmarkSupport.warmup(image, WARMUP_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def seq = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def pool = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
            fixedPoolTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def results = [
            new TilerBenchmarkSupport.VariantResult("Sequential (same-thread)", seq),
            new TilerBenchmarkSupport.VariantResult("Fixed pool (${FIXED_POOL_SIZE} threads)", pool)
        ]

        if (virtualThreadTiler != null) {
            def vt = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
                virtualThreadTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            results << new TilerBenchmarkSupport.VariantResult("Virtual threads", vt)
        } else {
            println "[BENCHMARK] JnaStreamingImageTiler: virtual threads unavailable on this runtime"
        }

        TilerBenchmarkSupport.printReport(
            "JnaStreamingImageTiler [full pyramid]",
            "${label} (${image.name}), vipsConcurrency=${VIPS_CONCURRENCY}",
            WARMUP_ROUNDS,
            MEASURE_ROUNDS,
            results
        )
    }

    private void benchmarkNativeDzStreaming(File image, String label, Executor sameThread, ExecutorService fixedPool, ExecutorService virtualPool) {
        if (!NativeLibraryDetector.isVipsAvailable()) {
            println "[BENCHMARK] Skipping NativeDzStreamingImageTiler - libvips not available"
            return
        }
        if (!NativeDzTilerBridgeLoader.isAvailable()) {
            println "[BENCHMARK] Skipping NativeDzStreamingImageTiler - native bridge not available"
            return
        }

        Executor sameThreadExec = Runnable::run
        def fallbackJna = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, sameThreadExec, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def sequential = new NativeDzStreamingImageTiler(fallbackJna, new ImageTilerConfig(null, sameThreadExec, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))
        def fixedFallback = new JnaStreamingImageTiler(null, new ImageTilerConfig(null, fixedPool, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY))

        TilerBenchmarkSupport.warmup(image, WARMUP_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def seq = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def results = [
            new TilerBenchmarkSupport.VariantResult("Sequential (same-thread)", seq)
        ]

        TilerBenchmarkSupport.printReport(
            "NativeDzStreamingImageTiler [full pyramid]",
            "${label} (${image.name}), vipsConcurrency=${VIPS_CONCURRENCY}",
            WARMUP_ROUNDS,
            MEASURE_ROUNDS,
            results
        )
    }

    private void benchmarkImageTiler5(File image, String label, Executor sameThread, ExecutorService fixedPool, ExecutorService virtualPool) {
        def sequential = new ImageTiler5(new ImageTilerConfig(sameThread, sameThread, TILE_SIZE, 6, TileFormat.JPEG))
        def fixedPoolTiler = new ImageTiler5(new ImageTilerConfig(fixedPool, fixedPool, TILE_SIZE, 6, TileFormat.JPEG))
        def virtualThreadTiler = virtualPool != null ?
            new ImageTiler5(new ImageTilerConfig(virtualPool, virtualPool, TILE_SIZE, 6, TileFormat.JPEG)) : null

        TilerBenchmarkSupport.warmup(image, WARMUP_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def seq = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
            sequential.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }
        def pool = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
            fixedPoolTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
            null
        }

        def results = [
            new TilerBenchmarkSupport.VariantResult("Sequential (same-thread)", seq),
            new TilerBenchmarkSupport.VariantResult("Fixed pool (${FIXED_POOL_SIZE} threads)", pool)
        ]

        if (virtualThreadTiler != null) {
            def vt = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
                virtualThreadTiler.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }
            results << new TilerBenchmarkSupport.VariantResult("Virtual threads", vt)
        } else {
            println "[BENCHMARK] ImageTiler5: virtual threads unavailable on this runtime"
        }

        TilerBenchmarkSupport.printReport(
            "ImageTiler5 (pure Java) [full pyramid]",
            "${label} (${image.name})",
            WARMUP_ROUNDS,
            MEASURE_ROUNDS,
            results
        )
    }

    private void benchmarkStreamingImageTiler(File image, String label) {
        def commandExecutor = new ProcessCommandExecutor()
        if (!commandExecutor.isInstalled("vips")) {
            println "[BENCHMARK] Skipping StreamingImageTiler - command not found: vips"
            return
        }

        def config = new ImageTilerConfig(null, null, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY)
        def streaming = new StreamingImageTiler(commandExecutor, "vips", config)

        try {
            TilerBenchmarkSupport.warmup(image, WARMUP_ROUNDS) { InputStream stream ->
                streaming.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }

            def timing = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
                streaming.tileImage(stream, TilerBenchmarkSupport.nullTilerSink(), 0, Integer.MAX_VALUE)
                null
            }

            TilerBenchmarkSupport.printReport(
                "StreamingImageTiler [full pyramid]",
                "${label} (${image.name}), vipsConcurrency=${VIPS_CONCURRENCY}",
                WARMUP_ROUNDS,
                MEASURE_ROUNDS,
                [new TilerBenchmarkSupport.VariantResult("vips cli stream", timing)]
            )
        } catch (Throwable t) {
            println "[BENCHMARK] Skipping StreamingImageTiler - failed to run: ${t.class.simpleName}: ${t.message}"
        }
    }

    private void benchmarkCliOnDemand(File image, String label) {
        def commandExecutor = new ProcessCommandExecutor()
        def config = new ImageTilerConfig(null, null, TILE_SIZE, 6, TileFormat.JPEG, null, VIPS_CONCURRENCY)
        def fallback = new OnDemandImageTiler(config)
        TileRequest request = representativeTile(image)

        println "[BENCHMARK] CLI representative tile: level=${request.level}, x=${request.x}, y=${request.y} (${label})"

        if (commandExecutor.isInstalled("vips")) {
            def vipsCli = new VipsCliOnDemandImageTiler(commandExecutor, "vips", config, fallback)
            def vipsTiming = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
                def result = vipsCli.generateTile(stream, TilerBenchmarkSupport.nullTilerSink(), request.level, request.x, request.y)
                if (!result.success) {
                    throw new IOException("VipsCliOnDemandImageTiler failed: " + result.message)
                }
                null
            }
            TilerBenchmarkSupport.printReport(
                "VipsCliOnDemandImageTiler [single tile]",
                "${label} (${image.name}), level=${request.level}, x=${request.x}, y=${request.y}",
                WARMUP_ROUNDS,
                MEASURE_ROUNDS,
                [new TilerBenchmarkSupport.VariantResult("vips cli", vipsTiming)]
            )
        } else {
            println "[BENCHMARK] Skipping VipsCliOnDemandImageTiler - command not found: vips"
        }

        if (commandExecutor.isInstalled("magick")) {
            def magickCli = new MagickCliOnDemandImageTiler(commandExecutor, "magick", config, fallback)
            def magickTiming = TilerBenchmarkSupport.benchmark(image, MEASURE_ROUNDS) { InputStream stream ->
                def result = magickCli.generateTile(stream, TilerBenchmarkSupport.nullTilerSink(), request.level, request.x, request.y)
                if (!result.success) {
                    throw new IOException("MagickCliOnDemandImageTiler failed: " + result.message)
                }
                null
            }
            TilerBenchmarkSupport.printReport(
                "MagickCliOnDemandImageTiler [single tile]",
                "${label} (${image.name}), level=${request.level}, x=${request.x}, y=${request.y}",
                WARMUP_ROUNDS,
                MEASURE_ROUNDS,
                [new TilerBenchmarkSupport.VariantResult("magick cli", magickTiming)]
            )
        } else {
            println "[BENCHMARK] Skipping MagickCliOnDemandImageTiler - command not found: magick"
        }
    }

    private TileRequest representativeTile(File imageFile) {
        def info = imageFile.withInputStream { InputStream stream ->
            return new OnDemandImageTiler(new ImageTilerConfig(null, null, TILE_SIZE, 6, TileFormat.JPEG)).getPyramidInfo(stream)
        }
        int level = Math.max(0, info.levels - 1)
        int x = Math.max(0, info.getTilesXForLevel(level).intdiv(2))
        int y = Math.max(0, info.getTilesYForLevel(level).intdiv(2))
        return new TileRequest(level, x, y)
    }

    private static ExecutorService maybeVirtualThreadExecutor() {
        try {
            return Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null) as ExecutorService
        } catch (Throwable ignored) {
            return null
        }
    }
}






