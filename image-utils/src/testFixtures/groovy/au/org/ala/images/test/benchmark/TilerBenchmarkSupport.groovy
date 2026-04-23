package au.org.ala.images.test.benchmark

import au.org.ala.images.tiling.TilerSink
import com.google.common.io.ByteSink
import groovy.transform.CompileStatic

@CompileStatic
class TilerBenchmarkSupport {

    static final int DEFAULT_WARMUP_ROUNDS = Integer.getInteger("tiler.bench.warmupRounds", 0)
    static final int DEFAULT_MEASURE_ROUNDS = Integer.getInteger("tiler.bench.measureRounds", 1)

    static final class Timing {
        final long meanMs
        final long stdevMs

        Timing(long meanMs, long stdevMs) {
            this.meanMs = meanMs
            this.stdevMs = stdevMs
        }
    }

    static final class VariantResult {
        final String label
        final Timing timing

        VariantResult(String label, Timing timing) {
            this.label = label
            this.timing = timing
        }
    }

    static final class BenchmarkImages {
        final File mediumImage
        final File largeImage

        BenchmarkImages(File mediumImage, File largeImage) {
            this.mediumImage = mediumImage
            this.largeImage = largeImage
        }
    }

    @CompileStatic
    static TilerSink nullTilerSink() {
        ByteSink devNull = new ByteSink() {
            @Override
            OutputStream openStream() {
                return OutputStream.nullOutputStream()
            }
        }

        TilerSink.ColumnSink colSink = (int row) -> devNull
        TilerSink.LevelSink lvlSink = (int col, int stripIndex, int maxColumnsPerStrip) -> colSink
        return (int level) -> lvlSink
    }

    @CompileStatic
    static BenchmarkImages resolveDefaultImages() {
        List<File> roots = [
            new File("../image-utils/src/testFixtures/resources/images"),
            new File("image-utils/src/testFixtures/resources/images")
        ]
        File imagesDir = roots.find { it.isDirectory() } as File
        if (imagesDir == null) {
            throw new IllegalStateException("Cannot locate testFixtures images directory")
        }
        return new BenchmarkImages(
            new File(imagesDir, "Bearded_Heath.jpg"),
            new File(imagesDir, "large_test_10000x10000.jpg")
        )
    }

    @CompileStatic
    static void warmup(File imageFile, int warmupRounds, Closure<Void> tileAction) {
        for (int i = 0; i < warmupRounds; i++) {
            imageFile.withInputStream { InputStream stream ->
                tileAction.call(stream)
            }
        }
    }

    @CompileStatic
    static Timing benchmark(File imageFile, int rounds, Closure<Void> tileAction) {
        List<Long> timingsNanos = []
        for (int i = 0; i < rounds; i++) {
            imageFile.withInputStream { InputStream stream ->
                long t0 = System.nanoTime()
                tileAction.call(stream)
                long t1 = System.nanoTime()
                timingsNanos << (t1 - t0)
            }
        }

        long total = 0L
        for (long t : timingsNanos) {
            total += t
        }
        long sampleCount = (long) timingsNanos.size()
        long mean = Math.floorDiv(total, sampleCount)

        long varianceTotal = 0L
        for (long t : timingsNanos) {
            long delta = t - mean
            varianceTotal += (delta * delta)
        }
        long variance = Math.floorDiv(varianceTotal, sampleCount)
        long stdev = (long) Math.sqrt((double) variance)
        long meanMs = (long) (mean / 1_000_000L)
        long stdevMs = (long) (stdev / 1_000_000L)
        return new Timing(meanMs, stdevMs)
    }

    @CompileStatic
    static void printReport(String benchmarkName, String imageLabel, int warmups, int rounds, List<VariantResult> results) {
        long maxMs = results.collect { VariantResult it -> it.timing.meanMs }.max() as long

        println ""
        println "======================================================================"
        println "BENCHMARK: ${benchmarkName} -- ${imageLabel}"
        println "rounds: ${rounds} measured + ${warmups} warmup"
        println "----------------------------------------------------------------------"
        println String.format("%-30s | %9s | %7s | %8s | %s",
            "Variant", "Mean (ms)", "+/- ms", "vs base", "Bar")
        println "${'-' * 30}-+-${'-' * 9}-+-${'-' * 7}-+-${'-' * 8}-+-${'-' * 41}"

        VariantResult baseline = results[0]
        for (VariantResult variant : results) {
            String speedup = "N/A"
            if (variant.timing.meanMs > 0L && baseline.timing.meanMs > 0L) {
                speedup = String.format("%.2fx", baseline.timing.meanMs / (double) variant.timing.meanMs)
            }
            println String.format("%-30s | %9d | %7d | %8s | %s",
                variant.label,
                variant.timing.meanMs,
                variant.timing.stdevMs,
                speedup,
                makeBar(variant.timing.meanMs, maxMs))
        }

        println "${'-' * 30}-+-${'-' * 9}-+-${'-' * 7}-+-${'-' * 8}-+-${'-' * 41}"
        for (int i = 1; i < results.size(); i++) {
            VariantResult variant = results[i]
            if (variant.timing.meanMs > baseline.timing.meanMs * 1.1d) {
                println "ANOMALY: ${variant.label} slower than baseline by >10%"
            }
            if (variant.timing.meanMs < baseline.timing.meanMs * 0.7d) {
                println "NOTABLE: ${variant.label} faster than baseline by >30%"
            }
        }
        println "======================================================================"
        println ""
    }

    @CompileStatic
    private static String makeBar(long ms, long maxMs) {
        int len = maxMs > 0 ? (int) Math.round((double) (40.0d * ms / maxMs)) : 0
        return "#" * Math.max(1, len)
    }
}







