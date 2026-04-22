package au.org.ala.images.test.benchmark

import au.org.ala.images.tiling.TilerSink
import com.google.common.io.ByteSink
import groovy.transform.CompileStatic

import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.util.concurrent.ConcurrentHashMap

@CompileStatic
class TileVisualEquivalenceSupport {

    @CompileStatic
    static final class TileKey {
        final int level
        final int x
        final int y

        TileKey(int level, int x, int y) {
            this.level = level
            this.x = x
            this.y = y
        }

        @Override
        boolean equals(Object other) {
            if (this.is(other)) {
                return true
            }
            if (!(other instanceof TileKey)) {
                return false
            }
            TileKey rhs = (TileKey) other
            return level == rhs.level && x == rhs.x && y == rhs.y
        }

        @Override
        int hashCode() {
            int result = Integer.hashCode(level)
            result = 31 * result + Integer.hashCode(x)
            result = 31 * result + Integer.hashCode(y)
            return result
        }

        @Override
        String toString() {
            return "${level}/${x}/${y}".toString()
        }
    }

    @CompileStatic
    static final class CapturedTiles {
        final Map<TileKey, byte[]> tiles = new ConcurrentHashMap<>()

        TilerSink sink() {
            TilerSink.ColumnSink columnSink = { int row ->
                TileKey key = CURRENT_TILE.get()
                return new ByteSink() {
                    @Override
                    OutputStream openStream() {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream()
                        return new FilterOutputStream(baos) {
                            @Override
                            void close() throws IOException {
                                super.close()
                                tiles.put(key, baos.toByteArray())
                            }
                        }
                    }
                }
            } as TilerSink.ColumnSink

            return { int level ->
                return { int col, int stripIndex, int maxColumnsPerStrip ->
                    return { int row ->
                        CURRENT_TILE.set(new TileKey(level, col + (stripIndex * maxColumnsPerStrip), row))
                        return columnSink.getTileSink(row)
                    } as TilerSink.ColumnSink
                } as TilerSink.LevelSink
            } as TilerSink
        }
    }

    @CompileStatic
    static final class TileDifference {
        final TileKey key
        final double rmse
        final int maxChannelDelta
        final String reason

        TileDifference(TileKey key, double rmse, int maxChannelDelta, String reason) {
            this.key = key
            this.rmse = rmse
            this.maxChannelDelta = maxChannelDelta
            this.reason = reason
        }
    }

    @CompileStatic
    static final class ComparisonReport {
        int referenceTileCount
        int candidateTileCount
        int missingInCandidate
        int extraInCandidate
        final List<TileDifference> mismatches = []

        boolean equivalent() {
            return missingInCandidate == 0 && extraInCandidate == 0 && mismatches.isEmpty()
        }
    }

    private static final ThreadLocal<TileKey> CURRENT_TILE = new ThreadLocal<>()

    static CapturedTiles captureTiles() {
        return new CapturedTiles()
    }

    static ComparisonReport compare(CapturedTiles reference,
                                    CapturedTiles candidate,
                                    double maxRmse,
                                    int maxChannelDelta,
                                    int maxReportedMismatches) {
        ComparisonReport report = new ComparisonReport()
        report.referenceTileCount = reference.tiles.size()
        report.candidateTileCount = candidate.tiles.size()

        Set<TileKey> referenceKeys = new HashSet<>(reference.tiles.keySet())
        Set<TileKey> candidateKeys = new HashSet<>(candidate.tiles.keySet())

        Set<TileKey> missing = new HashSet<>(referenceKeys)
        missing.removeAll(candidateKeys)
        report.missingInCandidate = missing.size()

        Set<TileKey> extra = new HashSet<>(candidateKeys)
        extra.removeAll(referenceKeys)
        report.extraInCandidate = extra.size()

        List<TileKey> common = new ArrayList<>(referenceKeys)
        common.retainAll(candidateKeys)
        common.sort { TileKey a, TileKey b ->
            int lv = Integer.compare(a.level, b.level)
            if (lv != 0) return lv
            int xv = Integer.compare(a.x, b.x)
            if (xv != 0) return xv
            return Integer.compare(a.y, b.y)
        }

        for (TileKey key : common) {
            byte[] referenceBytes = reference.tiles.get(key)
            byte[] candidateBytes = candidate.tiles.get(key)
            TileDifference diff = compareTile(key, referenceBytes, candidateBytes, maxRmse, maxChannelDelta)
            if (diff != null) {
                if (report.mismatches.size() < maxReportedMismatches) {
                    report.mismatches.add(diff)
                }
            }
        }

        return report
    }

    private static TileDifference compareTile(TileKey key,
                                              byte[] referenceBytes,
                                              byte[] candidateBytes,
                                              double maxRmse,
                                              int maxChannelDelta) {
        BufferedImage ref = ImageIO.read(new ByteArrayInputStream(referenceBytes))
        BufferedImage cand = ImageIO.read(new ByteArrayInputStream(candidateBytes))
        if (ref == null || cand == null) {
            return new TileDifference(key, Double.POSITIVE_INFINITY, 255, "undecodable image data")
        }
        if (ref.width != cand.width || ref.height != cand.height) {
            return new TileDifference(key, Double.POSITIVE_INFINITY, 255,
                    "dimension mismatch ref=${ref.width}x${ref.height} cand=${cand.width}x${cand.height}")
        }

        long sumSq = 0L
        int comparedChannels = 0
        int observedMax = 0
        for (int y = 0; y < ref.height; y++) {
            for (int x = 0; x < ref.width; x++) {
                int rgbA = ref.getRGB(x, y)
                int rgbB = cand.getRGB(x, y)
                int dr = Math.abs(((rgbA >> 16) & 0xff) - ((rgbB >> 16) & 0xff))
                int dg = Math.abs(((rgbA >> 8) & 0xff) - ((rgbB >> 8) & 0xff))
                int db = Math.abs((rgbA & 0xff) - (rgbB & 0xff))

                observedMax = Math.max(observedMax, Math.max(dr, Math.max(dg, db)))
                sumSq += (long) dr * dr
                sumSq += (long) dg * dg
                sumSq += (long) db * db
                comparedChannels += 3
            }
        }

        double rmse = Math.sqrt(sumSq / (double) comparedChannels)
        if (rmse > maxRmse || observedMax > maxChannelDelta) {
            return new TileDifference(key, rmse, observedMax,
                    "pixel delta exceeded thresholds rmse<=${maxRmse}, maxDelta<=${maxChannelDelta}")
        }

        return null
    }

    static String renderSummary(ComparisonReport report, int mismatchPreviewLimit) {
        StringBuilder sb = new StringBuilder()
        sb.append("referenceTiles=").append(report.referenceTileCount)
          .append(", candidateTiles=").append(report.candidateTileCount)
          .append(", missing=").append(report.missingInCandidate)
          .append(", extra=").append(report.extraInCandidate)
          .append(", mismatches=").append(report.mismatches.size())

        int limit = Math.min(mismatchPreviewLimit, report.mismatches.size())
        for (int i = 0; i < limit; i++) {
            TileDifference d = report.mismatches.get(i)
            sb.append("\n  - ").append(d.key)
              .append(" rmse=").append(String.format(Locale.ROOT, "%.4f", d.rmse))
              .append(" maxDelta=").append(d.maxChannelDelta)
              .append(" reason=").append(d.reason)
        }

        return sb.toString()
    }
}

