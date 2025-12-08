package au.org.ala.images.iiif;

import au.org.ala.images.util.DefaultImageReaderSelectionStrategy;
import com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.geom.AffineTransform;
import java.awt.image.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Objects;

/**
 * Minimal IIIF Image API 3.0 style processor that applies operations in the required order:
 * Region → Size → Rotation (with optional mirroring first) → Quality → Format.
 *
 * This class is intentionally focused and does not provide a URL parser. It expects
 * already-parsed parameters using the provided {@link Region}, {@link Size}, {@link Rotation},
 * {@link Quality} and {@link Format} value types.
 */
public class IiifImageProcessor {
    // Helper: canonical number formatting used by parameter canonicalizers
    private static String fmt(double v) {
        // Use BigDecimal via String ctor to preserve exact textual form when possible,
        // then strip trailing zeros and avoid scientific notation.
        java.math.BigDecimal bd = new java.math.BigDecimal(Double.toString(v));
        bd = bd.stripTrailingZeros();
        String s = bd.toPlainString();
        // Normalize "-0" to "0"
        if (s.equals("-0") || s.equals("-0.0")) return "0";
        return s;
    }

    private static final Logger log = LoggerFactory.getLogger(IiifImageProcessor.class);

    public IiifImageProcessor() {
    }

    /**
     * Process an image according to the IIIF-like parameters and write to an output stream in the requested format.
     * The output stream is not closed by this method.
     */
    public Result process(ByteSource imageBytes, Region region, Size size, Rotation rotation, Quality quality, Format format, OutputStream out) throws IOException {
        BufferedImage src;

        // Open stream once and create ImageReader
        try (InputStream is = imageBytes.openBufferedStream()) {
            ImageInputStream iis = ImageIO.createImageInputStream(is);
            if (iis == null) {
                throw new IOException("No ImageInputStream could be created for source image");
            }

            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                throw new IOException("No compatible ImageReader for source image");
            }

            // Use selection strategy to prefer TwelveMonkeys readers
            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                throw new IOException("No suitable ImageReader selected for source image");
            }

            try {
                reader.setInput(iis, true, false); // Set ignoreMetadata to false to allow reading metadata
                ImageReadParam readParam = reader.getDefaultReadParam();

                // Determine source dimensions
                int srcW = reader.getWidth(0);
                int srcH = reader.getHeight(0);

                // Compute region rectangle in source coordinates (Region step)
                Rectangle sourceRegion = computeSourceRegion(srcW, srcH, region);
                if (sourceRegion != null) {
                    // Guard against invalid sizes
                    if (sourceRegion.width <= 0 || sourceRegion.height <= 0) {
                        // degenerate region → read 1x1 minimal image via fallback (will be created later)
                        // In practice, clamp prevented this, but keep safe-guard
                        sourceRegion = new Rectangle(0, 0, Math.max(1, Math.min(1, srcW)), Math.max(1, Math.min(1, srcH)));
                    }
                    readParam.setSourceRegion(sourceRegion);
                } else {
                    // FULL region → treat as entire image
                    sourceRegion = new Rectangle(0, 0, srcW, srcH);
                }

                // Compute intended target dimensions from Size (applied to region result)
                Dimension targetDims = computeTargetSizeFromRegion(sourceRegion.width, sourceRegion.height, size);

                // Choose integer subsampling factors so decoded is >= target (avoid decoding larger than needed)
                int sx = 1;
                int sy = 1;
                if (targetDims != null) {
                    // If the request is an upscale or 'max', keep factors at 1
                    if (targetDims.width > 0) {
                        int cand = (int) Math.floor(sourceRegion.width / (double) targetDims.width);
                        if (cand >= 1) sx = cand;
                    }
                    if (targetDims.height > 0) {
                        int cand = (int) Math.floor(sourceRegion.height / (double) targetDims.height);
                        if (cand >= 1) sy = cand;
                    }
                }
                // Ensure at least 1
                sx = Math.max(1, sx);
                sy = Math.max(1, sy);
                readParam.setSourceSubsampling(sx, sy, 0, 0);

                // Read with subsampling and region applied
                src = reader.read(0, readParam);

                // We already applied the IIIF Region via setSourceRegion; avoid double-cropping by nulling region
                region = Region.full();
            } finally {
                reader.dispose();
            }
        }

        try {
            // 1. Region
            BufferedImage afterRegion = applyRegion(src, region);
            if (src != afterRegion) src.flush();

            // 2. Size
            BufferedImage afterSize = applySize(afterRegion, size);
            if (afterRegion != afterSize) afterRegion.flush();

            // 3. Rotation (mirror first if requested)
            BufferedImage afterRotation = applyRotation(afterSize, rotation);
            if (afterSize != afterRotation) afterSize.flush();

            // 4. Quality
            BufferedImage afterQuality = applyQuality(afterRotation, quality);
            if (afterRotation != afterQuality) afterRotation.flush();

            // 5. Format (encode)
            String formatName = format.getFormatName();
            boolean ok = ImageIO.write(afterQuality, formatName, out);
            if (!ok) {
                throw new IOException("No ImageIO writer for format: " + formatName);
            }

            Result res = new Result(afterQuality.getWidth(), afterQuality.getHeight(), format.getMimeType());
            if (afterQuality != src) afterQuality.flush();
            return res;
        } finally {
            // ensure src drained
            if (src != null) src.flush();
        }
    }

    /**
     * Compute the source region rectangle (in original image coordinates) for the requested IIIF Region.
     * Returns null for FULL region.
     */
    private Rectangle computeSourceRegion(int srcW, int srcH, Region region) {
        if (region == null || region.type == Region.Type.FULL) {
            return null;
        }
        if (region.type == Region.Type.SQUARE) {
            int side = Math.min(srcW, srcH);
            int x = (srcW - side) / 2;
            int y = (srcH - side) / 2;
            return new Rectangle(x, y, side, side);
        }
        if (region.type == Region.Type.ASPECT) {
            // Crop the largest possible centered rectangle whose width:height equals the requested ratio (w:h).
            // Achieve exact ratio by selecting an integer scale factor k such that:
            //   width = floor(k * w), height = floor(k * h), with both within src bounds.
            double arW = region.w;
            double arH = region.h;
            if (arW <= 0 || arH <= 0 || Double.isNaN(arW) || Double.isNaN(arH) || Double.isInfinite(arW) || Double.isInfinite(arH)) {
                return null; // treat as FULL on invalid input
            }

            // Compute the maximum integer k that fits within src dimensions.
            double kW = srcW / arW;
            double kH = srcH / arH;
            double k = Math.min(kW, kH);
            if (k <= 1) {
                // If the requested ratio parts are larger than the image, fallback to FULL
                return null;
            }

            int w = (int) Math.floor(k * arW);
            int h = (int) Math.floor(k * arH);
            // Safety: clamp in case of rounding edge cases
            if (w <= 0) w = 1;
            if (h <= 0) h = 1;
            if (w > srcW) w = srcW;
            if (h > srcH) h = srcH;

            int x = (srcW - w) / 2;
            int y = (srcH - h) / 2;
            return new Rectangle(x, y, w, h);
        }
        int x, y, w, h;
        if (region.isPercent) {
            x = (int) Math.round(srcW * region.x / 100.0);
            y = (int) Math.round(srcH * region.y / 100.0);
            w = (int) Math.round(srcW * region.w / 100.0);
            h = (int) Math.round(srcH * region.h / 100.0);
        } else {
            x = (int) Math.round(region.x);
            y = (int) Math.round(region.y);
            w = (int) Math.round(region.w);
            h = (int) Math.round(region.h);
        }
        // clamp to bounds
        x = Math.max(0, Math.min(x, srcW));
        y = Math.max(0, Math.min(y, srcH));
        w = Math.max(0, Math.min(w, srcW - x));
        h = Math.max(0, Math.min(h, srcH - y));
        if (w <= 0 || h <= 0) {
            // Degenerate region: fallback to 1x1 at clamped location
            w = 1; h = 1;
        }
        return new Rectangle(x, y, w, h);
    }

    /**
     * Compute the target size to be used for subsampling decisions, given the region's width/height and the IIIF Size.
     * Returns null if no sizing is requested (MAX), meaning no subsampling beyond region.
     */
    private Dimension computeTargetSizeFromRegion(int regionW, int regionH, Size size) {
        if (size == null || size.type == Size.Type.MAX) {
            return new Dimension(regionW, regionH);
        }
        int dstW = regionW;
        int dstH = regionH;
        switch (size.type) {
            case WIDTH_ONLY:
                dstW = size.w;
                dstH = (int) Math.round(regionH * (dstW / (double) regionW));
                break;
            case HEIGHT_ONLY:
                dstH = size.h;
                dstW = (int) Math.round(regionW * (dstH / (double) regionH));
                break;
            case PERCENT:
                dstW = (int) Math.round(regionW * size.percent / 100.0);
                dstH = (int) Math.round(regionH * size.percent / 100.0);
                break;
            case EXACT:
                dstW = size.w;
                dstH = size.h;
                break;
            case BEST_FIT:
                double scale = Math.min(size.w / (double) regionW, size.h / (double) regionH);
                dstW = (int) Math.round(regionW * scale);
                dstH = (int) Math.round(regionH * scale);
                break;
            default:
                break;
        }
        if (dstW <= 0) dstW = 1;
        if (dstH <= 0) dstH = 1;
        return new Dimension(dstW, dstH);
    }

    private BufferedImage applyRegion(BufferedImage src, Region region) {
        if (region == null || region.type == Region.Type.FULL) {
            return src;
        }

        if (region.type == Region.Type.SQUARE) {
            int side = Math.min(src.getWidth(), src.getHeight());
            int x = (src.getWidth() - side) / 2;
            int y = (src.getHeight() - side) / 2;
            return src.getSubimage(x, y, side, side);
        }

        int x, y, w, h;
        if (region.isPercent) {
            x = (int) Math.round(src.getWidth() * region.x / 100.0);
            y = (int) Math.round(src.getHeight() * region.y / 100.0);
            w = (int) Math.round(src.getWidth() * region.w / 100.0);
            h = (int) Math.round(src.getHeight() * region.h / 100.0);
        } else {
            x = (int) Math.round(region.x);
            y = (int) Math.round(region.y);
            w = (int) Math.round(region.w);
            h = (int) Math.round(region.h);
        }
        // clamp to bounds
        x = Math.max(0, Math.min(x, src.getWidth()));
        y = Math.max(0, Math.min(y, src.getHeight()));
        w = Math.max(0, Math.min(w, src.getWidth() - x));
        h = Math.max(0, Math.min(h, src.getHeight() - y));
        if (w <= 0 || h <= 0) {
            // return a 1x1 empty image to avoid exceptions
            return new BufferedImage(1, 1, BufferedImage.TYPE_3BYTE_BGR);
        }
        return src.getSubimage(x, y, w, h);
    }

    private BufferedImage applySize(BufferedImage src, Size size) {
        if (size == null || size.type == Size.Type.MAX) {
            return src;
        }

        int srcW = src.getWidth();
        int srcH = src.getHeight();

        int dstW = srcW;
        int dstH = srcH;

        switch (size.type) {
            case WIDTH_ONLY:
                dstW = size.w;
                dstH = (int) Math.round(srcH * (dstW / (double) srcW));
                break;
            case HEIGHT_ONLY:
                dstH = size.h;
                dstW = (int) Math.round(srcW * (dstH / (double) srcH));
                break;
            case PERCENT:
                dstW = (int) Math.round(srcW * size.percent / 100.0);
                dstH = (int) Math.round(srcH * size.percent / 100.0);
                break;
            case EXACT:
                // change aspect ratio if needed
                dstW = size.w;
                dstH = size.h;
                break;
            case BEST_FIT:
                double scale = Math.min(size.w / (double) srcW, size.h / (double) srcH);
                dstW = (int) Math.round(srcW * scale);
                dstH = (int) Math.round(srcH * scale);
                break;
            default:
                return src;
        }

        if (dstW <= 0) dstW = 1;
        if (dstH <= 0) dstH = 1;

        Image tmp = src.getScaledInstance(dstW, dstH, Image.SCALE_SMOOTH);
        BufferedImage resized = new BufferedImage(dstW, dstH, bestTypeFor(src));
        Graphics2D g2 = resized.createGraphics();
        try {
            g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
            g2.drawImage(tmp, 0, 0, null);
        } finally {
            g2.dispose();
        }
        return resized;
    }

    private BufferedImage applyRotation(BufferedImage src, Rotation rotation) {
        if (rotation == null || (rotation.degrees % 360.0 == 0.0 && !rotation.mirror)) {
            return src;
        }

        BufferedImage working = src;

        // Mirroring is applied BEFORE rotation per spec when present
        if (rotation.mirror) {
            AffineTransform tx = new AffineTransform();
            tx.scale(-1, 1);
            tx.translate(-working.getWidth(), 0);
            working = transform(working, tx);
        }

        double angleRad = Math.toRadians(rotation.degrees % 360.0);
        if (angleRad < 0) angleRad += Math.PI * 2;

        if (rotation.degrees % 360.0 == 0.0) {
            return working;
        }

        double sin = Math.abs(Math.sin(angleRad));
        double cos = Math.abs(Math.cos(angleRad));
        int w = working.getWidth();
        int h = working.getHeight();
        int newW = (int) Math.floor(w * cos + h * sin);
        int newH = (int) Math.floor(h * cos + w * sin);

        AffineTransform at = new AffineTransform();
        // Translate to center of new image
        at.translate(newW / 2.0, newH / 2.0);
        // Rotate around origin
        at.rotate(angleRad);
        // Translate back to image center
        at.translate(-w / 2.0, -h / 2.0);

        return transform(working, at, newW, newH);
    }

    private BufferedImage applyQuality(BufferedImage src, Quality quality) {
        if (quality == null || quality == Quality.DEFAULT || quality == Quality.COLOR) {
            return src;
        }
        if (quality == Quality.GRAY) {
            ColorConvertOp op = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
            BufferedImage out = new BufferedImage(src.getWidth(), src.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
            op.filter(src, out);
            return out;
        }
        if (quality == Quality.BITONAL) {
            // simple threshold at mid-tone
            int w = src.getWidth();
            int h = src.getHeight();
            BufferedImage out = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_BINARY);
            WritableRaster outRaster = out.getRaster();
            Raster inRaster = src.getRaster();
            int[] pixel = new int[src.getSampleModel().getNumBands()];
            for (int y = 0; y < h; y++) {
                for (int x = 0; x < w; x++) {
                    inRaster.getPixel(x, y, pixel);
                    int r, g, b;
                    if (pixel.length >= 3) {
                        r = pixel[0];
                        g = pixel[1];
                        b = pixel[2];
                    } else {
                        r = g = b = pixel[0];
                    }
                    int gray = (int) Math.round(0.2126 * r + 0.7152 * g + 0.0722 * b);
                    int bw = gray >= 128 ? 1 : 0;
                    outRaster.setSample(x, y, 0, bw);
                }
            }
            return out;
        }
        return src;
    }

    private int bestTypeFor(BufferedImage src) {
        int type = src.getType();
        if (type == BufferedImage.TYPE_CUSTOM || type == 0) {
            // choose based on presence of alpha
            boolean hasAlpha = src.getColorModel().hasAlpha();
            return hasAlpha ? BufferedImage.TYPE_4BYTE_ABGR : BufferedImage.TYPE_3BYTE_BGR;
        }
        return type;
    }

    private BufferedImage transform(BufferedImage src, AffineTransform tx) {
        return transform(src, tx, src.getWidth(), src.getHeight());
    }

    private BufferedImage transform(BufferedImage src, AffineTransform tx, int outW, int outH) {
        BufferedImage dst = new BufferedImage(outW, outH, bestTypeFor(src));
        Graphics2D g2 = dst.createGraphics();
        try {
            g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
            g2.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2.drawImage(src, tx, null);
        } finally {
            g2.dispose();
        }
        return dst;
    }

    // --- Value types -------------------------------------------------------

    public static class Region {
        public enum Type { FULL, SQUARE, ABSOLUTE, PERCENT, ASPECT }
        public final Type type;
        public final boolean isPercent;
        public final double x, y, w, h;

        private Region(Type type, boolean isPercent, double x, double y, double w, double h) {
            this.type = type;
            this.isPercent = isPercent;
            this.x = x; this.y = y; this.w = w; this.h = h;
        }

        public static Region full() { return new Region(Type.FULL, false, 0, 0, 0, 0); }
        public static Region square() { return new Region(Type.SQUARE, false, 0, 0, 0, 0); }
        public static Region absolute(double x, double y, double w, double h) { return new Region(Type.ABSOLUTE, false, x, y, w, h); }
        public static Region percent(double xPct, double yPct, double wPct, double hPct) { return new Region(Type.PERCENT, true, xPct, yPct, wPct, hPct); }
        /**
         * Aspect ratio crop that selects the largest possible centered rectangle with the given ratio (w:h).
         * Example: aspect(16, 9) for 16:9.
         */
        public static Region aspect(double wRatio, double hRatio) { return new Region(Type.ASPECT, false, 0, 0, wRatio, hRatio); }

        /**
         * Parse IIIF Region from query parameter string.
         * Supported:
         * - "full"
         * - "square"
         * - "x,y,w,h"
         * - "pct:x,y,w,h"
         *
         * Custom extension:
         * - "ar:w,h" or "ar:wxh" or "aspect:w:h" or "aspect:w/h" for aspect-ratio centered crop
         */
        public static Region parse(String s) {
            if (s == null) throw new IllegalArgumentException("Region string is null");
            String in = s.trim().toLowerCase();
            if (in.equals("full")) return full();
            if (in.equals("square")) return square();
            if (in.startsWith("ar:") || in.startsWith("aspect:")) {
                String work = in.startsWith("ar:") ? in.substring(3) : in.substring(7);
                // Split on ':' or 'x' to support formats: "aspect:16:9" or "ar:16x9" or "aspect:16/9"
                String sepWork = work.replace('x', ',').replace('/', ',').replace(':', ',');
                String[] pr = sepWork.split(",");
                if (pr.length != 2) {
                    throw new IllegalArgumentException("Invalid aspect region string: " + s);
                }
                try {
                    double rw = Double.parseDouble(pr[0]);
                    double rh = Double.parseDouble(pr[1]);
                    if (rw <= 0 || rh <= 0) {
                        throw new IllegalArgumentException("Aspect components must be > 0: " + s);
                    }
                    return aspect(rw, rh);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number in aspect region string: " + s, e);
                }
            }
            boolean percentFlag = false;
            String work = in;
            if (work.startsWith("pct:")) {
                percentFlag = true;
                work = work.substring(4);
            }
            String[] parts = work.split(",");
            if (parts.length != 4) {
                throw new IllegalArgumentException("Invalid region string: " + s);
            }
            try {
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                double w = Double.parseDouble(parts[2]);
                double h = Double.parseDouble(parts[3]);
                if (percentFlag) return percent(x, y, w, h);
                return absolute(x, y, w, h);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid number in region string: " + s, e);
            }
        }

        /**
         * Canonical IIIF region syntax:
         * - full
         * - square
         * - x,y,w,h (absolute)
         * - pct:x,y,w,h (percentage)
         * Custom extension:
         * - ar:w,h (aspect-ratio centered crop)
         */
        public String canonical() {
            switch (type) {
                case FULL:
                    return "full";
                case SQUARE:
                    return "square";
                case ABSOLUTE:
                    return fmt(x) + "," + fmt(y) + "," + fmt(w) + "," + fmt(h);
                case PERCENT:
                    return "pct:" + fmt(x) + "," + fmt(y) + "," + fmt(w) + "," + fmt(h);
                case ASPECT:
                    return "ar:" + fmt(w) + "," + fmt(h);
                default:
                    throw new IllegalStateException("Unknown Region type: " + type);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Region)) return false;
            Region region = (Region) o;
            return isPercent == region.isPercent &&
                    Double.compare(region.x, x) == 0 &&
                    Double.compare(region.y, y) == 0 &&
                    Double.compare(region.w, w) == 0 &&
                    Double.compare(region.h, h) == 0 &&
                    type == region.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, isPercent,
                    Double.valueOf(x), Double.valueOf(y), Double.valueOf(w), Double.valueOf(h));
        }

        @Override
        public String toString() {
            return "Region{" +
                    "type=" + type +
                    ", isPercent=" + isPercent +
                    ", x=" + x +
                    ", y=" + y +
                    ", w=" + w +
                    ", h=" + h +
                    '}';
        }
    }

    public static class Size {
        public enum Type { MAX, WIDTH_ONLY, HEIGHT_ONLY, PERCENT, EXACT, BEST_FIT }
        public final Type type;
        public final int w, h;
        public final double percent;
        public final boolean upscaling;

        private Size(Type type, int w, int h, double percent, boolean upscaling) {
            this.type = type; this.w = w; this.h = h; this.percent = percent; this.upscaling = upscaling;
        }

        public static Size max(boolean upscaling) { return new Size(Type.MAX, 0, 0, 0, upscaling); }
        public static Size width(int w, boolean upscaling) { return new Size(Type.WIDTH_ONLY, w, 0, 0, upscaling); }
        public static Size height(int h, boolean upscaling) { return new Size(Type.HEIGHT_ONLY, 0, h, 0, upscaling); }
        public static Size percent(double pct, boolean upscaling) { return new Size(Type.PERCENT, 0, 0, pct, upscaling); }
        public static Size exact(int w, int h, boolean upscaling) { return new Size(Type.EXACT, w, h, 0, upscaling); }
        public static Size bestFit(int maxW, int maxH, boolean upscaling) { return new Size(Type.BEST_FIT, maxW, maxH, 0, upscaling); }

        /**
         * Parse IIIF Size from query parameter string.
         * Supported (with optional leading '^' to allow upscaling):
         * - "max"
         * - "w,"
         * - ",h"
         * - "pct:n"
         * - "w,h"
         * - "!w,h" (best fit)
         */
        public static Size parse(String s) {
            if (s == null) throw new IllegalArgumentException("Size string is null");
            String in = s.trim();
            boolean up = false;
            if (in.startsWith("^")) {
                up = true;
                in = in.substring(1);
            }
            String lower = in.toLowerCase();
            if (lower.equals("max")) return max(up);
            boolean best = false;
            if (lower.startsWith("!")) {
                best = true;
                lower = lower.substring(1);
            }
            if (lower.startsWith("pct:")) {
                String num = lower.substring(4);
                try {
                    double pct = Double.parseDouble(num);
                    return percent(pct, up);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid pct value in size: " + s, e);
                }
            }
            String[] parts = lower.split(",", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid size string: " + s);
            }
            try {
                if (!parts[0].isEmpty() && parts[1].isEmpty()) {
                    int w = Integer.parseInt(parts[0]);
                    return width(w, up);
                } else if (parts[0].isEmpty() && !parts[1].isEmpty()) {
                    int h = Integer.parseInt(parts[1]);
                    return height(h, up);
                } else if (!parts[0].isEmpty() && !parts[1].isEmpty()) {
                    int w = Integer.parseInt(parts[0]);
                    int h = Integer.parseInt(parts[1]);
                    if (best) return bestFit(w, h, up);
                    return exact(w, h, up);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid number in size string: " + s, e);
            }
            throw new IllegalArgumentException("Invalid size string: " + s);
        }

        /**
         * Canonical IIIF size syntax (Image API 2.x/3.x compatible):
         * Optional '^' prefix when upscaling is allowed.
         * - max
         * - w, (width only)
         * - ,h (height only)
         * - pct:n (percentage)
         * - w,h (exact)
         * - !w,h (best fit)
         */
        public String canonical() {
            String prefix = upscaling ? "^" : "";
            switch (type) {
                case MAX:
                    return prefix + "max";
                case WIDTH_ONLY:
                    return prefix + w + ",";
                case HEIGHT_ONLY:
                    return prefix + "," + h;
                case PERCENT:
                    return prefix + "pct:" + fmt(percent);
                case EXACT:
                    return prefix + w + "," + h;
                case BEST_FIT:
                    return prefix + "!" + w + "," + h;
                default:
                    throw new IllegalStateException("Unknown Size type: " + type);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Size)) return false;
            Size size = (Size) o;
            return w == size.w &&
                    h == size.h &&
                    Double.compare(size.percent, percent) == 0 &&
                    upscaling == size.upscaling &&
                    type == size.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, w, h, Double.valueOf(percent), upscaling);
        }

        @Override
        public String toString() {
            return "Size{" +
                    "type=" + type +
                    ", w=" + w +
                    ", h=" + h +
                    ", percent=" + percent +
                    ", upscaling=" + upscaling +
                    '}';
        }
    }

    public static class Rotation {
        public final boolean mirror; // true if the '!' modifier is present
        public final double degrees; // 0–360
        public Rotation(boolean mirror, double degrees) {
            this.mirror = mirror; this.degrees = degrees;
        }
        public static Rotation none() { return new Rotation(false, 0); }

        /**
         * Parse IIIF Rotation from query parameter string.
         * Supported: optional leading '!' for mirroring, then degrees (double).
         */
        public static Rotation parse(String s) {
            if (s == null) throw new IllegalArgumentException("Rotation string is null");
            String in = s.trim();
            boolean mirror = false;
            if (in.startsWith("!")) {
                mirror = true;
                in = in.substring(1);
            }
            try {
                double deg = Double.parseDouble(in);
                return new Rotation(mirror, deg);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid rotation string: " + s, e);
            }
        }

        /**
         * Canonical IIIF rotation syntax: optional '!' for mirroring, followed by degrees.
         */
        public String canonical() {
            return (mirror ? "!" : "") + fmt(degrees);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Rotation)) return false;
            Rotation rotation = (Rotation) o;
            return mirror == rotation.mirror && Double.compare(rotation.degrees, degrees) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mirror, Double.valueOf(degrees));
        }

        @Override
        public String toString() {
            return "Rotation{" +
                    "mirror=" + mirror +
                    ", degrees=" + degrees +
                    '}';
        }
    }

    // moved/extended below with a canonical() method

    public enum Format {
        JPG("jpg", "image/jpeg"),
        PNG("png", "image/png"),
        WEBP("webp", "image/webp"),
        TIFF("tif", "image/tiff"),
        GIF("gif", "image/gif");

        private final String formatName;
        private final String mimeType;
        Format(String formatName, String mimeType) {
            this.formatName = formatName;
            this.mimeType = mimeType;
        }
        public String getFormatName() { return formatName; }
        public String getMimeType() { return mimeType; }

        /**
         * Canonical IIIF format syntax: extension token (e.g., "jpg").
         */
        public String canonical() { return formatName; }

        /**
         * Parse output format token. Accepts common synonyms and case-insensitive:
         * jpg/jpeg, png, webp, tif/tiff, gif
         */
        public static Format parse(String s) {
            if (s == null) throw new IllegalArgumentException("Format string is null");
            String in = s.trim().toLowerCase();
            switch (in) {
                case "jpg":
                case "jpeg":
                    return JPG;
                case "png":
                    return PNG;
                case "webp":
                    return WEBP;
                case "tif":
                case "tiff":
                    return TIFF;
                case "gif":
                    return GIF;
                default:
                    throw new IllegalArgumentException("Unknown format: " + s);
            }
        }
    }

    /**
     * Canonical token for Quality, lower-case per IIIF.
     */
    public enum Quality { DEFAULT, COLOR, GRAY, BITONAL;
        public String canonical() { return name().toLowerCase(); }

        /**
         * Parse quality token. Accepts case-insensitive tokens and common synonyms:
         * default/native, color/colour, gray/grey, bitonal/mono/binary
         */
        public static Quality parse(String s) {
            if (s == null) throw new IllegalArgumentException("Quality string is null");
            String in = s.trim().toLowerCase();
            switch (in) {
                case "default":
                case "native":
                    return DEFAULT;
                case "color":
                case "colour":
                    return COLOR;
                case "gray":
                case "grey":
                case "grayscale":
                case "greyscale":
                    return GRAY;
                case "bitonal":
                case "mono":
                case "binary":
                    return BITONAL;
                default:
                    throw new IllegalArgumentException("Unknown quality: " + s);
            }
        }
    }

    public static class Result {
        public final int width;
        public final int height;
        public final String mimeType;
        public Result(int width, int height, String mimeType) {
            this.width = width; this.height = height; this.mimeType = mimeType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Result)) return false;
            Result result = (Result) o;
            return width == result.width && height == result.height && Objects.equals(mimeType, result.mimeType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(width, height, mimeType);
        }

        @Override
        public String toString() {
            return "Result{" +
                    "width=" + width +
                    ", height=" + height +
                    ", mimeType='" + mimeType + '\'' +
                    '}';
        }
    }
}
