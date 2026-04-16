package au.org.ala.images.iiif;

import com.google.common.io.ByteSource;
import com.google.errorprone.annotations.ThreadSafe;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Minimal IIIF Image API 3.0 style processor that applies operations in the required order:
 * Region → Size → Rotation (with optional mirroring first) → Quality → Format.
 */
@ThreadSafe
public interface IiifImageProcessor {

    /**
     * Process an image according to the IIIF-like parameters and write to an output stream in the requested format.
     * The output stream is not closed by this method.
     */
    Result process(ByteSource imageBytes, Region region, Size size, Rotation rotation, Quality quality, Format format, OutputStream out) throws IOException;

    // Helper: canonical number formatting used by parameter canonicalizers
    static String fmt(double v) {
        java.math.BigDecimal bd = new java.math.BigDecimal(Double.toString(v));
        bd = bd.stripTrailingZeros();
        String s = bd.toPlainString();
        if (s.equals("-0") || s.equals("-0.0")) return "0";
        return s;
    }

    // --- Value types -------------------------------------------------------

    class Region {
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
        public static Region aspect(double wRatio, double hRatio) { return new Region(Type.ASPECT, false, 0, 0, wRatio, hRatio); }

        public static Region parse(String s) {
            if (s == null) throw new IllegalArgumentException("Region string is null");
            String in = s.trim().toLowerCase();
            if (in.equals("full")) return full();
            if (in.equals("square")) return square();
            if (in.startsWith("ar:") || in.startsWith("aspect:")) {
                String work = in.startsWith("ar:") ? in.substring(3) : in.substring(7);
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

        public String canonical() {
            switch (type) {
                case FULL: return "full";
                case SQUARE: return "square";
                case ABSOLUTE: return fmt(x) + "," + fmt(y) + "," + fmt(w) + "," + fmt(h);
                case PERCENT: return "pct:" + fmt(x) + "," + fmt(y) + "," + fmt(w) + "," + fmt(h);
                case ASPECT: return "ar:" + fmt(w) + "," + fmt(h);
                default: throw new IllegalStateException("Unknown Region type: " + type);
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
            return Objects.hash(type, isPercent, x, y, w, h);
        }

        @Override
        public String toString() {
            return "Region{type=" + type + ", isPercent=" + isPercent + ", x=" + x + ", y=" + y + ", w=" + w + ", h=" + h + '}';
        }
    }

    class Size {
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

        public String canonical() {
            String prefix = upscaling ? "^" : "";
            switch (type) {
                case MAX: return prefix + "max";
                case WIDTH_ONLY: return prefix + w + ",";
                case HEIGHT_ONLY: return prefix + "," + h;
                case PERCENT: return prefix + "pct:" + fmt(percent);
                case EXACT: return prefix + w + "," + h;
                case BEST_FIT: return prefix + "!" + w + "," + h;
                default: throw new IllegalStateException("Unknown Size type: " + type);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Size)) return false;
            Size size = (Size) o;
            return w == size.w && h == size.h && Double.compare(size.percent, percent) == 0 && upscaling == size.upscaling && type == size.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, w, h, percent, upscaling);
        }

        @Override
        public String toString() {
            return "Size{type=" + type + ", w=" + w + ", h=" + h + ", percent=" + percent + ", upscaling=" + upscaling + '}';
        }
    }

    class Rotation {
        public final boolean mirror;
        public final double degrees;
        public Rotation(boolean mirror, double degrees) {
            this.mirror = mirror; this.degrees = degrees;
        }
        public static Rotation none() { return new Rotation(false, 0); }

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
            return Objects.hash(mirror, degrees);
        }

        @Override
        public String toString() {
            return "Rotation{mirror=" + mirror + ", degrees=" + degrees + '}';
        }
    }

    enum Format {
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
        public String canonical() { return formatName; }

        public static Format parse(String s) {
            if (s == null) throw new IllegalArgumentException("Format string is null");
            String in = s.trim().toLowerCase();
            switch (in) {
                case "jpg":
                case "jpeg": return JPG;
                case "png": return PNG;
                case "webp": return WEBP;
                case "tif":
                case "tiff": return TIFF;
                case "gif": return GIF;
                default: throw new IllegalArgumentException("Unknown format: " + s);
            }
        }
    }

    enum Quality {
        DEFAULT, COLOR, GRAY, BITONAL;
        public String canonical() { return name().toLowerCase(); }

        public static Quality parse(String s) {
            if (s == null) throw new IllegalArgumentException("Quality string is null");
            String in = s.trim().toLowerCase();
            switch (in) {
                case "default":
                case "native": return DEFAULT;
                case "color":
                case "colour": return COLOR;
                case "gray":
                case "grey":
                case "grayscale":
                case "greyscale": return GRAY;
                case "bitonal":
                case "mono":
                case "binary": return BITONAL;
                default: throw new IllegalArgumentException("Unknown quality: " + s);
            }
        }
    }

    class Result {
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
            return "Result{width=" + width + ", height=" + height + ", mimeType='" + mimeType + "'}";
        }
    }
}
