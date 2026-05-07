package au.org.ala.images.iiif;

import au.org.ala.images.util.DefaultImageReaderSelectionStrategy;
import com.google.common.io.ByteSource;
import org.apache.commons.io.IOUtils;
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

/**
 * Pure Java implementation of IIIF Image API 3.0 style processor using ImageIO and BufferedImage.
 */
public class JavaIiifImageProcessor implements IiifImageProcessor {

    private static final Logger log = LoggerFactory.getLogger(JavaIiifImageProcessor.class);

    public JavaIiifImageProcessor() {
    }

    @Override
    public Result process(ByteSource imageBytes, Region region, Size size, Rotation rotation, Quality quality, Format format, OutputStream out) throws IOException {
        BufferedImage src;

        // Open stream once and create ImageReader
        try (InputStream is = imageBytes.openBufferedStream()) {
            ImageInputStream iis = ImageIO.createImageInputStream(is);
            if (iis == null) {
                IOUtils.consume(is);
                throw new IOException("No ImageInputStream could be created for source image");
            }

            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                IOUtils.consume(is);
                throw new IOException("No compatible ImageReader for source image");
            }

            // Use selection strategy to prefer TwelveMonkeys readers
            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                IOUtils.consume(is);
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
            double arW = region.w;
            double arH = region.h;
            if (arW <= 0 || arH <= 0 || Double.isNaN(arW) || Double.isNaN(arH) || Double.isInfinite(arW) || Double.isInfinite(arH)) {
                return null;
            }

            double kW = srcW / arW;
            double kH = srcH / arH;
            double k = Math.min(kW, kH);
            if (k <= 1) {
                return null;
            }

            int w = (int) Math.floor(k * arW);
            int h = (int) Math.floor(k * arH);
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
        x = Math.max(0, Math.min(x, srcW));
        y = Math.max(0, Math.min(y, srcH));
        w = Math.max(0, Math.min(w, srcW - x));
        h = Math.max(0, Math.min(h, srcH - y));
        if (w <= 0 || h <= 0) {
            w = 1; h = 1;
        }
        return new Rectangle(x, y, w, h);
    }

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
        x = Math.max(0, Math.min(x, src.getWidth()));
        y = Math.max(0, Math.min(y, src.getHeight()));
        w = Math.max(0, Math.min(w, src.getWidth() - x));
        h = Math.max(0, Math.min(h, src.getHeight() - y));
        if (w <= 0 || h <= 0) {
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
        if (rotation == null) {
            return src;
        }

        double deg = ((rotation.degrees % 360.0) + 360.0) % 360.0;
        BufferedImage working = src;

        if (rotation.mirror) {
            AffineTransform tx = new AffineTransform();
            tx.scale(-1, 1);
            tx.translate(-working.getWidth(), 0);
            working = transform(working, tx);
        }

        if (deg == 0.0) {
            return working;
        }

        double angleRad = Math.toRadians(deg);

        double sin = Math.abs(Math.sin(angleRad));
        double cos = Math.abs(Math.cos(angleRad));
        int w = working.getWidth();
        int h = working.getHeight();
        int newW = (int) Math.floor(w * cos + h * sin);
        int newH = (int) Math.floor(h * cos + w * sin);

        AffineTransform at = new AffineTransform();
        at.translate(newW / 2.0, newH / 2.0);
        at.rotate(angleRad);
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
}
