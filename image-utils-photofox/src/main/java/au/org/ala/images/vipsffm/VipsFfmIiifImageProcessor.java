package au.org.ala.images.vipsffm;

import app.photofox.vipsffm.VImage;
import app.photofox.vipsffm.VipsOption;
import app.photofox.vipsffm.enums.VipsAngle;
import app.photofox.vipsffm.enums.VipsDirection;
import app.photofox.vipsffm.enums.VipsInterpretation;
import app.photofox.vipsffm.enums.VipsOperationRelational;
import au.org.ala.images.iiif.IiifImageProcessor;
import au.org.ala.images.iiif.JavaIiifImageProcessor;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Photofox vips-ffm based IIIF processor.
 */
public class VipsFfmIiifImageProcessor implements IiifImageProcessor {

    private static final Logger log = LoggerFactory.getLogger(VipsFfmIiifImageProcessor.class);
    private static final double EPS = 1e-10;

    private final IiifImageProcessor fallback;

    public VipsFfmIiifImageProcessor(IiifImageProcessor fallback) {
        // Keep behavior safe if no fallback was provided by caller.
        this.fallback = fallback != null ? fallback : new JavaIiifImageProcessor();
    }

    @Override
    public Result process(
        ByteSource imageBytes,
        Region region,
        Size size,
        Rotation rotation,
        Quality quality,
        Format format,
        OutputStream out
    ) throws IOException {
        boolean startedWrite = false;

        try (Arena arena = Arena.ofConfined(); InputStream in = imageBytes.openBufferedStream()) {
            VImage image = VImage.newFromStream(arena, in);
            VImage workingImage = applyRegion(image, region, image.getWidth(), image.getHeight());
            workingImage = applySize(workingImage, size);
            workingImage = applyRotation(workingImage, rotation);
            workingImage = applyQuality(workingImage, quality);

            String suffix = "." + format.getFormatName();
            startedWrite = true;
            workingImage.writeToStream(out, suffix);

            return new Result(workingImage.getWidth(), workingImage.getHeight(), format.getMimeType());
        } catch (Exception e) {
            if (startedWrite) {
                throw new IOException("Photofox IIIF processing failed after write started", e);
            }
            log.warn("Photofox IIIF processing failed, falling back", e);
            return fallback.process(imageBytes, region, size, rotation, quality, format, out);
        }
    }

    private VImage applyRegion(VImage image, Region region, int srcW, int srcH) throws Exception {
        if (region == null || region.type == Region.Type.FULL) {
            return image;
        }

        int x;
        int y;
        int w;
        int h;

        if (region.type == Region.Type.SQUARE) {
            int side = Math.min(srcW, srcH);
            x = (srcW - side) / 2;
            y = (srcH - side) / 2;
            w = side;
            h = side;
        } else if (region.type == Region.Type.ASPECT) {
            double arW = region.w;
            double arH = region.h;
            double k = Math.min((double) srcW / arW, (double) srcH / arH);
            w = (int) Math.floor(k * arW);
            h = (int) Math.floor(k * arH);
            x = (srcW - w) / 2;
            y = (srcH - h) / 2;
        } else if (region.isPercent) {
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

        x = Math.max(0, Math.min(x, srcW - 1));
        y = Math.max(0, Math.min(y, srcH - 1));
        w = Math.max(1, Math.min(w, srcW - x));
        h = Math.max(1, Math.min(h, srcH - y));

        return image.extractArea(x, y, w, h);
    }

    private VImage applySize(VImage image, Size size) throws Exception {
        if (size == null || size.type == Size.Type.MAX) {
            return image;
        }

        int srcW = image.getWidth();
        int srcH = image.getHeight();
        double scaleW = 1.0;
        double scaleH = 1.0;

        switch (size.type) {
            case WIDTH_ONLY:
                scaleW = size.w / (double) srcW;
                scaleH = scaleW;
                break;
            case HEIGHT_ONLY:
                scaleH = size.h / (double) srcH;
                scaleW = scaleH;
                break;
            case PERCENT:
                scaleW = size.percent / 100.0;
                scaleH = scaleW;
                break;
            case EXACT:
                scaleW = size.w / (double) srcW;
                scaleH = size.h / (double) srcH;
                break;
            case BEST_FIT:
                scaleW = Math.min(size.w / (double) srcW, size.h / (double) srcH);
                scaleH = scaleW;
                break;
            default:
                return image;
        }

        if (!size.upscaling) {
            scaleW = Math.min(1.0d, scaleW);
            scaleH = Math.min(1.0d, scaleH);
        }

        if (Math.abs(scaleW - 1.0d) < EPS && Math.abs(scaleH - 1.0d) < EPS) {
            return image;
        }

        if (Math.abs(scaleW - scaleH) < EPS) {
            return image.resize(scaleW);
        }
        return image.resize(scaleW, VipsOption.Double("vscale", scaleH));
    }

    private VImage applyRotation(VImage image, Rotation rotation) throws Exception {
        if (rotation == null) {
            return image;
        }

        double deg = ((rotation.degrees % 360.0) + 360.0) % 360.0;
        VImage current = image;

        if (rotation.mirror) {
            current = current.flip(VipsDirection.DIRECTION_HORIZONTAL);
        }

        if (Math.abs(deg) < EPS) {
            return current;
        }
        if (Math.abs(deg - 90.0) < EPS) {
            return current.rot(VipsAngle.ANGLE_D90);
        }
        if (Math.abs(deg - 180.0) < EPS) {
            return current.rot(VipsAngle.ANGLE_D180);
        }
        if (Math.abs(deg - 270.0) < EPS) {
            return current.rot(VipsAngle.ANGLE_D270);
        }
        return current.rotate(deg);
    }

    private VImage applyQuality(VImage image, Quality quality) throws Exception {
        if (quality == null || quality == Quality.DEFAULT || quality == Quality.COLOR) {
            return image;
        }
        if (quality == Quality.GRAY) {
            return image.colourspace(VipsInterpretation.INTERPRETATION_B_W);
        }
        if (quality == Quality.BITONAL) {
            VImage gray = image.colourspace(VipsInterpretation.INTERPRETATION_B_W);
            return gray.relationalConst(VipsOperationRelational.OPERATION_RELATIONAL_MORE, List.of(128.0d));
        }
        return image;
    }
}

