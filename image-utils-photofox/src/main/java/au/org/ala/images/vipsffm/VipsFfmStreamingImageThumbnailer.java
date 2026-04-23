package au.org.ala.images.vipsffm;

import app.photofox.vipsffm.VImage;
import app.photofox.vipsffm.VipsOption;
import au.org.ala.images.thumb.IImageThumbnailer;
import au.org.ala.images.thumb.ThumbDefinition;
import au.org.ala.images.thumb.ThumbnailingResult;
import au.org.ala.images.util.ByteSinkFactory;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.awt.Color;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alternate FFM-based thumbnailer that uses the lopcode/vips-ffm library.
 */
public class VipsFfmStreamingImageThumbnailer implements IImageThumbnailer {

    private static final Logger log = LoggerFactory.getLogger(VipsFfmStreamingImageThumbnailer.class);

    private final IImageThumbnailer fallbackThumbnailer;

    public VipsFfmStreamingImageThumbnailer(IImageThumbnailer fallbackThumbnailer) {
        this.fallbackThumbnailer = fallbackThumbnailer;
        log.info("VipsFfmStreamingImageThumbnailer initialized (lopcode/vips-ffm)");
    }

    @Override
    public List<ThumbnailingResult> generateThumbnails(
        ByteSource imageBytes,
        ByteSinkFactory byteSinkFactory,
        List<ThumbDefinition> thumbDefs
    ) throws IOException {
        List<ThumbnailingResult> results = new ArrayList<>();

        for (ThumbDefinition thumbDef : thumbDefs) {
            try {
                ThumbnailingResult result = generateSingleThumbnail(imageBytes, byteSinkFactory, thumbDef);
                if (result != null) {
                    results.add(result);
                }
            } catch (Exception e) {
                log.error("Failed to generate thumbnail {} using vips-ffm, trying fallback", thumbDef.getName(), e);
                try {
                    List<ThumbnailingResult> fallbackResults = fallbackThumbnailer.generateThumbnails(
                        imageBytes,
                        byteSinkFactory,
                        List.of(thumbDef)
                    );
                    if (fallbackResults != null) {
                        results.addAll(fallbackResults);
                    }
                } catch (Exception fallbackError) {
                    log.error("Fallback also failed for {}", thumbDef.getName(), fallbackError);
                }
            }
        }

        return results;
    }

    private ThumbnailingResult generateSingleThumbnail(
        ByteSource imageBytes,
        ByteSinkFactory byteSinkFactory,
        ThumbDefinition thumbDef
    ) throws IOException {
        ByteSink destination = byteSinkFactory.getByteSinkForNames(thumbDef.getName());
        int size = thumbDef.getMaximumDimension();
        Color backgroundColor = thumbDef.getBackgroundColor();

        log.debug("Generating single thumbnail with vips-ffm: {}", thumbDef.getName());

        try (Arena arena = Arena.ofConfined(); InputStream inputStream = imageBytes.openStream()) {
            VImage image = VImage.newFromStream(arena, inputStream);

            // Call thumbnail operation.
            VImage thumb = callVipsThumbnail(image, thumbDef, size, backgroundColor);

            // Save to target (streaming output).
            String suffix = thumbDef.getName().endsWith(".png") ? ".png" : ".jpg[Q=85]";
            try (OutputStream outputStream = destination.openStream()) {
                thumb.writeToStream(outputStream, suffix);
            }

            // Get actual dimensions.
            int actualWidth = thumb.getWidth();
            int actualHeight = thumb.getHeight();

            return new ThumbnailingResult(actualWidth, actualHeight, thumbDef.isSquare(), thumbDef.getName());
        } catch (Exception e) {
            throw new IOException("vips-ffm thumbnailing failed", e);
        }
    }

    /**
     * Call vips thumbnail operation with appropriate options.
     */
    private VImage callVipsThumbnail(VImage inputImage, ThumbDefinition thumbDef, int size, Color backgroundColor) {
        // Based on JNA implementation, we try to match the logic.
        if (thumbDef.isSquare() && thumbDef.isCentreCrop()) {
            // Centre crop to square.
            return inputImage.thumbnailImage(size, VipsOption.Int("height", size), VipsOption.String("crop", "centre"));
        } else if (thumbDef.isSquare()) {
            // Fit within square.
            return inputImage.thumbnailImage(size, VipsOption.Int("height", size));
        } else if (thumbDef.getWidth() != -1) {
            // Specific width.
            return inputImage.thumbnailImage(thumbDef.getWidth());
        } else {
            // Default: fit within size x size.
            return inputImage.thumbnailImage(size);
        }
    }
}


