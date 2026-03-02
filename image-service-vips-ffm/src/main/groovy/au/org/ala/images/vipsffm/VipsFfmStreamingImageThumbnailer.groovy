package au.org.ala.images.vipsffm

import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.ByteSource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import vips.ffm.VipsImage
import vips.ffm.VipsSource
import vips.ffm.VipsTarget

import java.awt.Color
import java.lang.foreign.Arena

/**
 * Alternate FFM-based thumbnailer that uses the lopcode/vips-ffm library.
 */
@Slf4j
@CompileStatic
class VipsFfmStreamingImageThumbnailer implements IImageThumbnailer {

    private final IImageThumbnailer fallbackThumbnailer

    VipsFfmStreamingImageThumbnailer(IImageThumbnailer fallbackThumbnailer) {
        this.fallbackThumbnailer = fallbackThumbnailer
        log.info("VipsFfmStreamingImageThumbnailer initialized (lopcode/vips-ffm)")
    }

    @Override
    List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
        List<ThumbnailingResult> results = new ArrayList<>()

        for (ThumbDefinition thumbDef : thumbDefs) {
            try {
                ThumbnailingResult result = generateSingleThumbnail(imageBytes, byteSinkFactory, thumbDef)
                if (result) {
                    results.add(result)
                }
            } catch (Exception e) {
                log.error("Failed to generate thumbnail ${thumbDef.name} using vips-ffm, trying fallback", e)
                try {
                    List<ThumbnailingResult> fallbackResults = fallbackThumbnailer.generateThumbnails(imageBytes, byteSinkFactory, [thumbDef])
                    if (fallbackResults) {
                        results.addAll(fallbackResults)
                    }
                } catch (Exception fallbackError) {
                    log.error("Fallback also failed for ${thumbDef.name}", fallbackError)
                }
            }
        }

        return results
    }

    private ThumbnailingResult generateSingleThumbnail(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, ThumbDefinition thumbDef) throws IOException {
        ByteSink destination = byteSinkFactory.getByteSinkForNames(thumbDef.name)
        int size = thumbDef.maximumDimension
        Color backgroundColor = thumbDef.backgroundColor

        log.debug("Generating single thumbnail with vips-ffm: {}", thumbDef.name)

        try (var arena = Arena.ofConfined()) {
            try (InputStream inputStream = imageBytes.openStream()) {
                VipsSource source = VipsSource.newFromStream(arena, inputStream)
                VipsImage image = VipsImage.newFromSource(source, "")

                // Call thumbnail operation
                VipsImage thumb = callVipsThumbnail(image, thumbDef, size, backgroundColor)

                // Save to target (streaming output)
                String suffix = thumbDef.name.endsWith('.png') ? '.png' : '.jpg[Q=85]'
                try (OutputStream outputStream = destination.openStream()) {
                    VipsTarget target = VipsTarget.newToStream(arena, outputStream)
                    thumb.writeToTarget(suffix, target)
                }

                // Get actual dimensions
                int actualWidth = thumb.getWidth()
                int actualHeight = thumb.getHeight()

                return new ThumbnailingResult(actualWidth, actualHeight, thumbDef.square, thumbDef.name)
            }
        } catch (Exception e) {
            throw new IOException("vips-ffm thumbnailing failed", e)
        }
    }

    /**
     * Call vips thumbnail operation with appropriate options.
     */
    private VipsImage callVipsThumbnail(VipsImage inputImage, ThumbDefinition thumbDef, int size, Color backgroundColor) {
        // Based on JNA implementation, we try to match the logic
        if (thumbDef.square && thumbDef.centreCrop) {
            // Centre crop to square
            return inputImage.thumbnail(size, "height", size, "crop", "centre")
        } else if (thumbDef.square) {
            // Fit within square
            return inputImage.thumbnail(size, "height", size)
        } else if (thumbDef.width != -1) {
            // Specific width
            return inputImage.thumbnail(thumbDef.width)
        } else {
            // Default: fit within size x size
            return inputImage.thumbnail(size)
        }
    }

}
