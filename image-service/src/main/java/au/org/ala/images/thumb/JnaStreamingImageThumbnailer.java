package au.org.ala.images.thumb;

import au.org.ala.images.jna.InputStreamVipsSource;
import au.org.ala.images.jna.NativeLibraryDetector;
import au.org.ala.images.jna.OutputStreamVipsTarget;
import au.org.ala.images.jna.VipsLibrary;
import au.org.ala.images.util.ByteSinkFactory;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Experimental JNA-based thumbnailer that uses libvips directly via JNA.
 * This avoids process spawning overhead and can be more efficient for high-throughput scenarios.
 *
 * Automatically falls back to process-based approach if libvips is not available.
 */
public class JnaStreamingImageThumbnailer implements IImageThumbnailer {

    private static final Logger log = LoggerFactory.getLogger(JnaStreamingImageThumbnailer.class);

    private final VipsLibrary vips;
    private final IImageThumbnailer fallbackThumbnailer;

    public JnaStreamingImageThumbnailer(IImageThumbnailer fallbackThumbnailer) {
        this.fallbackThumbnailer = fallbackThumbnailer;
        this.vips = NativeLibraryDetector.getVipsLibrary();

        if (vips != null) {
            log.info("JnaStreamingImageThumbnailer initialized with native libvips");
        } else {
            log.info("JnaStreamingImageThumbnailer: libvips not available, will use fallback");
        }
    }

    @Override
    public List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
        // Fallback if libvips not available
        if (vips == null) {
            log.debug("Using fallback thumbnailer");
            return fallbackThumbnailer.generateThumbnails(imageBytes, byteSinkFactory, thumbDefs);
        }

        List<ThumbnailingResult> results = new ArrayList<>();

        for (ThumbDefinition thumbDef : thumbDefs) {
            try {
                ThumbnailingResult result = generateSingleThumbnail(imageBytes, byteSinkFactory, thumbDef);
                if (result != null) {
                    results.add(result);
                }
            } catch (Exception e) {
                log.error("Failed to generate thumbnail {}, trying fallback", thumbDef.getName(), e);
                try {
                    // Try fallback for this thumbnail
                    List<ThumbDefinition> singleDef = new ArrayList<>();
                    singleDef.add(thumbDef);
                    List<ThumbnailingResult> fallbackResults = fallbackThumbnailer.generateThumbnails(
                            imageBytes, byteSinkFactory, singleDef);
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

    private ThumbnailingResult generateSingleThumbnail(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, ThumbDefinition thumbDef) throws IOException {
        ByteSink destination = byteSinkFactory.getByteSinkForNames(thumbDef.getName());
        int size = thumbDef.getMaximumDimension();
        Color backgroundColor = thumbDef.getBackgroundColor();

        InputStreamVipsSource vipsSource = null;
        OutputStreamVipsTarget vipsTarget = null;
        Pointer inputImage = null;
        Pointer outputImage = null;

        try {
            // Create streaming source from ByteSource
            vipsSource = new InputStreamVipsSource(vips, imageBytes);

            // Load image from source - this streams the data without buffering entire image
            inputImage = vips.vips_image_new_from_source(vipsSource.getSource(), "", (Object) null);
            if (inputImage == null || inputImage == Pointer.NULL) {
                String error = vips.vips_error_buffer();
                vips.vips_error_clear();
                throw new IOException("Failed to load image from source with libvips: " + error);
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition());

            // Create thumbnail using vips_thumbnail_image
            PointerByReference outRef = new PointerByReference();

            int result = callVipsThumbnail(inputImage, outRef, thumbDef, size, backgroundColor);

            if (result != 0) {
                String error = vips.vips_error_buffer();
                vips.vips_error_clear();
                throw new IOException("Failed to create thumbnail with libvips: " + error);
            }

            outputImage = outRef.getValue();
            if (outputImage == null) {
                throw new IOException("vips_thumbnail_image returned null output");
            }

            // Save to target (streaming output)
            String suffix = thumbDef.getName().endsWith(".png") ? ".png" : ".jpg[Q=85]";
            OutputStream outputStream = destination.openStream();
            try {
                vipsTarget = new OutputStreamVipsTarget(vips, outputStream);
                result = vips.vips_image_write_to_target(outputImage, suffix, vipsTarget.getTarget(), (Object) null);

                if (result != 0) {
                    String error = vips.vips_error_buffer();
                    vips.vips_error_clear();
                    throw new IOException("Failed to write thumbnail to target: " + error);
                }
            } finally {
                if (vipsTarget != null) {
                    vipsTarget.close();
                }
                outputStream.close();
            }

            // Get actual dimensions from the output image
            int actualWidth = vips.vips_image_get_width(outputImage);
            int actualHeight = vips.vips_image_get_height(outputImage);

            return new ThumbnailingResult(actualWidth, actualHeight, thumbDef.isSquare(), thumbDef.getName());

        } finally {
            // Clean up vips objects
            if (outputImage != null && outputImage != Pointer.NULL) {
                vips.g_object_unref(outputImage);
            }
            if (inputImage != null && inputImage != Pointer.NULL) {
                vips.g_object_unref(inputImage);
            }
            if (vipsSource != null) {
                vipsSource.close();
            }
        }
    }

    /**
     * Call vips thumbnail operation with appropriate options.
     * This uses vips operations API which is complex, so we build options as strings.
     */
    private int callVipsThumbnail(Pointer inputImage, PointerByReference outRef, ThumbDefinition thumbDef, int size, Color backgroundColor) {
        // For simplicity, we'll call vips_thumbnail_image with size parameter
        // The varargs in JNA are tricky, so we need to be careful to use correct types (Integer, not String)
        if (thumbDef.isSquare() && thumbDef.isCentreCrop()) {
            // Centre crop to square - use height parameter and crop
            // VIPS_INTERESTING_CENTRE = 1
            return vips.vips_thumbnail_image(inputImage, outRef, size,
                    "height", size,
                    "crop", 1,
                    null);
        } else if (thumbDef.isSquare()) {
            // Fit within square with background
            return vips.vips_thumbnail_image(inputImage, outRef, size,
                    "height", size,
                    null);
        } else if (thumbDef.getWidth() != -1) {
            // Specific width
            return vips.vips_thumbnail_image(inputImage, outRef, thumbDef.getWidth(), (Object) null);
        } else {
            // Default: fit within size x size
            return vips.vips_thumbnail_image(inputImage, outRef, size, (Object) null);
        }
    }
}
