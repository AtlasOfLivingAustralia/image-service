package au.org.ala.images.thumb

import au.org.ala.images.jna.InputStreamVipsSource
import au.org.ala.images.jna.OutputStreamVipsTarget
import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.jna.VipsLibrary
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.ByteSource
import com.sun.jna.Pointer
import com.sun.jna.ptr.LongByReference
import com.sun.jna.ptr.PointerByReference
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.awt.Color

/**
 * Experimental JNA-based thumbnailer that uses libvips directly via JNA.
 * This avoids process spawning overhead and can be more efficient for high-throughput scenarios.
 *
 * Automatically falls back to process-based approach if libvips is not available.
 */
@Slf4j
@CompileStatic
class JnaStreamingImageThumbnailer implements IImageThumbnailer {

    private final VipsLibrary vips
    private final IImageThumbnailer fallbackThumbnailer

    JnaStreamingImageThumbnailer(IImageThumbnailer fallbackThumbnailer) {
        this.fallbackThumbnailer = fallbackThumbnailer
        this.vips = NativeLibraryDetector.getVipsLibrary()

        if (vips) {
            log.info("JnaStreamingImageThumbnailer initialized with native libvips")
        } else {
            log.info("JnaStreamingImageThumbnailer: libvips not available, will use fallback")
        }
    }

    @Override
    List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
        // Fallback if libvips not available
        if (!vips) {
            log.debug("Using fallback thumbnailer")
            return fallbackThumbnailer.generateThumbnails(imageBytes, byteSinkFactory, thumbDefs)
        }

        List<ThumbnailingResult> results = new ArrayList<>()

        for (ThumbDefinition thumbDef : thumbDefs) {
            try {
                ThumbnailingResult result = generateSingleThumbnail(imageBytes, byteSinkFactory, thumbDef)
                if (result) {
                    results.add(result)
                }
            } catch (Exception e) {
                log.error("Failed to generate thumbnail ${thumbDef.name}, trying fallback", e)
                try {
                    // Try fallback for this thumbnail
                    List<ThumbnailingResult> fallbackResults = fallbackThumbnailer.generateThumbnails(
                            imageBytes, byteSinkFactory, [thumbDef])
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

        InputStreamVipsSource vipsSource = null
        OutputStreamVipsTarget vipsTarget = null
        Pointer inputImage = null
        Pointer outputImage = null

        try {
            // Create streaming source from ByteSource
            vipsSource = new InputStreamVipsSource(vips, imageBytes)

            // Load image from source - this streams the data without buffering entire image
            inputImage = vips.vips_image_new_from_source(vipsSource.getSource(), "", null)
            if (inputImage == null || inputImage == Pointer.NULL) {
                String error = vips.vips_error_buffer()
                vips.vips_error_clear()
                throw new IOException("Failed to load image from source with libvips: ${error}")
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition())

            // Create thumbnail using vips_thumbnail_image
            PointerByReference outRef = new PointerByReference()

            int result = callVipsThumbnail(inputImage, outRef, thumbDef, size, backgroundColor)

            if (result != 0) {
                String error = vips.vips_error_buffer()
                vips.vips_error_clear()
                throw new IOException("Failed to create thumbnail with libvips: ${error}")
            }

            outputImage = outRef.value
            if (outputImage == null) {
                throw new IOException("vips_thumbnail_image returned null output")
            }

            // Save to target (streaming output)
            String suffix = thumbDef.name.endsWith('.png') ? '.png' : '.jpg[Q=85]'
            OutputStream outputStream = destination.openStream()
            try {
                vipsTarget = new OutputStreamVipsTarget(vips, outputStream)
                result = vips.vips_image_write_to_target(outputImage, suffix, vipsTarget.getTarget())

                if (result != 0) {
                    String error = vips.vips_error_buffer()
                    vips.vips_error_clear()
                    throw new IOException("Failed to write thumbnail to target: ${error}")
                }
            } finally {
                vipsTarget?.close()
                outputStream.close()
            }

            // Get actual dimensions from the output image
            int actualWidth = vips.vips_image_get_width(outputImage)
            int actualHeight = vips.vips_image_get_height(outputImage)

            return new ThumbnailingResult(actualWidth, actualHeight, thumbDef.square, thumbDef.name)

        } finally {
            // Clean up vips objects
            if (outputImage != null && outputImage != Pointer.NULL) {
                vips.g_object_unref(outputImage)
            }
            if (inputImage != null && inputImage != Pointer.NULL) {
                vips.g_object_unref(inputImage)
            }
            if (vipsSource != null) {
                vipsSource.close()
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
        if (thumbDef.square && thumbDef.centreCrop) {
            // Centre crop to square - use height parameter and crop
            // VIPS_INTERESTING_CENTRE = 1
            return vips.vips_thumbnail_image(inputImage, outRef.pointer, size,
                    "height", size,
                    "crop", 1,
                    null)
        } else if (thumbDef.square) {
            // Fit within square with background
            return vips.vips_thumbnail_image(inputImage, outRef.pointer, size,
                    "height", size,
                    null)
        } else if (thumbDef.width != -1) {
            // Specific width
            return vips.vips_thumbnail_image(inputImage, outRef.pointer, thumbDef.width, null)
        } else {
            // Default: fit within size x size
            return vips.vips_thumbnail_image(inputImage, outRef.pointer, size, null)
        }
    }

    private List<String> buildVipsThumbnailOptions(ThumbDefinition thumbDef, int size, Color backgroundColor) {
        List<String> options = []

        if (thumbDef.square) {
            options.add("height")
            options.add(size.toString())
        }

        if (thumbDef.square && thumbDef.centreCrop) {
            options.add("crop")
            options.add("centre")
        }

        if (backgroundColor && thumbDef.square) {
            // Background color not directly supported in thumbnail operation
            // Would need to use embed operation separately
        }

        return options
    }
}
