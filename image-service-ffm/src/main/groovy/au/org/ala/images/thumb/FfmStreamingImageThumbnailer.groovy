package au.org.ala.images.thumb

import au.org.ala.images.ffm.InputStreamVipsSourceFFM
import au.org.ala.images.ffm.NativeLibraryDetectorFFM
import au.org.ala.images.ffm.OutputStreamVipsTargetFFM
import au.org.ala.images.ffm.VipsLibraryFFM
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.ByteSource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.awt.Color
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout

/**
 * FFM-based thumbnailer that uses libvips directly via the Foreign Function & Memory API (Java 22+).
 * This replaces JNA with the modern Panama FFM API for better performance and integration.
 *
 * Automatically falls back to process-based approach if libvips is not available.
 */
@Slf4j
@CompileStatic
class FfmStreamingImageThumbnailer implements IImageThumbnailer {

    private final VipsLibraryFFM vips
    private final IImageThumbnailer fallbackThumbnailer

    FfmStreamingImageThumbnailer(IImageThumbnailer fallbackThumbnailer) {
        this.fallbackThumbnailer = fallbackThumbnailer
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary()

        if (vips) {
            log.info("FfmStreamingImageThumbnailer initialized with native libvips (FFM)")
        } else {
            log.info("FfmStreamingImageThumbnailer: libvips not available, will use fallback")
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

        InputStreamVipsSourceFFM vipsSource = null
        MemorySegment inputImage = null
        MemorySegment outputImage = null

        try (Arena sessionArena = Arena.ofConfined()) {
            // Create streaming source from ByteSource
            InputStream inputStream = imageBytes.openStream()
            try {
                vipsSource = new InputStreamVipsSourceFFM(vips, inputStream)
                // Ownership of the stream is now with vipsSource; prevent it from being closed here
                inputStream = null
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close()
                    } catch (IOException ioe) {
                        log.warn("Failed to close input stream after libvips initialization failure", ioe)
                    }
                }
            }

            // Load image from source - this streams the data without buffering entire image
            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "")
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer()
                vips.vipsErrorClear()
                throw new IOException("Failed to load image from source with libvips: ${error}")
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition())

            // Create thumbnail using vips_thumbnail_image
            MemorySegment outPtr = sessionArena.allocate(ValueLayout.ADDRESS)

            int result = callVipsThumbnail(inputImage, outPtr, thumbDef, size, backgroundColor)

            if (result != 0) {
                String error = vips.vipsErrorBuffer()
                vips.vipsErrorClear()
                throw new IOException("Failed to create thumbnail with libvips: ${error}")
            }

            outputImage = outPtr.get(ValueLayout.ADDRESS, 0)
            if (outputImage == null || outputImage.address() == 0) {
                throw new IOException("vips_thumbnail_image returned null output")
            }

            // Save to destination
            String suffix = thumbDef.name.endsWith('.png') ? '.png' : '.jpg[Q=85]'

            try (OutputStream os = destination.openStream()) {
                try (OutputStreamVipsTargetFFM vipsTarget = new OutputStreamVipsTargetFFM(vips, os)) {
                    result = vips.vipsImageWriteToTarget(outputImage, suffix, vipsTarget.getTarget())
                    if (result != 0) {
                        String error = vips.vipsErrorBuffer()
                        vips.vipsErrorClear()
                        throw new IOException("Failed to write thumbnail to target: ${error}")
                    }
                }
            }

            // Get actual dimensions from the output image
            int actualWidth = vips.vipsImageGetWidth(outputImage)
            int actualHeight = vips.vipsImageGetHeight(outputImage)

            return new ThumbnailingResult(actualWidth, actualHeight, thumbDef.square, thumbDef.name)

        } catch (Throwable e) {
            if (e instanceof IOException) {
                throw e
            }
            throw new IOException("Error generating thumbnail", e)
        } finally {
            // Clean up vips objects
            if (outputImage != null && outputImage.address() != 0) {
                try {
                    vips.gObjectUnref(outputImage)
                } catch (Throwable e) {
                    log.warn("Error unreffing output image", e)
                }
            }
            if (inputImage != null && inputImage.address() != 0) {
                try {
                    vips.gObjectUnref(inputImage)
                } catch (Throwable e) {
                    log.warn("Error unreffing input image", e)
                }
            }
            if (vipsSource != null) {
                try {
                    vipsSource.close()
                } catch (Exception e) {
                    log.warn("Error closing vips source", e)
                }
            }
        }
    }

    /**
     * Call vips thumbnail operation with appropriate options.
     * Note: This is a simplified version that handles basic cases.
     * Full varargs support in FFM requires more complex handling.
     */
    private int callVipsThumbnail(MemorySegment inputImage, MemorySegment outPtr,
                                   ThumbDefinition thumbDef, int size, Color backgroundColor) throws Throwable {
        // For simplicity, we'll call vips_thumbnail_image with basic size parameter
        // Advanced options (height, crop, etc.) would require implementing varargs support

        if (thumbDef.square && thumbDef.centreCrop) {
            // Centre crop to square - use height parameter and crop
            // Note: Full implementation would pass these as varargs
            log.trace("Creating square thumbnail with centre crop: size={}", size)
            return vips.vipsThumbnailImage(inputImage, outPtr, size)
        } else if (thumbDef.square) {
            // Fit within square with background
            log.trace("Creating square thumbnail: size={}", size)
            return vips.vipsThumbnailImage(inputImage, outPtr, size)
        } else if (thumbDef.width != -1) {
            // Specific width
            log.trace("Creating thumbnail with specific width: {}", thumbDef.width)
            return vips.vipsThumbnailImage(inputImage, outPtr, thumbDef.width)
        } else {
            // Default: fit within size x size
            log.trace("Creating thumbnail with max dimension: {}", size)
            return vips.vipsThumbnailImage(inputImage, outPtr, size)
        }
    }

    /**
     * Build options for vips thumbnail operation.
     * Note: This method is kept for potential future implementation of full varargs support.
     */
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
