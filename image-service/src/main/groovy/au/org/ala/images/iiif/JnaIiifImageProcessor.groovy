package au.org.ala.images.iiif

import au.org.ala.images.jna.InputStreamVipsSource
import au.org.ala.images.jna.OutputStreamVipsTarget
import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.jna.VipsLibrary
import com.google.common.io.ByteSource
import com.sun.jna.Pointer
import com.sun.jna.ptr.PointerByReference
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.awt.Rectangle

/**
 * JNA-based IIIF image processor that uses libvips directly.
 */
@Slf4j
@CompileStatic
class JnaIiifImageProcessor implements IiifImageProcessor {

    private static final double EPS = 1e-10

    private final VipsLibrary vips
    private final IiifImageProcessor fallback

    JnaIiifImageProcessor(IiifImageProcessor fallback) {
        this(NativeLibraryDetector.getVipsLibrary(), fallback)
    }

    JnaIiifImageProcessor(VipsLibrary vips, IiifImageProcessor fallback) {
        this.vips = vips
        this.fallback = fallback
    }

    @Override
    IiifImageProcessor.Result process(ByteSource imageBytes, IiifImageProcessor.Region region, IiifImageProcessor.Size size, IiifImageProcessor.Rotation rotation, IiifImageProcessor.Quality quality, IiifImageProcessor.Format format, OutputStream out) throws IOException {
        if (vips == null) {
            return fallback.process(imageBytes, region, size, rotation, quality, format, out)
        }

        InputStreamVipsSource vipsSource = null
        OutputStreamVipsTarget vipsTarget = null
        Pointer image = null
        Pointer workingImage = null

        try {
            vipsSource = new InputStreamVipsSource(vips, imageBytes)

            // Load image
            image = vips.vips_image_new_from_source(vipsSource.getSource(), "", (Object) null)
            if (image == null || image == Pointer.NULL) {
                throw new IOException("Failed to load image: ${vips.vips_error_buffer()}")
            }

            int srcW = vips.vips_image_get_width(image)
            int srcH = vips.vips_image_get_height(image)

            // 1. Region
            workingImage = applyRegion(image, region, srcW, srcH)
            
            // 2. Size
            Pointer sizedImage = applySize(workingImage, size)
            if (sizedImage != workingImage) {
                if (workingImage != image) vips.g_object_unref(workingImage)
                workingImage = sizedImage
            }

            // 3. Rotation
            Pointer rotatedImage = applyRotation(workingImage, rotation)
            if (rotatedImage != workingImage) {
                if (workingImage != image) vips.g_object_unref(workingImage)
                workingImage = rotatedImage
            }

            // 4. Quality
            Pointer qualitiedImage = applyQuality(workingImage, quality)
            if (qualitiedImage != workingImage) {
                if (workingImage != image) vips.g_object_unref(workingImage)
                workingImage = qualitiedImage
            }

            // 5. Format & Output
            String suffix = "." + format.getFormatName()
            vipsTarget = new OutputStreamVipsTarget(vips, out)
            int result = vips.vips_image_write_to_target(workingImage, suffix, vipsTarget.getTarget())
            if (result != 0) {
                throw new IOException("Failed to write image: ${vips.vips_error_buffer()}")
            }

            return new IiifImageProcessor.Result(
                    vips.vips_image_get_width(workingImage),
                    vips.vips_image_get_height(workingImage),
                    format.getMimeType()
            )

        } catch (Exception e) {
            if (vipsTarget != null && vipsTarget.getBytesWritten() > 0) {
                log.error("JNA IIIF processing failed after writing {} bytes, cannot fallback", vipsTarget.getBytesWritten(), e)
                throw new IOException("Failed to write image and partial bytes already written to output", e)
            }
            log.error("JNA IIIF processing failed, falling back", e)
            return fallback.process(imageBytes, region, size, rotation, quality, format, out)
        } finally {
            if (workingImage != null && workingImage != image) {
                vips.g_object_unref(workingImage)
            }
            if (image != null) {
                vips.g_object_unref(image)
            }
            vipsSource?.close()
            vipsTarget?.close()
        }
    }

    private Pointer applyRegion(Pointer image, IiifImageProcessor.Region region, int srcW, int srcH) {
        if (region == null || region.type == IiifImageProcessor.Region.Type.FULL) {
            return image
        }

        int x, y, w, h
        if (region.type == IiifImageProcessor.Region.Type.SQUARE) {
            int side = Math.min(srcW, srcH)
            x = (int) ((srcW - side) / 2)
            y = (int) ((srcH - side) / 2)
            w = side
            h = side
        } else if (region.type == IiifImageProcessor.Region.Type.ASPECT) {
            double arW = (double) region.w
            double arH = (double) region.h
            double k = Math.min((double) srcW / arW, (double) srcH / arH)
            w = (int) Math.floor(k * arW)
            h = (int) Math.floor(k * arH)
            x = (int) ((srcW - w) / 2)
            y = (int) ((srcH - h) / 2)
        } else if (region.isPercent) {
            x = (int) Math.round(srcW * region.x / 100.0)
            y = (int) Math.round(srcH * region.y / 100.0)
            w = (int) Math.round(srcW * region.w / 100.0)
            h = (int) Math.round(srcH * region.h / 100.0)
        } else {
            x = (int) Math.round(region.x)
            y = (int) Math.round(region.y)
            w = (int) Math.round(region.w)
            h = (int) Math.round(region.h)
        }

        // clamp
        x = Math.max(0, Math.min(x, srcW - 1))
        y = Math.max(0, Math.min(y, srcH - 1))
        w = Math.max(1, Math.min(w, srcW - x))
        h = Math.max(1, Math.min(h, srcH - y))

        PointerByReference out = new PointerByReference()
        int result = vips.vips_crop(image, out, x, y, w, h, (Object) null)
        if (result != 0) {
            throw new RuntimeException("vips_crop failed: " + vips.vips_error_buffer())
        }
        return out.getValue()
    }

    private Pointer applySize(Pointer image, IiifImageProcessor.Size size) {
        if (size == null || size.type == IiifImageProcessor.Size.Type.MAX) {
            return image
        }

        int srcW = vips.vips_image_get_width(image)
        int srcH = vips.vips_image_get_height(image)
        double scaleW = 1.0
        double scaleH = 1.0

        switch (size.type) {
            case IiifImageProcessor.Size.Type.WIDTH_ONLY:
                scaleW = size.w / (double) srcW
                scaleH = scaleW
                break
            case IiifImageProcessor.Size.Type.HEIGHT_ONLY:
                scaleH = size.h / (double) srcH
                scaleW = scaleH
                break
            case IiifImageProcessor.Size.Type.PERCENT:
                scaleW = size.percent / 100.0
                scaleH = scaleW
                break
            case IiifImageProcessor.Size.Type.EXACT:
                scaleW = size.w / (double) srcW
                scaleH = size.h / (double) srcH
                break
            case IiifImageProcessor.Size.Type.BEST_FIT:
                scaleW = Math.min(size.w / (double) srcW, size.h / (double) srcH)
                scaleH = scaleW
                break
        }

        if (!size.upscaling) {
            scaleW = Math.min(1.0d, (double) scaleW)
            scaleH = Math.min(1.0d, (double) scaleH)
        }

        if (Math.abs(scaleW - 1.0d) < EPS && Math.abs(scaleH - 1.0d) < EPS) {
            return image
        }

        PointerByReference out = new PointerByReference()
        int result
        if (Math.abs(scaleW - scaleH) < EPS) {
            result = vips.vips_resize(image, out, (double) scaleW, (Object) null)
        } else {
            // For now, let's just do proportional to stay simple, or use vips_resize twice? No.
            result = vips.vips_resize(image, out, (double) scaleW, "vscale", (double) scaleH, (Object) null)
        }

        if (result != 0) {
            throw new RuntimeException("vips_resize failed: " + vips.vips_error_buffer())
        }
        return out.getValue()
    }

    private Pointer applyRotation(Pointer image, IiifImageProcessor.Rotation rotation) {
        if (rotation == null) {
            return image
        }
        double deg = ((rotation.degrees % 360.0) + 360.0) % 360.0
        if (deg == 0.0 && !rotation.mirror) {
            return image
        }

        Pointer current = image

        // 1. Mirroring
        if (rotation.mirror) {
            PointerByReference out = new PointerByReference()
            // VIPS_DIRECTION_HORIZONTAL = 0
            int result = vips.vips_flip(current, out, 0, (Object) null)
            if (result != 0) {
                throw new RuntimeException("vips_flip failed: " + vips.vips_error_buffer())
            }
            if (current != image) vips.g_object_unref(current)
            current = out.getValue()
        }

        // 2. Rotation
        if (deg != 0) {
            PointerByReference out = new PointerByReference()
            int result
            if (Math.abs((double) (deg - 90.0)) < EPS) {
                result = vips.vips_rot(current, out, 1, (Object) null)
            } else if (Math.abs((double) (deg - 180.0)) < EPS) {
                result = vips.vips_rot(current, out, 2, (Object) null)
            } else if (Math.abs((double) (deg - 270.0)) < EPS) {
                result = vips.vips_rot(current, out, 3, (Object) null)
            } else {
                // Arbitrary rotation - use vips_rotate which may be slower but is needed for non-right-angle rotations
                result = vips.vips_rotate(current, out, deg, (Object) null)
            }

            if (result != 0) {
                throw new RuntimeException("Native rotation failed or unsupported for ${deg} degrees: ${vips.vips_error_buffer()}")
            }
            if (current != image) vips.g_object_unref(current)
            current = out.getValue()
        }

        return current
    }

    private Pointer applyQuality(Pointer image, IiifImageProcessor.Quality quality) {
        if (quality == null || quality == IiifImageProcessor.Quality.DEFAULT || quality == IiifImageProcessor.Quality.COLOR) {
            return image
        }

        if (quality == IiifImageProcessor.Quality.GRAY) {
            PointerByReference out = new PointerByReference()
            // VIPS_INTERPRETATION_B_W = 2
            int result = vips.vips_colourspace(image, out, 2, (Object) null)
            if (result != 0) {
                throw new RuntimeException("vips_colourspace failed: " + vips.vips_error_buffer())
            }
            return out.getValue()
        } else if (quality == IiifImageProcessor.Quality.BITONAL) {
            PointerByReference out = new PointerByReference()
            // Convert to gray first then threshold
            Pointer gray = null
            PointerByReference grayRef = new PointerByReference()
            if (vips.vips_colourspace(image, grayRef, 2, (Object) null) != 0) {
                throw new RuntimeException("vips_colourspace failed (for BITONAL): " + vips.vips_error_buffer())
            }
            gray = grayRef.getValue()
            // VIPS_OPERATION_RELATIONAL_MORE = 4
            if (vips.vips_relational_const(gray, out, 4, [128.0] as double[], (Object) null) != 0) {
                vips.g_object_unref(gray)
                throw new RuntimeException("vips_relational_const failed (for BITONAL): " + vips.vips_error_buffer())
            }
            vips.g_object_unref(gray)
            return out.getValue()
        }

        return image
    }
}
