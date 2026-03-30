package au.org.ala.images.ffm

import au.org.ala.images.iiif.IiifImageProcessor
import com.google.common.io.ByteSource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout

/**
 * FFM-based IIIF image processor that uses libvips via Panama API.
 */
@Slf4j
@CompileStatic
class FfmIiifImageProcessor implements IiifImageProcessor {

    private final VipsLibraryFFM vips
    private final IiifImageProcessor fallback

    FfmIiifImageProcessor(IiifImageProcessor fallback) {
        this.fallback = fallback
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary()
    }

    @Override
    IiifImageProcessor.Result process(ByteSource imageBytes, IiifImageProcessor.Region region, IiifImageProcessor.Size size, IiifImageProcessor.Rotation rotation, IiifImageProcessor.Quality quality, IiifImageProcessor.Format format, OutputStream out) throws IOException {
        if (vips == null) {
            return fallback.process(imageBytes, region, size, rotation, quality, format, out)
        }

        InputStream inputStream = null
        InputStreamVipsSourceFFM vipsSource = null
        OutputStreamVipsTargetFFM vipsTarget = null
        MemorySegment image = MemorySegment.NULL
        MemorySegment workingImage = MemorySegment.NULL

        try {
            inputStream = imageBytes.openStream()
            vipsSource = new InputStreamVipsSourceFFM(vips, inputStream)

            // Load image
            image = vips.vipsImageNewFromSource(vipsSource.getSource(), "")
            if (image == null || image.address() == 0) {
                throw new IOException("Failed to load image: " + vips.vipsErrorBuffer())
            }

            int srcW = vips.vipsImageGetWidth(image)
            int srcH = vips.vipsImageGetHeight(image)

            // 1. Region
            workingImage = applyRegion(image, region, srcW, srcH)

            // 2. Size
            MemorySegment sizedImage = applySize(workingImage, size)
            if (sizedImage != workingImage) {
                if (workingImage != image) vips.gObjectUnref(workingImage)
                workingImage = sizedImage
            }

            // 3. Rotation
            MemorySegment rotatedImage = applyRotation(workingImage, rotation)
            if (rotatedImage != workingImage) {
                if (workingImage != image) vips.gObjectUnref(workingImage)
                workingImage = rotatedImage
            }

            // 4. Quality
            MemorySegment qualitiedImage = applyQuality(workingImage, quality)
            if (qualitiedImage != workingImage) {
                if (workingImage != image) vips.gObjectUnref(workingImage)
                workingImage = qualitiedImage
            }

            // 5. Format & Output
            String suffix = "." + format.getFormatName()
            vipsTarget = new OutputStreamVipsTargetFFM(vips, out)
            int result = vips.vipsImageWriteToTarget(workingImage, suffix, vipsTarget.getTarget())
            if (result != 0) {
                throw new IOException("Failed to write image: " + vips.vipsErrorBuffer())
            }

            return new IiifImageProcessor.Result(
                    vips.vipsImageGetWidth(workingImage),
                    vips.vipsImageGetHeight(workingImage),
                    format.getMimeType()
            )

        } catch (Throwable e) {
            log.error("FFM IIIF processing failed, falling back", e)
            return fallback.process(imageBytes, region, size, rotation, quality, format, out)
        } finally {
            try {
                if (workingImage != null && workingImage != image && workingImage.address() != 0) {
                    vips.gObjectUnref(workingImage)
                }
                if (image != null && image.address() != 0) {
                    vips.gObjectUnref(image)
                }
            } catch (Throwable ignored) {}
            vipsSource?.close()
            vipsTarget?.close()
            inputStream?.close()
        }
    }

    private MemorySegment applyRegion(MemorySegment image, IiifImageProcessor.Region region, int srcW, int srcH) throws Throwable {
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
        x = Math.max(0, Math.min(x, srcW))
        y = Math.max(0, Math.min(y, srcH))
        w = Math.max(1, Math.min(w, srcW - x))
        h = Math.max(1, Math.min(h, srcH - y))

        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment outPtr = tempArena.allocate(ValueLayout.ADDRESS)
            int result = vips.vipsCrop(image, outPtr, x, y, w, h)
            if (result != 0) {
                log.warn("vips_crop failed: {}", vips.vipsErrorBuffer())
                return image
            }
            return outPtr.get(ValueLayout.ADDRESS, 0)
        }
    }

    private MemorySegment applySize(MemorySegment image, IiifImageProcessor.Size size) throws Throwable {
        if (size == null || size.type == IiifImageProcessor.Size.Type.MAX) {
            return image
        }

        int srcW = vips.vipsImageGetWidth(image)
        int srcH = vips.vipsImageGetHeight(image)
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
            scaleW = Math.min(1.0d, scaleW)
            scaleH = Math.min(1.0d, scaleH)
        }

        if (scaleW == 1.0 && scaleH == 1.0) {
            return image
        }

        // FFM implementation of vipsResize currently only supports proportional scaling 
        // because we only mapped one MethodHandle signature.
        // For IIIF Level 2 this is usually fine.
        try (Arena tempArena = Arena.ofConfined()) {
            MemorySegment outPtr = tempArena.allocate(ValueLayout.ADDRESS)
            int result = vips.vipsResize(image, outPtr, scaleW)
            if (result != 0) {
                log.warn("vips_resize failed: {}", vips.vipsErrorBuffer())
                return image
            }
            return outPtr.get(ValueLayout.ADDRESS, 0)
        }
    }

    private MemorySegment applyRotation(MemorySegment image, IiifImageProcessor.Rotation rotation) throws Throwable {
        if (rotation == null || (rotation.degrees % 360.0 == 0.0 && !rotation.mirror)) {
            return image
        }

        MemorySegment current = image

        // 1. Mirroring
        if (rotation.mirror) {
            try (Arena tempArena = Arena.ofConfined()) {
                MemorySegment outPtr = tempArena.allocate(ValueLayout.ADDRESS)
                // VIPS_DIRECTION_HORIZONTAL = 0
                int result = vips.vipsFlip(current, outPtr, 0)
                if (result == 0) {
                    if (current != image) vips.gObjectUnref(current)
                    current = outPtr.get(ValueLayout.ADDRESS, 0)
                }
            }
        }

        // 2. Rotation
        double deg = rotation.degrees % 360.0
        if (deg != 0) {
            try (Arena tempArena = Arena.ofConfined()) {
                MemorySegment outPtr = tempArena.allocate(ValueLayout.ADDRESS)
                int result = -1
                if (deg == 90) {
                    result = vips.vipsRot(current, outPtr, 1)
                } else if (deg == 180) {
                    result = vips.vipsRot(current, outPtr, 2)
                } else if (deg == 270) {
                    result = vips.vipsRot(current, outPtr, 3)
                }

                if (result == 0) {
                    if (current != image) vips.gObjectUnref(current)
                    current = outPtr.get(ValueLayout.ADDRESS, 0)
                } else {
                    log.warn("Native FFM rotation failed or unsupported for {} degrees", deg)
                }
            }
        }

        return current
    }

    private MemorySegment applyQuality(MemorySegment image, IiifImageProcessor.Quality quality) throws Throwable {
        if (quality == null || quality == IiifImageProcessor.Quality.DEFAULT || quality == IiifImageProcessor.Quality.COLOR) {
            return image
        }

        if (quality == IiifImageProcessor.Quality.GRAY) {
            try (Arena tempArena = Arena.ofConfined()) {
                MemorySegment outPtr = tempArena.allocate(ValueLayout.ADDRESS)
                // VIPS_INTERPRETATION_B_W = 2
                int result = vips.vipsColourspace(image, outPtr, 2)
                if (result == 0) {
                    return outPtr.get(ValueLayout.ADDRESS, 0)
                }
            }
        } else if (quality == IiifImageProcessor.Quality.BITONAL) {
            try (Arena tempArena = Arena.ofConfined()) {
                MemorySegment grayPtr = tempArena.allocate(ValueLayout.ADDRESS)
                if (vips.vipsColourspace(image, grayPtr, 2) == 0) {
                    MemorySegment gray = grayPtr.get(ValueLayout.ADDRESS, 0)
                    MemorySegment outPtr = tempArena.allocate(ValueLayout.ADDRESS)
                    // VIPS_OPERATION_RELATIONAL_MORE = 4
                    if (vips.vipsRelationalConst(gray, outPtr, 4, [128.0d] as double[]) == 0) {
                        vips.gObjectUnref(gray)
                        return outPtr.get(ValueLayout.ADDRESS, 0)
                    }
                    vips.gObjectUnref(gray)
                }
            }
        }

        return image
    }
}
