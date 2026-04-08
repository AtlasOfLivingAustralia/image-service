package au.org.ala.images.thumb;

import au.org.ala.images.ffm.InputStreamVipsSourceFFM;
import au.org.ala.images.ffm.NativeLibraryDetectorFFM;
import au.org.ala.images.ffm.OutputStreamVipsTargetFFM;
import au.org.ala.images.ffm.VipsLibraryFFM;
import au.org.ala.images.util.ByteSinkFactory;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

/**
 * FFM-based thumbnailer that uses libvips directly via the Foreign Function & Memory API.
 */
public class FfmStreamingImageThumbnailer implements IImageThumbnailer {

    private static final Logger log = LoggerFactory.getLogger(FfmStreamingImageThumbnailer.class);

    private final VipsLibraryFFM vips;
    private final IImageThumbnailer fallbackThumbnailer;

    public FfmStreamingImageThumbnailer(IImageThumbnailer fallbackThumbnailer) {
        this.fallbackThumbnailer = fallbackThumbnailer;
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary();

        if (vips != null) {
            log.info("FfmStreamingImageThumbnailer initialized with native libvips (FFM)");
        } else {
            log.info("FfmStreamingImageThumbnailer: libvips not available, will use fallback");
        }
    }

    @Override
    public List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
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
                log.error("Failed to generate thumbnail " + thumbDef.getName() + ", trying fallback", e);
                try {
                    List<ThumbDefinition> singleList = new ArrayList<>();
                    singleList.add(thumbDef);
                    List<ThumbnailingResult> fallbackResults = fallbackThumbnailer.generateThumbnails(imageBytes, byteSinkFactory, singleList);
                    if (fallbackResults != null) {
                        results.addAll(fallbackResults);
                    }
                } catch (Exception fallbackError) {
                    log.error("Fallback also failed for " + thumbDef.getName(), fallbackError);
                }
            }
        }

        return results;
    }

    private ThumbnailingResult generateSingleThumbnail(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, ThumbDefinition thumbDef) throws IOException {
        ByteSink destination = byteSinkFactory.getByteSinkForNames(thumbDef.getName());
        int size = thumbDef.getMaximumDimension();
        Color backgroundColor = thumbDef.getBackgroundColor();

        InputStreamVipsSourceFFM vipsSource = null;
        MemorySegment inputImage = null;

        try (Arena sessionArena = Arena.ofConfined()) {
            vipsSource = new InputStreamVipsSourceFFM(vips, imageBytes);

            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "");
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to load image from source with libvips: " + error);
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition());

            MemorySegment outPtr = sessionArena.allocate(ValueLayout.ADDRESS);
            int result = callVipsThumbnail(inputImage, outPtr, thumbDef, size, backgroundColor);

            if (result != 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("libvips thumbnail operation failed: " + error);
            }

            MemorySegment outputImage = outPtr.get(ValueLayout.ADDRESS, 0);

            try {
                String suffix = thumbDef.getName().endsWith(".png") ? ".png" : ".jpg[Q=85]";
                try (OutputStreamVipsTargetFFM vipsTarget = new OutputStreamVipsTargetFFM(vips, destination.openStream())) {
                    int writeResult = vips.vipsImageWriteToTarget(outputImage, suffix, vipsTarget.getTarget());
                    if (writeResult != 0) {
                        String error = vips.vipsErrorBuffer();
                        vips.vipsErrorClear();
                        throw new IOException("libvips write to target failed: " + error);
                    }
                }

                int actualWidth = vips.vipsImageGetWidth(outputImage);
                int actualHeight = vips.vipsImageGetHeight(outputImage);

                return new ThumbnailingResult(actualWidth, actualHeight, thumbDef.isSquare(), thumbDef.getName());
            } finally {
                if (outputImage != null && outputImage.address() != 0) {
                    vips.gObjectUnref(outputImage);
                }
            }
        } catch (Throwable e) {
            if (e instanceof IOException) throw (IOException) e;
            throw new IOException("FFM thumbnailing failed", e);
        } finally {
            if (inputImage != null && inputImage.address() != 0) {
                try {
                    vips.gObjectUnref(inputImage);
                } catch (Throwable e) {
                    log.warn("Error unreffing input image", e);
                }
            }
            if (vipsSource != null) {
                vipsSource.close();
            }
        }
    }

    private int callVipsThumbnail(MemorySegment inputImage, MemorySegment outPtr,
                                   ThumbDefinition thumbDef, int size, Color backgroundColor) throws Throwable {
        if (thumbDef.isSquare() && thumbDef.isCentreCrop()) {
            log.trace("Creating square thumbnail with centre crop: size={}", size);
            return vips.vipsThumbnailImage(inputImage, outPtr, size,
                "height", size,
                "crop", 1);
        } else if (thumbDef.isSquare()) {
            log.trace("Creating square thumbnail: size={}", size);
            return vips.vipsThumbnailImage(inputImage, outPtr, size,
                "height", size);
        } else if (thumbDef.getWidth() != -1) {
            log.trace("Creating thumbnail with specific width: {}", thumbDef.getWidth());
            return vips.vipsThumbnailImage(inputImage, outPtr, thumbDef.getWidth());
        } else {
            log.trace("Creating thumbnail with max dimension: {}", size);
            return vips.vipsThumbnailImage(inputImage, outPtr, size);
        }
    }
}
