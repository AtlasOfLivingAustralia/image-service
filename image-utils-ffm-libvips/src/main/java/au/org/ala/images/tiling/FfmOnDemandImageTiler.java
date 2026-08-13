package au.org.ala.images.tiling;

import au.org.ala.images.ffm.InputStreamVipsSourceFFM;
import au.org.ala.images.ffm.NativeLibraryDetectorFFM;
import au.org.ala.images.ffm.OutputStreamVipsTargetFFM;
import au.org.ala.images.ffm.VipsLibraryFFM;
import au.org.ala.images.tiling.OnDemandImageTiler;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * FFM-based on-demand tiler using libvips.
 */
public class FfmOnDemandImageTiler implements IOnDemandImageTiler {

    private static final Logger log = LoggerFactory.getLogger(FfmOnDemandImageTiler.class);

    private final VipsLibraryFFM vips;
    private final IOnDemandImageTiler fallback;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final Color tileBackgroundColor;
    private final ZoomFactorStrategy zoomFactorStrategy;
    private final boolean padTiles;

    public FfmOnDemandImageTiler(ImageTilerConfig config, IOnDemandImageTiler fallback) {
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary();
        this.fallback = fallback;
        if (config != null) {
            this.tileSize = config.getTileSize();
            this.tileFormat = config.getTileFormat();
            this.tileBackgroundColor = config.getTileBackgroundColor();
            this.zoomFactorStrategy = config.getZoomFactorStrategy();
            this.padTiles = config.isPadTiles();
        } else {
            this.tileSize = 256;
            this.tileFormat = TileFormat.JPEG;
            this.tileBackgroundColor = Color.gray;
            this.zoomFactorStrategy = new DefaultZoomFactorStrategy(this.tileSize);
            this.padTiles = true;
        }
    }

    @Override
    public TileGenerationResult generateTile(InputStream imageInputStream, TilerSink tilerSink, int level, int x, int y) {
        if (vips == null) {
            return fallback.generateTile(imageInputStream, tilerSink, level, x, y);
        }

        BufferedInputStream bis = (imageInputStream instanceof BufferedInputStream) 
                ? (BufferedInputStream) imageInputStream : new BufferedInputStream(imageInputStream, 1024 * 1024);

        bis.mark(10 * 1024 * 1024);
        try (InputStreamVipsSourceFFM vipsSource = new InputStreamVipsSourceFFM(vips, bis)) {
            MemorySegment image = vips.vipsImageNewFromSource(vipsSource.getSource(), "");
            if (image == null || image.address() == 0) {
                throw new IOException("Failed to load image: " + vips.vipsErrorBuffer());
            }

            MemorySegment workingImage = image;
            try (Arena tempArena = Arena.ofConfined()) {
                int width = vips.vipsImageGetWidth(image);
                int height = vips.vipsImageGetHeight(image);
                int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width);
                TilePyramidInfo info = new TilePyramidInfo(width, height, pyramid, tileSize);

                if (level < 0 || level >= info.getLevels()) {
                    return TileGenerationResult.invalidLevel(level, info.getLevels());
                }

                int subsample = info.getSubsampleForLevel(level);

                // Validate coordinates
                int levelWidth = (int) Math.ceil((double) info.getImageWidth() / subsample);
                int levelHeight = (int) Math.ceil((double) info.getImageHeight() / subsample);
                int tilesX = (int) Math.ceil((double) levelWidth / tileSize);
                int tilesY = (int) Math.ceil((double) levelHeight / tileSize);

                if (x < 0 || x >= tilesX || y < 0 || y >= tilesY) {
                    return TileGenerationResult.outOfBounds(level, x, y, tilesX, tilesY);
                }

                // Handle bottom-up coordinate properly (TMS)
                int tileTopAtLevel = Math.max(0, levelHeight - (y + 1) * tileSize);
                int tileBottomAtLevel = levelHeight - y * tileSize;

                int srcX = x * tileSize * subsample;
                int srcY = tileTopAtLevel * subsample;
                int srcW = tileSize * subsample;
                int srcH = (tileBottomAtLevel - tileTopAtLevel) * subsample;

                // Adjust for edges
                srcW = Math.min(srcW, info.getImageWidth() - srcX);

                MemorySegment outPtr = tempArena.allocate(ValueLayout.ADDRESS);
                if (vips.vipsCrop(image, outPtr, srcX, srcY, srcW, srcH) != 0) {
                    throw new IOException("vips_crop failed: " + vips.vipsErrorBuffer());
                }
                workingImage = outPtr.get(ValueLayout.ADDRESS, 0);

                // Resize to target size if needed
                if (subsample != 1) {
                    double scale = 1.0 / subsample;
                    MemorySegment resizedPtr = tempArena.allocate(ValueLayout.ADDRESS);
                    if (vips.vipsResize(workingImage, resizedPtr, scale) != 0) {
                        throw new IOException("vips_resize failed: " + vips.vipsErrorBuffer());
                    }
                    MemorySegment resized = resizedPtr.get(ValueLayout.ADDRESS, 0);
                    vips.gObjectUnref(workingImage);
                    workingImage = resized;
                }

                TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
                TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
                ByteSink byteSink = columnSink.getTileSink(y);

                writeTileImage(workingImage, byteSink, tempArena);

                return TileGenerationResult.success();
            } finally {
                if (workingImage != null && workingImage != image && workingImage.address() != 0) {
                    vips.gObjectUnref(workingImage);
                }
                vips.gObjectUnref(image);
            }
        } catch (Throwable e) {
            log.warn("FFM tile generation failed, falling back: {}", e.getMessage());
            try {
                bis.reset();
            } catch (IOException resetEx) {
                log.debug("Failed to reset stream for fallback: {}", resetEx.getMessage());
            }
            return fallback.generateTile(bis, tilerSink, level, x, y);
        }
    }

    private void writeTileImage(MemorySegment tileImage, ByteSink byteSink, Arena arena) throws IOException {
        MemorySegment outputImage = tileImage;
        try {
            outputImage = prepareImageForOutput(tileImage, arena);
            try (OutputStream outputStream = byteSink.openStream();
                 OutputStreamVipsTargetFFM target = new OutputStreamVipsTargetFFM(vips, outputStream)) {
                String suffix = tileFormat == TileFormat.PNG ? ".png" : ".jpg";
                if (vips.vipsImageWriteToTarget(outputImage, suffix, target.getTarget()) != 0) {
                    throw new IOException("vips_image_write_to_target failed: " + vips.vipsErrorBuffer());
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException("Failed to write tile image", e);
        } finally {
            if (outputImage != null && outputImage.address() != 0 && !outputImage.equals(tileImage)) {
                try {
                    vips.gObjectUnref(outputImage);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    private MemorySegment prepareImageForOutput(MemorySegment tileImage, Arena arena) throws IOException {
        try {
            int width = vips.vipsImageGetWidth(tileImage);
            int height = vips.vipsImageGetHeight(tileImage);
            if (!TilePadding.requiresPadding(padTiles, tileSize, width, height)) {
                return tileImage;
            }

            MemorySegment paddedInput = tileImage;
            MemorySegment alphaImage = null;
            if (tileFormat == TileFormat.PNG && vips.vipsImageHasAlpha(tileImage) == 0) {
                MemorySegment alphaPtr = arena.allocate(ValueLayout.ADDRESS);
                if (vips.vipsAddAlpha(tileImage, alphaPtr) != 0) {
                    throw new IOException("vips_addalpha failed: " + vips.vipsErrorBuffer());
                }
                alphaImage = alphaPtr.get(ValueLayout.ADDRESS, 0);
                paddedInput = alphaImage;
            }

            MemorySegment background = null;
            try {
                background = vips.vipsArrayDoubleNew(resolveBackgroundArray());
                MemorySegment paddedPtr = arena.allocate(ValueLayout.ADDRESS);
                if (vips.vipsEmbed(paddedInput, paddedPtr, 0, tileSize - height, tileSize, tileSize, 5, background) != 0) {
                    throw new IOException("vips_embed failed: " + vips.vipsErrorBuffer());
                }
                return paddedPtr.get(ValueLayout.ADDRESS, 0);
            } finally {
                if (background != null && background.address() != 0) {
                    vips.vipsAreaUnref(background);
                }
                if (alphaImage != null && alphaImage.address() != 0) {
                    vips.gObjectUnref(alphaImage);
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException("Failed to prepare tile image", e);
        }
    }

    private double[] resolveBackgroundArray() {
        if (tileFormat == TileFormat.PNG) {
            return new double[]{0d, 0d, 0d, 0d};
        }
        return new double[]{
                tileBackgroundColor.getRed(),
                tileBackgroundColor.getGreen(),
                tileBackgroundColor.getBlue()
        };
    }

}
