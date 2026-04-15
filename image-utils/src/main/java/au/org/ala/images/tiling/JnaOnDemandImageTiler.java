package au.org.ala.images.tiling;

import au.org.ala.images.jna.InputStreamVipsSource;
import au.org.ala.images.jna.NativeLibraryDetector;
import au.org.ala.images.jna.OutputStreamVipsTarget;
import au.org.ala.images.jna.VipsLibrary;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * JNA-based on-demand tiler using libvips.
 */
public class JnaOnDemandImageTiler implements IOnDemandImageTiler {

    private static final Logger log = LoggerFactory.getLogger(JnaOnDemandImageTiler.class);

    private final VipsLibrary vips;
    private final IOnDemandImageTiler fallback;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final ZoomFactorStrategy zoomFactorStrategy;

    public JnaOnDemandImageTiler(ImageTilerConfig config, IOnDemandImageTiler fallback) {
        this.vips = NativeLibraryDetector.getVipsLibrary();
        this.fallback = fallback;
        if (config != null) {
            this.tileSize = config.getTileSize();
            this.tileFormat = config.getTileFormat();
            this.zoomFactorStrategy = config.getZoomFactorStrategy();
        } else {
            this.tileSize = 256;
            this.tileFormat = TileFormat.JPEG;
            this.zoomFactorStrategy = new DefaultZoomFactorStrategy(this.tileSize);
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
        try (InputStreamVipsSource vipsSource = new InputStreamVipsSource(vips, bis)) {
            Pointer image = vips.vips_image_new_from_source(vipsSource.getSource(), "", (Object) null);
            if (image == null) {
                throw new IOException("Failed to load image: " + vips.vips_error_buffer());
            }

            Pointer workingImage = null;
            try {
                int width = vips.vips_image_get_width(image);
                int height = vips.vips_image_get_height(image);
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

                PointerByReference croppedRef = new PointerByReference();
                if (vips.vips_crop(image, croppedRef, srcX, srcY, srcW, srcH, (Object) null) != 0) {
                    throw new IOException("vips_crop failed: " + vips.vips_error_buffer());
                }
                workingImage = croppedRef.getValue();

                // Resize to target size if needed
                if (subsample != 1) {
                     double scale = 1.0 / subsample;
                     PointerByReference resizedRef = new PointerByReference();
                     if (vips.vips_resize(workingImage, resizedRef, scale, (Object) null) != 0) {
                         throw new IOException("vips_resize failed: " + vips.vips_error_buffer());
                     }
                     vips.g_object_unref(workingImage);
                     workingImage = resizedRef.getValue();
                }

                TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
                TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
                ByteSink byteSink = columnSink.getTileSink(y);

                try (OutputStream os = byteSink.openStream();
                     OutputStreamVipsTarget vipsTarget = new OutputStreamVipsTarget(vips, os)) {
                    String suffix = tileFormat == TileFormat.PNG ? ".png" : ".jpg";
                    if (vips.vips_image_write_to_target(workingImage, suffix, vipsTarget.getTarget(), (Object) null) != 0) {
                         throw new IOException("vips_image_write_to_target failed: " + vips.vips_error_buffer());
                    }
                }

                return TileGenerationResult.success();
            } finally {
                if (workingImage != null) vips.g_object_unref(workingImage);
                vips.g_object_unref(image);
            }
        } catch (Exception e) {
            log.warn("JNA tile generation failed, falling back: {}", e.getMessage());
            try {
                bis.reset();
            } catch (IOException resetEx) {
                log.debug("Failed to reset stream for fallback: {}", resetEx.getMessage());
            }
            return fallback.generateTile(bis, tilerSink, level, x, y);
        }
    }

}
