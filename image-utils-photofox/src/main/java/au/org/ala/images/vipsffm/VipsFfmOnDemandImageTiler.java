package au.org.ala.images.vipsffm;

import app.photofox.vipsffm.VImage;
import au.org.ala.images.tiling.DefaultZoomFactorStrategy;
import au.org.ala.images.tiling.IOnDemandImageTiler;
import au.org.ala.images.tiling.ImageTilerConfig;
import au.org.ala.images.tiling.TileFormat;
import au.org.ala.images.tiling.TileGenerationResult;
import au.org.ala.images.tiling.TilePyramidInfo;
import au.org.ala.images.tiling.TilerSink;
import au.org.ala.images.tiling.ZoomFactorStrategy;
import com.google.common.io.ByteSink;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * On-demand tiler using app.photofox.vipsffm (lopcode/vips-ffm).
 */
public class VipsFfmOnDemandImageTiler implements IOnDemandImageTiler {

    private static final Logger log = LoggerFactory.getLogger(VipsFfmOnDemandImageTiler.class);

    private final IOnDemandImageTiler fallback;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final ZoomFactorStrategy zoomFactorStrategy;

    public VipsFfmOnDemandImageTiler(ImageTilerConfig config, IOnDemandImageTiler fallback) {
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
        BufferedInputStream bis = imageInputStream instanceof BufferedInputStream
            ? (BufferedInputStream) imageInputStream
            : new BufferedInputStream(imageInputStream, 10 * 1024 * 1024);

        bis.mark(10 * 1024 * 1024);
        try (Arena arena = Arena.ofConfined()) {
            VImage image = VImage.newFromStream(arena, bis);
            int width = image.getWidth();
            int height = image.getHeight();
            int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width);
            TilePyramidInfo info = new TilePyramidInfo(width, height, pyramid, tileSize);

            if (level < 0 || level >= info.getLevels()) {
                return TileGenerationResult.invalidLevel(level, info.getLevels());
            }

            int subsample = info.getSubsampleForLevel(level);

            // Validate coordinates.
            int levelWidth = (int) Math.ceil((double) info.getImageWidth() / subsample);
            int levelHeight = (int) Math.ceil((double) info.getImageHeight() / subsample);
            int tilesX = (int) Math.ceil((double) levelWidth / tileSize);
            int tilesY = (int) Math.ceil((double) levelHeight / tileSize);

            if (x < 0 || x >= tilesX || y < 0 || y >= tilesY) {
                return TileGenerationResult.outOfBounds(level, x, y, tilesX, tilesY);
            }

            // Handle bottom-up coordinate properly (TMS).
            int tileTopAtLevel = Math.max(0, levelHeight - (y + 1) * tileSize);
            int tileBottomAtLevel = levelHeight - y * tileSize;

            int srcX = x * tileSize * subsample;
            int srcY = tileTopAtLevel * subsample;
            int srcW = tileSize * subsample;
            int srcH = (tileBottomAtLevel - tileTopAtLevel) * subsample;

            // Adjust for edges.
            srcW = Math.min(srcW, info.getImageWidth() - srcX);

            VImage workingImage = image.extractArea(srcX, srcY, srcW, srcH);

            if (subsample != 1) {
                double scale = 1.0 / subsample;
                workingImage = workingImage.resize(scale);
            }

            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
            ByteSink byteSink = columnSink.getTileSink(y);

            try (OutputStream os = byteSink.openStream()) {
                String suffix = tileFormat == TileFormat.PNG ? ".png" : ".jpg[Q=85]";
                workingImage.writeToStream(os, suffix);
            }

            return TileGenerationResult.success();
        } catch (Exception e) {
            log.warn("vips-ffm on-demand tiling failed, falling back: {}", e.getMessage());
            try {
                bis.reset();
            } catch (IOException resetEx) {
                log.debug("Failed to reset stream for fallback: {}", resetEx.getMessage());
            }
            return fallback.generateTile(bis, tilerSink, level, x, y);
        }
    }
}


