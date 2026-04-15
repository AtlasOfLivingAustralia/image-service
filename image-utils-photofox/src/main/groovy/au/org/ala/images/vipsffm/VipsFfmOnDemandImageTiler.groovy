package au.org.ala.images.vipsffm

import app.photofox.vipsffm.VImage
import app.photofox.vipsffm.VipsOption
import au.org.ala.images.tiling.DefaultZoomFactorStrategy
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.OnDemandImageTiler
import au.org.ala.images.tiling.TileFormat
import au.org.ala.images.tiling.TileGenerationResult
import au.org.ala.images.tiling.TilePyramidInfo
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.tiling.ZoomFactorStrategy
import com.google.common.io.ByteSink
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import javax.imageio.ImageIO
import java.awt.Color
import java.awt.image.BufferedImage
import java.lang.foreign.Arena

/**
 * On-demand tiler using app.photofox.vipsffm (lopcode/vips-ffm)
 */
@Slf4j
@CompileStatic
class VipsFfmOnDemandImageTiler implements IOnDemandImageTiler {

    private final IOnDemandImageTiler fallback
    private final int tileSize
    private final TileFormat tileFormat
    private final ZoomFactorStrategy zoomFactorStrategy

    VipsFfmOnDemandImageTiler(ImageTilerConfig config, IOnDemandImageTiler fallback) {
        this.fallback = fallback
        if (config != null) {
            this.tileSize = config.tileSize
            this.tileFormat = config.tileFormat
            this.zoomFactorStrategy = config.zoomFactorStrategy
        } else {
            this.tileSize = 256
            this.tileFormat = TileFormat.JPEG
            this.zoomFactorStrategy = new DefaultZoomFactorStrategy(this.tileSize)
        }
    }

    @Override
    TileGenerationResult generateTile(InputStream imageInputStream, TilerSink tilerSink, int level, int x, int y) {
        BufferedInputStream bis = (imageInputStream instanceof BufferedInputStream) 
                ? (BufferedInputStream) imageInputStream : new BufferedInputStream(imageInputStream, 10 * 1024 * 1024)

        bis.mark(10 * 1024 * 1024)
        try (var arena = Arena.ofConfined()) {
            VImage image = VImage.newFromStream(arena, bis)
            int width = image.width
            int height = image.height
            int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width)
            TilePyramidInfo info = new TilePyramidInfo(width, height, pyramid, tileSize)

            if (level < 0 || level >= info.levels) {
                return TileGenerationResult.invalidLevel(level, info.levels)
            }

            int subsample = info.getSubsampleForLevel(level)

            // Validate coordinates
            int levelWidth = (int) Math.ceil((double) info.imageWidth / subsample)
            int levelHeight = (int) Math.ceil((double) info.imageHeight / subsample)
            int tilesX = (int) Math.ceil((double) levelWidth / tileSize)
            int tilesY = (int) Math.ceil((double) levelHeight / tileSize)

            if (x < 0 || x >= tilesX || y < 0 || y >= tilesY) {
                return TileGenerationResult.outOfBounds(level, x, y, tilesX, tilesY)
            }

            // Handle bottom-up coordinate properly (TMS)
            int tileTopAtLevel = Math.max(0, levelHeight - (y + 1) * tileSize)
            int tileBottomAtLevel = levelHeight - y * tileSize

            int srcX = x * tileSize * subsample
            int srcY = tileTopAtLevel * subsample
            int srcW = tileSize * subsample
            int srcH = (tileBottomAtLevel - tileTopAtLevel) * subsample

            // Adjust for edges
            srcW = Math.min(srcW, info.imageWidth - srcX)

            VImage workingImage = image.extractArea(srcX, srcY, srcW, srcH)
            
            if (subsample != 1) {
                double scale = 1.0 / subsample
                workingImage = workingImage.resize(scale)
            }

            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level)
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1)
            ByteSink byteSink = columnSink.getTileSink(y)

            try (OutputStream os = byteSink.openStream()) {
                String suffix = tileFormat == TileFormat.PNG ? ".png" : ".jpg[Q=85]"
                workingImage.writeToStream(os, suffix)
            }

            return TileGenerationResult.success()
        } catch (Exception e) {
            log.warn("vips-ffm on-demand tiling failed, falling back: {}", e.getMessage())
            try {
                bis.reset()
            } catch (IOException resetEx) {
                log.debug("Failed to reset stream for fallback: ${resetEx.message}")
            }
            return fallback.generateTile(bis, tilerSink, level, x, y)
        }
    }

}
