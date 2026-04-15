package au.org.ala.images.tiling;

import au.org.ala.images.optimisation.CommandExecutor;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ImageMagick CLI-based on-demand tiler.
 */
public class MagickCliOnDemandImageTiler implements IOnDemandImageTiler {

    private static final Logger log = LoggerFactory.getLogger(MagickCliOnDemandImageTiler.class);

    private final CommandExecutor commandExecutor;
    private final String magickCommand;
    private final IOnDemandImageTiler fallback;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final ZoomFactorStrategy zoomFactorStrategy;

    public MagickCliOnDemandImageTiler(CommandExecutor commandExecutor, String magickCommand, ImageTilerConfig config, IOnDemandImageTiler fallback) {
        this.commandExecutor = commandExecutor;
        this.magickCommand = magickCommand;
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
        BufferedInputStream bis = (imageInputStream instanceof BufferedInputStream) 
                ? (BufferedInputStream) imageInputStream : new BufferedInputStream(imageInputStream, 1024 * 1024);
        
        try {
            TilePyramidInfo info = getPyramidInfo(bis);

            if (level < 0 || level >= info.getLevels()) {
                return TileGenerationResult.invalidLevel(level, info.getLevels());
            }
            if (x < 0 || x >= info.getTilesXForLevel(level) || y < 0 || y >= info.getTilesYForLevel(level)) {
                return TileGenerationResult.outOfBounds(level, x, y, info.getTilesXForLevel(level), info.getTilesYForLevel(level));
            }

            int subsample = info.getSubsampleForLevel(level);

            // Handle bottom-up coordinate properly (TMS)
            int levelHeight = (int) Math.ceil((double) info.getImageHeight() / subsample);
            int tileTopAtLevel = Math.max(0, levelHeight - (y + 1) * tileSize);
            int tileBottomAtLevel = levelHeight - y * tileSize;

            int srcX = x * tileSize * subsample;
            int srcY = tileTopAtLevel * subsample;
            int srcW = tileSize * subsample;
            int srcH = (tileBottomAtLevel - tileTopAtLevel) * subsample;

            // Adjust for edges
            srcW = Math.min(srcW, info.getImageWidth() - srcX);

            int targetWidth = srcW / subsample;
            int targetHeight = srcH / subsample;

            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
            ByteSink byteSink = columnSink.getTileSink(y);

            String suffix = (tileFormat == TileFormat.PNG) ? "png:" : "jpg:";
            
            try (OutputStream os = byteSink.openStream()) {
                // magick stdin -crop WxH+X+Y +repage -resize WxH stdout:
                CommandExecutor.ExecResult res = commandExecutor.exec(magickCommand, Arrays.asList(
                    "-", 
                    "-crop", srcW + "x" + srcH + "+" + srcX + "+" + srcY, 
                    "+repage", 
                    "-resize", targetWidth + "x" + targetHeight,
                    suffix + "-"
                ), null, bis, 60, os);
                
                if (res.exitCode != 0) {
                    throw new IOException("magick crop failed: " + res.stderr);
                }
            }

            return TileGenerationResult.success();

        } catch (Exception e) {
            log.warn("Magick CLI tile generation failed, falling back: {}", e.getMessage());
            return fallback.generateTile(imageInputStream, tilerSink, level, x, y);
        }
    }

    private TilePyramidInfo getPyramidInfo(BufferedInputStream bis) throws IOException {
        bis.mark(1024 * 1024);
        try {
            CommandExecutor.ExecResult res = commandExecutor.exec(magickCommand, Arrays.asList("identify", "-format", "%wx%h", "-"), null, bis, 10, null);
            if (res.exitCode != 0) {
                if (fallback instanceof OnDemandImageTiler) {
                    bis.reset();
                    return ((OnDemandImageTiler) fallback).getPyramidInfo(bis);
                }
                throw new IOException("magick identify failed: " + res.stderr);
            }
            
            Pattern p = Pattern.compile("(\\d+)x(\\d+)");
            Matcher m = p.matcher(res.stdout);
            if (m.find()) {
                int width = Integer.parseInt(m.group(1));
                int height = Integer.parseInt(m.group(2));
                int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width);
                return new TilePyramidInfo(width, height, pyramid, tileSize);
            }
            throw new IOException("Could not parse magick identify output: " + res.stdout);
        } finally {
            bis.reset();
        }
    }
}
