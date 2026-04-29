package au.org.ala.images.tiling;

import au.org.ala.images.optimisation.CommandExecutor;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.awt.Color;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private final Color tileBackgroundColor;
    private final ZoomFactorStrategy zoomFactorStrategy;
    private final boolean padTiles;

    public MagickCliOnDemandImageTiler(CommandExecutor commandExecutor, String magickCommand, ImageTilerConfig config, IOnDemandImageTiler fallback) {
        this.commandExecutor = commandExecutor;
        this.magickCommand = magickCommand;
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

            CommandExecutor.ExecResult res;
            try (OutputStream outputStream = byteSink.openStream()) {
                res = commandExecutor.exec(magickCommand, buildMagickArgs(srcX, srcY, srcW, srcH, targetWidth, targetHeight), null, bis, 60, outputStream);
            }
                
            if (res.exitCode != 0) {
                throw new IOException("magick crop failed: " + res.stderr);
            }

            return TileGenerationResult.success();

        } catch (Exception e) {
            log.warn("Magick CLI tile generation failed, falling back: {}", e.getMessage());
            return fallback.generateTile(imageInputStream, tilerSink, level, x, y);
        }
    }

    private List<String> buildMagickArgs(int srcX, int srcY, int srcW, int srcH, int targetWidth, int targetHeight) {
        List<String> args = new ArrayList<>();
        args.add("-");
        args.add("-crop");
        args.add(srcW + "x" + srcH + "+" + srcX + "+" + srcY);
        args.add("+repage");
        args.add("-resize");
        args.add(targetWidth + "x" + targetHeight);
        if (TilePadding.requiresPadding(padTiles, tileSize, targetWidth, targetHeight)) {
            args.add("-background");
            args.add(tileFormat == TileFormat.PNG ? "rgba(0,0,0,0)" : String.format("rgb(%d,%d,%d)",
                tileBackgroundColor.getRed(),
                tileBackgroundColor.getGreen(),
                tileBackgroundColor.getBlue()));
            args.add("-gravity");
            args.add("SouthWest");
            args.add("-extent");
            args.add(tileSize + "x" + tileSize);
        }
        args.add((tileFormat == TileFormat.PNG) ? "png:-" : "jpg:-");
        return args;
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
