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
 * libvips CLI-based on-demand tiler.
 */
public class VipsCliOnDemandImageTiler implements IOnDemandImageTiler {

    private static final Logger log = LoggerFactory.getLogger(VipsCliOnDemandImageTiler.class);

    private final CommandExecutor commandExecutor;
    private final String vipsCommand;
    private final IOnDemandImageTiler fallback;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final ZoomFactorStrategy zoomFactorStrategy;

    public VipsCliOnDemandImageTiler(CommandExecutor commandExecutor, String vipsCommand, ImageTilerConfig config, IOnDemandImageTiler fallback) {
        this.commandExecutor = commandExecutor;
        this.vipsCommand = vipsCommand;
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

            int targetWidth = tileSize;
            int targetHeight = (tileBottomAtLevel - tileTopAtLevel);
            
            double scale = 1.0 / subsample;

            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
            ByteSink byteSink = columnSink.getTileSink(y);

            String suffix = (tileFormat == TileFormat.PNG) ? ".png" : ".jpg";

            try (OutputStream os = byteSink.openStream()) {
                if (subsample == 1) {
                    // No resize needed, just extract
                    CommandExecutor.ExecResult res = commandExecutor.exec(vipsCommand, Arrays.asList(
                        "extract_area", "stdin", ".stdout" + suffix,
                        String.valueOf(srcX), String.valueOf(srcY), String.valueOf(srcW), String.valueOf(srcH)
                    ), null, bis, 60, os);
                    if (res.exitCode != 0) {
                        throw new IOException("vips extract_area failed: " + res.stderr);
                    }
                } else {
                    // Pipe extract_area to resize
                    String shellCommand = String.format("%s extract_area stdin .stdout%s %d %d %d %d | %s resize stdin .stdout%s %f",
                        vipsCommand, suffix, srcX, srcY, srcW, srcH,
                        vipsCommand, suffix, scale);
                    
                    CommandExecutor.ExecResult res = commandExecutor.exec("sh", Arrays.asList("-c", shellCommand), null, bis, 60, os);
                    if (res.exitCode != 0) {
                        throw new IOException("vips pipe failed: " + res.stderr);
                    }
                }
            }

            return TileGenerationResult.success();

        } catch (Exception e) {
            log.warn("Vips CLI tile generation failed, falling back: {}", e.getMessage());
            return fallback.generateTile(imageInputStream, tilerSink, level, x, y);
        }
    }

    private TilePyramidInfo getPyramidInfo(BufferedInputStream bis) throws IOException {
        bis.mark(1024 * 1024);
        try {
            CommandExecutor.ExecResult res = commandExecutor.exec(vipsCommand, Arrays.asList("header", "stdin"), null, bis, 10, null);
            if (res.exitCode != 0) {
                // If vipsheader fails, maybe it's not a vips-supported format, or the stream is empty.
                // We'll try the fallback or throw.
                if (fallback instanceof OnDemandImageTiler) {
                    bis.reset();
                    return ((OnDemandImageTiler) fallback).getPyramidInfo(bis);
                }
                throw new IOException("vipsheader failed: " + res.stderr);
            }
            
            Pattern p = Pattern.compile("(\\d+)x(\\d+)");
            Matcher m = p.matcher(res.stdout);
            if (m.find()) {
                int width = Integer.parseInt(m.group(1));
                int height = Integer.parseInt(m.group(2));
                int[] pyramid = zoomFactorStrategy.getZoomFactors(height, width);
                return new TilePyramidInfo(width, height, pyramid, tileSize);
            }
            throw new IOException("Could not parse vipsheader output: " + res.stdout);
        } finally {
            bis.reset();
        }
    }
}
