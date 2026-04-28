package au.org.ala.images.tiling;

import au.org.ala.images.optimisation.CommandExecutor;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.awt.Color;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CLI-based streaming tiler that uses vips per-tile pipeline stages.
 * <p>
 * Mirrors the geometry of {@link JnaStreamingImageTiler}: for each zoom level the source
 * image is resized once to a native-format intermediate, then individual tiles are
 * extracted with TMS bottom-up (y=0 is bottom) row numbering — identical to every
 * other tiler in this package.
 * <p>
 * Pipeline per tile:
 * <pre>
 *   subsample == 1:  vips extract_area &lt;srcFile&gt; .stdout&lt;suffix&gt; left top w h
 *   subsample  &gt; 1:  vips resize       &lt;srcFile&gt; level-N.v scale
 *                    vips extract_area  level-N.v .stdout&lt;suffix&gt; left top w h
 * </pre>
 */
public class StreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(StreamingImageTiler.class);

    private static final long HEADER_TIMEOUT_SECONDS = 10;
    private static final long TILE_TIMEOUT_SECONDS   = 60;
    private static final long RESIZE_TIMEOUT_SECONDS = 120;

    private final CommandExecutor commandExecutor;
    private final String vipsCommand;
    private final int tileSize;
    private final TileFormat tileFormat;
    private final Color tileBackgroundColor;
    private final boolean padTiles;
    private final ZoomFactorStrategy zoomFactorStrategy;

    // ── Constructors ────────────────────────────────────────────────────────────

    /** Preferred constructor: vips binary, all settings from config. */
    public StreamingImageTiler(CommandExecutor commandExecutor, ImageTilerConfig config) {
        this(commandExecutor, "vips", config);
    }

    public StreamingImageTiler(CommandExecutor commandExecutor, String vipsCommand, ImageTilerConfig config) {
        this.commandExecutor    = commandExecutor;
        this.vipsCommand        = vipsCommand;
        this.tileSize           = config.getTileSize();
        this.tileFormat         = config.getTileFormat();
        this.tileBackgroundColor = config.getTileBackgroundColor();
        this.padTiles           = config.isPadTiles();
        this.zoomFactorStrategy = config.getZoomFactorStrategy();
    }

    /** Legacy constructor kept for existing call-site compatibility. */
    public StreamingImageTiler(CommandExecutor commandExecutor) {
        this(commandExecutor, "vips", 120, 256);
    }

    public StreamingImageTiler(CommandExecutor commandExecutor, String tool) {
        this(commandExecutor, tool, 120, 256);
    }

    public StreamingImageTiler(CommandExecutor commandExecutor, String tool,
                               @SuppressWarnings("unused") long ignoredTimeout, int tileSize) {
        this.commandExecutor    = commandExecutor;
        this.vipsCommand        = tool;
        this.tileSize           = tileSize;
        this.tileFormat         = TileFormat.JPEG;
        this.tileBackgroundColor = Color.gray;
        this.padTiles           = true;
        this.zoomFactorStrategy = new DefaultZoomFactorStrategy(tileSize);
    }

    // ── IImageTiler ─────────────────────────────────────────────────────────────

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream,
                                       TilerSink tilerSink,
                                       int minLevel,
                                       int maxLevel) throws IOException, InterruptedException {
        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        // Buffer the entire source to a temp file so it can be re-read per level/tile.
        Path srcTmp = Files.createTempFile("sit-src-", ".img");
        try {
            copyToFile(imageInputStream, srcTmp);
            return tileFromFile(srcTmp.toFile(), tilerSink, minLevel, maxLevel);
        } finally {
            silentDelete(srcTmp.toFile());
        }
    }

    // ── Core logic ──────────────────────────────────────────────────────────────

    private ImageTilerResults tileFromFile(File srcFile,
                                           TilerSink tilerSink,
                                           int minLevel,
                                           int maxLevel) throws IOException {
        // 1. Read image dimensions once.
        int[] dims        = readImageDimensions(srcFile);
        int origWidth     = dims[0];
        int origHeight    = dims[1];

        int[] pyramid     = zoomFactorStrategy.getZoomFactors(origHeight, origWidth);
        int zoomLevels    = pyramid.length;
        int finalMaxLevel = Math.min(maxLevel, zoomLevels - 1);

        if (minLevel > finalMaxLevel) {
            log.debug("StreamingImageTiler: requested levels {}-{} but only {} available",
                    minLevel, maxLevel, zoomLevels);
            return new ImageTilerResults(true, zoomLevels);
        }

        String suffix = tileFormat == TileFormat.PNG ? ".png" : ".jpg";

        for (int level = minLevel; level <= finalMaxLevel; level++) {
            int subsample   = pyramid[level];
            int levelWidth;
            int levelHeight;

            // 2. Produce a level-resolution intermediate in vips native format (.v)
            //    — lossless and fast for subsequent per-tile extract_area calls.
            //    Re-use the original file when subsample == 1 (no resize needed).
            File levelFile    = srcFile;
            boolean ownLevel  = false;
            if (subsample > 1) {
                levelFile = File.createTempFile("sit-lvl" + level + "-", ".v");
                ownLevel  = true;
                resizeToFile(srcFile, levelFile, 1.0 / subsample);
                int[] levelDims = readImageDimensions(levelFile);
                levelWidth = levelDims[0];
                levelHeight = levelDims[1];
            } else {
                levelWidth = origWidth;
                levelHeight = origHeight;
            }

            int cols = (int) Math.ceil((double) levelWidth  / tileSize);
            int rows = (int) Math.ceil((double) levelHeight / tileSize);

            log.debug("StreamingImageTiler level {}: {}x{} (subsample {}), {}x{} tiles",
                    level, levelWidth, levelHeight, subsample, cols, rows);

            try {
                TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);

                for (int col = 0; col < cols; col++) {
                    TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, 1);

                    for (int tmsRow = 0; tmsRow < rows; tmsRow++) {
                        // TMS origin is bottom-left: row 0 = bottom of image.
                        // Convert to image-space top coordinate (matches JnaStreamingImageTiler.generateSingleTile).
                        int left   = col * tileSize;
                        int top    = Math.max(0, levelHeight - (tmsRow + 1) * tileSize);
                        int bottom = levelHeight - tmsRow * tileSize;
                        int w      = Math.min(tileSize, levelWidth  - left);
                        int h      = bottom - top;

                        ByteSink tileSink = columnSink.getTileSink(tmsRow);
                        extractTile(levelFile, tileSink, suffix, left, top, w, h, level, col, tmsRow);
                    }
                }
            } finally {
                if (ownLevel) {
                    silentDelete(levelFile);
                }
            }
        }

        return new ImageTilerResults(true, zoomLevels);
    }

    // ── vips helpers ────────────────────────────────────────────────────────────

    /** Read image width × height from {@code vips header} output. */
    private int[] readImageDimensions(File file) throws IOException {
        List<String> errors = new ArrayList<>();

        CommandExecutor.ExecResult headerRes = commandExecutor.exec(
                vipsCommand,
                Arrays.asList("header", file.getAbsolutePath()),
                null, (InputStream) null, HEADER_TIMEOUT_SECONDS, (OutputStream) null);
        if (headerRes.exitCode == 0) {
            int[] parsed = parseDimensionsFromWxH(headerRes.stdout);
            if (parsed != null) {
                return parsed;
            }
            errors.add("" + vipsCommand + " header: unparseable output: " + headerRes.stdout);
        } else {
            errors.add("" + vipsCommand + " header failed: " + headerRes.stderr);
        }

        String vipsHeaderCommand = resolveVipsHeaderCommand(vipsCommand);
        CommandExecutor.ExecResult widthRes = commandExecutor.exec(
                vipsHeaderCommand,
                Arrays.asList("-f", "width", file.getAbsolutePath()),
                null, (InputStream) null, HEADER_TIMEOUT_SECONDS, (OutputStream) null);
        CommandExecutor.ExecResult heightRes = commandExecutor.exec(
                vipsHeaderCommand,
                Arrays.asList("-f", "height", file.getAbsolutePath()),
                null, (InputStream) null, HEADER_TIMEOUT_SECONDS, (OutputStream) null);
        if (widthRes.exitCode == 0 && heightRes.exitCode == 0) {
            Integer width = parseFirstNumber(widthRes.stdout);
            Integer height = parseFirstNumber(heightRes.stdout);
            if (width != null && height != null) {
                return new int[]{ width, height };
            }
            errors.add(vipsHeaderCommand + ": unparseable output (width='" + widthRes.stdout + "', height='" + heightRes.stdout + "')");
        } else {
            errors.add(vipsHeaderCommand + " failed: width='" + widthRes.stderr + "', height='" + heightRes.stderr + "'");
        }

        throw new IOException("Unable to read image dimensions via libvips CLI. Attempts: " + String.join(" | ", errors));
    }

    private static int[] parseDimensionsFromWxH(String output) {
        Pattern p = Pattern.compile("(\\d+)x(\\d+)");
        Matcher m = p.matcher(output == null ? "" : output);
        if (!m.find()) {
            return null;
        }
        return new int[]{ Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)) };
    }

    private static Integer parseFirstNumber(String output) {
        Pattern p = Pattern.compile("(\\d+)");
        Matcher m = p.matcher(output == null ? "" : output);
        if (!m.find()) {
            return null;
        }
        return Integer.parseInt(m.group(1));
    }

    private static String resolveVipsHeaderCommand(String vipsCmd) {
        if (vipsCmd == null || vipsCmd.isEmpty()) {
            return "vipsheader";
        }
        File cmdFile = new File(vipsCmd);
        String name = cmdFile.getName();
        String parent = cmdFile.getParent();
        if ("vips".equals(name)) {
            return parent == null ? "vipsheader" : new File(parent, "vipsheader").getPath();
        }
        return "vipsheader";
    }

    /**
     * Resize {@code src} by {@code scale} and write to {@code dst} in vips native
     * format (.v) for lossless, fast subsequent tile extractions.
     */
    private void resizeToFile(File src, File dst, double scale) throws IOException {
        CommandExecutor.ExecResult res = commandExecutor.exec(
                vipsCommand,
                Arrays.asList("resize", src.getAbsolutePath(), dst.getAbsolutePath(),
                        Double.toString(scale)),
                null, (InputStream) null, RESIZE_TIMEOUT_SECONDS, (OutputStream) null);
        if (res.exitCode != 0) {
            throw new IOException("vips resize failed (scale=" + scale + "): " + res.stderr);
        }
    }

    /**
     * Extract a single tile region from {@code levelFile} and stream the encoded
     * bytes directly into {@code tileSink} via the OS stdout pipe — zero extra heap copy.
     */
    private void extractTile(File levelFile,
                               ByteSink tileSink,
                               String suffix,
                               int left, int top, int width, int height,
                               int level, int col, int tmsRow) throws IOException {
        CommandExecutor.ExecResult res;
        try (OutputStream outputStream = tileSink.openStream()) {
            res = execTilePipeline(levelFile, outputStream, suffix, left, top, width, height);
        }
        if (res.exitCode != 0) {
            throw new IOException(String.format(
                    "vips tile pipeline failed at level %d tile %d/%d: %s",
                    level, col, tmsRow, res.stderr));
        }
    }

    private CommandExecutor.ExecResult execTilePipeline(File levelFile,
                                                        OutputStream outputStream,
                                                        String suffix,
                                                        int left,
                                                        int top,
                                                        int width,
                                                        int height) {
        List<CommandExecutor.PipelineStage> stages = buildTileStages(levelFile, suffix, left, top, width, height);
        if (stages.size() == 1) {
            CommandExecutor.PipelineStage stage = stages.get(0);
            return commandExecutor.exec(stage.cmd, stage.args, null, (InputStream) null, TILE_TIMEOUT_SECONDS, outputStream);
        }
        return commandExecutor.execPipeline(stages, null, null, TILE_TIMEOUT_SECONDS, outputStream);
    }

    private List<CommandExecutor.PipelineStage> buildTileStages(File levelFile,
                                                                String suffix,
                                                                int left,
                                                                int top,
                                                                int width,
                                                                int height) {
        List<CommandExecutor.PipelineStage> stages = new ArrayList<>();
        boolean requiresPadding = TilePadding.requiresPadding(padTiles, tileSize, width, height);
        stages.add(new CommandExecutor.PipelineStage(vipsCommand, Arrays.asList(
                "extract_area",
                levelFile.getAbsolutePath(),
                requiresPadding ? ".stdout.v" : ".stdout" + suffix,
                String.valueOf(left),
                String.valueOf(top),
                String.valueOf(width),
                String.valueOf(height)
        )));
        if (requiresPadding) {
            if (tileFormat == TileFormat.PNG) {
                stages.add(new CommandExecutor.PipelineStage(vipsCommand, Arrays.asList(
                        "bandjoin_const",
                        "stdin",
                        ".stdout.v",
                        "[0]"
                )));
            }
            stages.add(new CommandExecutor.PipelineStage(vipsCommand, Arrays.asList(
                    "gravity",
                    "stdin",
                    ".stdout" + suffix,
                    "south-west",
                    String.valueOf(tileSize),
                    String.valueOf(tileSize),
                    "--extend", "background",
                    "--background", backgroundString()
            )));
        }
        return stages;
    }

    private String backgroundString() {
        if (tileFormat == TileFormat.PNG) {
            return "0 0 0 0";
        }
        return tileBackgroundColor.getRed() + " " + tileBackgroundColor.getGreen() + " " + tileBackgroundColor.getBlue();
    }

    // ── Utilities ────────────────────────────────────────────────────────────────

    private static void copyToFile(InputStream in, Path dest) throws IOException {
        try (OutputStream out = new BufferedOutputStream(
                new FileOutputStream(dest.toFile()), 256 * 1024)) {
            byte[] buf = new byte[256 * 1024];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
        }
    }

    private static void silentDelete(File f) {
        if (f != null && f.exists()) {
            //noinspection ResultOfMethodCallIgnored
            f.delete();
        }
    }
}
