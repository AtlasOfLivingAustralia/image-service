package au.org.ala.images.tiling;

import au.org.ala.images.optimisation.CommandExecutor;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Streaming tiler that uses external tools (vips) to generate tiles without loading
 * the full image into memory. Streams bytes through the tool's stdin/stdout.
 */
public class StreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(StreamingImageTiler.class);

    private final CommandExecutor commandExecutor;
    private final String tool; // 'vips' primarily
    private final long timeoutSeconds;
    private final int tileSize;

    public StreamingImageTiler(CommandExecutor commandExecutor) {
        this(commandExecutor, "vips", 120, 256);
    }

    public StreamingImageTiler(CommandExecutor commandExecutor, String tool) {
        this(commandExecutor, tool, 120, 256);
    }

    public StreamingImageTiler(CommandExecutor commandExecutor, String tool, long timeoutSeconds, int tileSize) {
        this.commandExecutor = commandExecutor;
        this.tool = tool;
        this.timeoutSeconds = timeoutSeconds;
        this.tileSize = tileSize;
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        Path tempOutDirPath = java.nio.file.Files.createTempDirectory("tile-out-");
        File tempOutDir = tempOutDirPath.toFile();

        try {
            if ("vips".equals(tool)) {
                return tileWithVips(imageInputStream, tempOutDir, tilerSink, minLevel, maxLevel);
            } else {
                throw new IllegalArgumentException("Unsupported tool: " + tool);
            }
        } finally {
            deleteDirectory(tempOutDir);
        }
    }

    private ImageTilerResults tileWithVips(InputStream inputStream, File outputDir, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        // Use vips dzsave to generate Deep Zoom tiles from stdin
        // Format: vips dzsave stdin output --tile-size 256 --depth onetile
        List<String> args = new ArrayList<>();
        args.add("dzsave");
        args.add("stdin");
        args.add(new File(outputDir, "tiles").getAbsolutePath());
        args.add("--tile-size");
        args.add(String.valueOf(tileSize));
        args.add("--overlap");
        args.add("0");
        args.add("--suffix");
        args.add(".png");
        args.add("--depth");
        args.add("onetile");
        args.add("--layout");
        args.add("google");  // Use Google Maps tile layout (z/x/y)

        CommandExecutor.ExecResult result = commandExecutor.exec("vips", args, outputDir, inputStream, timeoutSeconds, null);

        if (result.exitCode != 0) {
            log.error("vips dzsave failed with exit code {}: {}", result.exitCode, result.stderr);
            return new ImageTilerResults(false, 0);
        }

        // Parse the generated tiles and copy them to the tiler sink
        // For 'google' layout, tiles are directly in the base directory
        File tilesDir = new File(outputDir, "tiles");
        if (!tilesDir.exists() || !tilesDir.isDirectory()) {
            // Some versions/layouts might still use _files suffix
            tilesDir = new File(outputDir, "tiles_files");
        }

        if (!tilesDir.exists() || !tilesDir.isDirectory()) {
            log.error("vips did not create expected tiles directory: {}", tilesDir.getAbsolutePath());
            return new ImageTilerResults(false, 0);
        }

        // Copy tiles from vips output to tiler sink
        int maxZoomLevel = 0;
        File[] levelDirs = tilesDir.listFiles(File::isDirectory);
        if (levelDirs != null) {
            for (File levelDir : levelDirs) {
                int level;
                try {
                    level = Integer.parseInt(levelDir.getName());
                } catch (NumberFormatException e) {
                    continue;
                }
                
                if (level < minLevel || level > maxLevel) {
                    continue;
                }
                maxZoomLevel = Math.max(maxZoomLevel, level);

                TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);

                // Group files by column
                Map<Integer, List<File>> columnFiles = new TreeMap<>();
                File[] colDirs = levelDir.listFiles(File::isDirectory);
                if (colDirs != null) {
                    for (File colDir : colDirs) {
                        int col;
                        try {
                            col = Integer.parseInt(colDir.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        
                        File[] rowFilesArr = colDir.listFiles(File::isFile);
                        if (rowFilesArr != null) {
                            List<File> rowFiles = new ArrayList<>();
                            for (File f : rowFilesArr) {
                                rowFiles.add(f);
                            }
                            Collections.sort(rowFiles, (f1, f2) -> f1.getName().compareTo(f2.getName()));
                            columnFiles.put(col, rowFiles);
                        }
                    }
                }

                // Process each column
                for (Map.Entry<Integer, List<File>> entry : columnFiles.entrySet()) {
                    int col = entry.getKey();
                    List<File> rowFiles = entry.getValue();
                    TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, Integer.MAX_VALUE);

                    for (File rowFile : rowFiles) {
                        int row;
                        try {
                            row = Integer.parseInt(rowFile.getName().replace(".png", ""));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        ByteSink tileSink = columnSink.getTileSink(row);

                        // Copy tile to sink
                        Files.asByteSource(rowFile).copyTo(tileSink);
                    }
                }
            }
        }

        return new ImageTilerResults(true, maxZoomLevel + 1);
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        deleteDirectory(f);
                    } else {
                        f.delete();
                    }
                }
            }
            dir.delete();
        }
    }
}
