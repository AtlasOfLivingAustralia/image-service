package au.org.ala.images.tiling

import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.ImageTilerResults
import au.org.ala.images.tiling.TilerSink
import com.google.common.io.ByteSink
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.io.IOUtils

import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.nio.file.Files

/**
 * Streaming tiler that uses external tools (vips) to generate tiles without loading
 * the full image into memory. Streams bytes through the tool's stdin/stdout.
 */
@Slf4j
@CompileStatic
class StreamingImageTiler implements IImageTiler {

    private final CommandExecutor commandExecutor
    private final String tool // 'vips' primarily
    private final long timeoutSeconds
    private final int tileSize

    StreamingImageTiler(CommandExecutor commandExecutor, String tool = 'vips', long timeoutSeconds = 120, int tileSize = 256) {
        this.commandExecutor = commandExecutor
        this.tool = tool
        this.timeoutSeconds = timeoutSeconds
        this.tileSize = tileSize
    }

    @Override
    ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels")
        }

        File tempOutDir = Files.createTempDirectory('tile-out-').toFile()

        try {
            if (tool == 'vips') {
                return tileWithVips(imageInputStream, tempOutDir, tilerSink, minLevel, maxLevel)
            } else {
                throw new IllegalArgumentException("Unsupported tool: ${tool}")
            }
        } finally {
            deleteDirectory(tempOutDir)
        }
    }

    private ImageTilerResults tileWithVips(InputStream inputStream, File outputDir, TilerSink tilerSink, int minLevel, int maxLevel) {
        // Use vips dzsave to generate Deep Zoom tiles from stdin
        // Format: vips dzsave stdin output --tile-size 256 --depth onetile
        List<String> args = [
            'dzsave',
            'stdin',
            new File(outputDir, 'tiles').absolutePath,
            '--tile-size', tileSize.toString(),
            '--overlap', '0',
            '--suffix', '.png',
            '--depth', 'onetile',
            '--layout', 'google'  // Use Google Maps tile layout (z/x/y)
        ]

        CommandExecutor.ExecResult result = commandExecutor.exec('vips', args, outputDir, inputStream, timeoutSeconds, null)

        if (result.exitCode != 0) {
            log.error("vips dzsave failed with exit code ${result.exitCode}: ${result.stderr}")
            return new ImageTilerResults(false, 0)
        }

        // Parse the generated tiles and copy them to the tiler sink
        // For 'google' layout, tiles are directly in the base directory
        File tilesDir = new File(outputDir, 'tiles')
        if (!tilesDir.exists() || !tilesDir.isDirectory()) {
            // Some versions/layouts might still use _files suffix
            tilesDir = new File(outputDir, 'tiles_files')
        }

        if (!tilesDir.exists() || !tilesDir.isDirectory()) {
            log.error("vips did not create expected tiles directory: ${tilesDir.absolutePath}")
            return new ImageTilerResults(false, 0)
        }

        // Copy tiles from vips output to tiler sink
        int maxZoomLevel = 0
        tilesDir.eachDir { File levelDir ->
            int level = Integer.parseInt(levelDir.name)
            if (level < minLevel || level > maxLevel) {
                return
            }
            maxZoomLevel = Math.max(maxZoomLevel, level)

            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level)

            // Group files by column
            Map<Integer, List<File>> columnFiles = [:]
            levelDir.listFiles().each { File colDir ->
                if (colDir.isDirectory()) {
                    int col = Integer.parseInt(colDir.name)
                    List<File> rowFiles = colDir.listFiles().findAll { it.isFile() }.sort { it.name }
                    columnFiles[col] = rowFiles
                }
            }

            // Process each column
            columnFiles.each { int col, List<File> rowFiles ->
                TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, Integer.MAX_VALUE)

                rowFiles.each { File rowFile ->
                    int row = Integer.parseInt(rowFile.name.replace('.png', ''))
                    ByteSink tileSink = columnSink.getTileSink(row)

                    // Copy tile to sink
                    com.google.common.io.Files.asByteSource(rowFile).copyTo(tileSink)
                }
            }
        }

        return new ImageTilerResults(true, maxZoomLevel + 1)
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            dir.eachFileRecurse { File f ->
                if (f.isFile()) {
                    f.delete()
                }
            }
            dir.deleteDir()
        }
    }
}
