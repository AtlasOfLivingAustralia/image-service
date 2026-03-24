package au.org.ala.images.vipsffm

import app.photofox.vipsffm.VImage
import app.photofox.vipsffm.VSource
import app.photofox.vipsffm.VipsOption
import app.photofox.vipsffm.enums.VipsForeignDzDepth
import app.photofox.vipsffm.enums.VipsForeignDzLayout
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.ImageTilerResults
import au.org.ala.images.tiling.TilerSink
import com.google.common.io.ByteSink
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.foreign.Arena
import java.nio.file.Files

/**
 * Alternate FFM-based tiler that uses the lopcode/vips-ffm library.
 */
@Slf4j
@CompileStatic
class VipsFfmStreamingImageTiler implements IImageTiler {

    private final IImageTiler fallbackTiler
    private final int tileSize

    VipsFfmStreamingImageTiler(IImageTiler fallbackTiler, int tileSize = 256) {
        this.fallbackTiler = fallbackTiler
        this.tileSize = tileSize
        log.info("VipsFfmStreamingImageTiler initialized (lopcode/vips-ffm)")
    }

    @Override
    ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        log.debug("Tiling image with vips-ffm: minLevel={}, maxLevel={}", minLevel, maxLevel)

        // Establish a reset point for the fallback tiler when supported
        if (imageInputStream.markSupported()) {
            imageInputStream.mark(Integer.MAX_VALUE)
        }
        try {
            return tileWithVipsFfm(imageInputStream, tilerSink, minLevel, maxLevel)
        } catch (Exception e) {
            log.error("vips-ffm tiling failed, trying fallback", e)
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset()
                } catch (IOException resetException) {
                    log.warn("Failed to reset image input stream after vips-ffm tiling failure; proceeding with fallback tiler", resetException)
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel)
        }
    }

    private ImageTilerResults tileWithVipsFfm(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        try (var arena = Arena.ofConfined()) {
            VImage image = VImage.newFromStream(arena, imageInputStream)

            File tempOutDir = File.createTempFile('tile-out-vipsffm-', '', new File(System.getProperty('java.io.tmpdir')))
            tempOutDir.delete()
            tempOutDir.mkdirs()

            try {
                File tilesBase = new File(tempOutDir, 'tiles')

                // Call vips_dzsave to generate tiles
                image.dzsave(tilesBase.absolutePath,
                        VipsOption.Int("tile-size", tileSize),
                        VipsOption.Int("overlap", 0),
                        VipsOption.String("suffix", ".png"),
                        VipsOption.Enum("depth", VipsForeignDzDepth.FOREIGN_DZ_DEPTH_ONETILE),
                        VipsOption.Enum("layout", VipsForeignDzLayout.FOREIGN_DZ_LAYOUT_GOOGLE)
                )

                // Parse the generated tiles and copy them to the tiler sink
                File tilesDir = new File(tempOutDir, 'tiles_files')
                if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                    throw new IOException("vips_dzsave did not create expected tiles directory: ${tilesDir.absolutePath}")
                }

                int maxZoomLevel = 0
                tilesDir.eachDir { File levelDir ->
                    int level = Integer.parseInt(levelDir.name)
                    if (level < minLevel || level > maxLevel) {
                        return
                    }
                    maxZoomLevel = Math.max(maxZoomLevel, level)

                    TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level)

                    levelDir.eachDir { File colDir ->
                        int col = Integer.parseInt(colDir.name)
                        TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, Integer.MAX_VALUE)

                        colDir.eachFile { File rowFile ->
                            if (rowFile.isFile()) {
                                int row = Integer.parseInt(rowFile.name.replace('.png', ''))
                                ByteSink tileSink = columnSink.getTileSink(row)
                                com.google.common.io.Files.asByteSource(rowFile).copyTo(tileSink)
                            }
                        }
                    }
                }

                return new ImageTilerResults(true, maxZoomLevel + 1)
            } finally {
                deleteDirectory(tempOutDir)
            }
        }
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
