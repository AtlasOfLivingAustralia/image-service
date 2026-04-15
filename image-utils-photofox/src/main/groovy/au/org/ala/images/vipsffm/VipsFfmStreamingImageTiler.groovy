package au.org.ala.images.vipsffm

import app.photofox.vipsffm.VImage
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
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TileFormat

/**
 * Alternate FFM-based tiler that uses the lopcode/vips-ffm library.
 */
@Slf4j
@CompileStatic
class VipsFfmStreamingImageTiler implements IImageTiler {

    private final IImageTiler fallbackTiler
    private final int tileSize
    private final Executor ioExecutor
    private final Executor levelExecutor

    VipsFfmStreamingImageTiler(IImageTiler fallbackTiler, ImageTilerConfig config) {
        this.fallbackTiler = fallbackTiler
        this.tileSize = config.tileSize
        this.ioExecutor = config.ioExecutor
        this.levelExecutor = config.levelExecutor
        log.info("VipsFfmStreamingImageTiler initialized (lopcode/vips-ffm)")
    }

    @Override
    ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int level) throws IOException, InterruptedException {
        log.debug("Tiling image for level {} with vips-ffm", level)

        if (level < 0) {
            throw new IllegalArgumentException("Invalid level: " + level)
        }

        if (imageInputStream.markSupported()) {
            imageInputStream.mark(10 * 1024 * 1024)
        }

        try {
            return tileLevelWithVipsFfm(imageInputStream, tilerSink, level)
        } catch (Exception e) {
            log.error("vips-ffm single-level tiling failed, trying fallback", e)
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset()
                } catch (IOException resetException) {
                    log.warn("Failed to reset stream", resetException)
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, level)
        }
    }

    private ImageTilerResults tileLevelWithVipsFfm(InputStream imageInputStream, TilerSink tilerSink, int level) throws IOException {
        try (var arena = Arena.ofShared()) {
            VImage image = VImage.newFromStream(arena, imageInputStream)
            int width = image.width
            int height = image.height

            int maxLevel = (int) Math.ceil((double) (Math.log(Math.max(width, height) / (double) tileSize) / Math.log(2)))
            double scale = Math.pow(0.5d, (double) (maxLevel - level))

            VImage resizedImage = image.resize(scale)
            int resizedWidth = resizedImage.width
            int resizedHeight = resizedImage.height

            int cols = (int) Math.ceil(resizedWidth / (double) tileSize)
            int rows = (int) Math.ceil(resizedHeight / (double) tileSize)

            log.debug("Tiling level {}: {}x{} (scale {}), {}x{} tiles", level, resizedWidth, resizedHeight, scale, cols, rows)

            TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level)
            List<CompletableFuture<Void>> futures = []
            AtomicBoolean errorOccurred = new AtomicBoolean(false)

            for (int x = 0; x < cols; x++) {
                final int col = x
                TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, rows)

                for (int y = 0; y < rows; y++) {
                    final int row = y
                    if (errorOccurred.get()) break

                    final VImage finalResizedImage = resizedImage
                    CompletableFuture<Void> future = CompletableFuture.runAsync({
                        if (errorOccurred.get()) return

                        try {
                            int left = col * tileSize
                            int top = row * tileSize
                            int w = Math.min(tileSize, resizedWidth - left)
                            int h = Math.min(tileSize, resizedHeight - top)

                            VImage tileImage = finalResizedImage.extractArea(left, top, w, h)
                            ByteSink tileSink = columnSink.getTileSink(row)
                            
                            try (OutputStream os = tileSink.openStream()) {
                                tileImage.writeToStream(os, ".png")
                            }
                        } catch (Exception e) {
                            log.error("Error generating tile {}/{}", col, row, e)
                            errorOccurred.set(true)
                        }
                    } as Runnable, levelExecutor)
                    futures.add(future)
                }
            }

            CompletableFuture.allOf(futures as CompletableFuture[]).join()

            if (errorOccurred.get()) {
                throw new IOException("One or more tiles failed to generate")
            }

            return new ImageTilerResults(true, maxLevel + 1)
        }
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

            File tempOutDir = Files.createTempDirectory('tile-out-vipsffm-').toFile()

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
                File tilesDir = tilesBase
                if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                    // Some versions/layouts might still use _files suffix
                    tilesDir = new File(tempOutDir, 'tiles_files')
                }

                if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                    throw new IOException("vips_dzsave did not create expected tiles directory: ${tilesBase.absolutePath} or ${tilesDir.absolutePath}")
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
