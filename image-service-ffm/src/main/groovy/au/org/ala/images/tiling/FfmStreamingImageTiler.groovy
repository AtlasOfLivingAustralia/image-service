package au.org.ala.images.tiling

import au.org.ala.images.ffm.InputStreamVipsSourceFFM
import au.org.ala.images.ffm.NativeLibraryDetectorFFM
import au.org.ala.images.ffm.VipsLibraryFFM
import com.google.common.io.ByteSink
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.foreign.MemorySegment
import java.nio.file.Files

/**
 * FFM-based tiler that uses libvips directly via the Foreign Function & Memory API (Java 22+).
 * This replaces JNA with the modern Panama FFM API for better performance and integration.
 *
 * Automatically falls back to process-based approach if libvips is not available.
 */
@Slf4j
@CompileStatic
class FfmStreamingImageTiler implements IImageTiler {

    private final VipsLibraryFFM vips
    private final IImageTiler fallbackTiler
    private final int tileSize

    FfmStreamingImageTiler(IImageTiler fallbackTiler, int tileSize = 256) {
        this.fallbackTiler = fallbackTiler
        this.tileSize = tileSize
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary()

        if (vips) {
            log.info("FfmStreamingImageTiler initialized with native libvips (FFM)")
        } else {
            log.info("FfmStreamingImageTiler: libvips not available, will use fallback")
        }
    }

    @Override
    ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        // Fallback if libvips not available
        if (!vips) {
            log.debug("Using fallback tiler")
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel)
        }

        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels")
        }

        // Mark the stream so we can safely reset on FFM failure, if supported
        if (imageInputStream.markSupported()) {
            imageInputStream.mark(Integer.MAX_VALUE)
        }
        try {
            return tileWithVipsFFM(imageInputStream, tilerSink, minLevel, maxLevel)
        } catch (Exception e) {
            log.error("FFM tiling failed, trying fallback", e)
            // Reset the input stream if possible, but do not let failures prevent fallback
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset()
                } catch (IOException resetEx) {
                    log.warn("Failed to reset image input stream after FFM tiling failure; proceeding with fallback anyway", resetEx)
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel)
        }
    }

    private ImageTilerResults tileWithVipsFFM(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        InputStreamVipsSourceFFM vipsSource = null
        MemorySegment inputImage = null
        File tempOutDir = null

        try {
            // Create streaming source from InputStream - avoids loading entire image into memory
            vipsSource = new InputStreamVipsSourceFFM(vips, imageInputStream)

            // Load image from source - this streams the data
            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "")
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer()
                vips.vipsErrorClear()
                throw new IOException("Failed to load image from source with libvips: ${error}")
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition())

            // Create temp directory for tiles output
            // Unfortunately, vips_dzsave doesn't have a memory-only mode, so we still need temp files
            tempOutDir = Files.createTempDirectory('tile-out-ffm-').toFile()

            File tilesBase = new File(tempOutDir, 'tiles')

            // Call vips_dzsave to generate tiles
            // Note: This simplified version uses basic parameters
            // Full implementation with tile-size, overlap, etc. would require varargs support
            int result = vips.vipsDzsave(inputImage, tilesBase.absolutePath)

            if (result != 0) {
                String error = vips.vipsErrorBuffer()
                vips.vipsErrorClear()
                throw new IOException("vips_dzsave failed: ${error}")
            }

            // Parse the generated tiles and copy them to the tiler sink
            File tilesDir = new File(tempOutDir, 'tiles_files')
            if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                throw new IOException("vips_dzsave did not create expected tiles directory: ${tilesDir.absolutePath}")
            }

            // Copy tiles from vips output to tiler sink (same logic as process-based version)
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

        } catch (Throwable e) {
            if (e instanceof IOException) {
                throw e
            }
            throw new IOException("Error tiling image with FFM", e)
        } finally {
            // Clean up vips objects
            if (inputImage != null && inputImage.address() != 0) {
                try {
                    vips.gObjectUnref(inputImage)
                } catch (Throwable e) {
                    log.warn("Error unreffing input image", e)
                }
            }

            // Clean up source
            if (vipsSource != null) {
                try {
                    vipsSource.close()
                } catch (Exception e) {
                    log.warn("Error closing vips source", e)
                }
            }

            // Clean up temp directory
            if (tempOutDir != null) {
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
