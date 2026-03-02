package au.org.ala.images.tiling

import au.org.ala.images.jna.InputStreamVipsSource
import au.org.ala.images.jna.NativeLibraryDetector
import au.org.ala.images.jna.VipsLibrary
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.ImageTilerResults
import au.org.ala.images.tiling.TilerSink
import com.google.common.io.ByteSink
import com.sun.jna.Pointer
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Experimental JNA-based tiler that uses libvips directly via JNA.
 * This avoids process spawning overhead and can be more efficient for high-throughput scenarios.
 *
 * Automatically falls back to process-based approach if libvips is not available.
 */
@Slf4j
@CompileStatic
class JnaStreamingImageTiler implements IImageTiler {

    private final VipsLibrary vips
    private final IImageTiler fallbackTiler
    private final int tileSize

    JnaStreamingImageTiler(IImageTiler fallbackTiler, int tileSize = 256) {
        this.fallbackTiler = fallbackTiler
        this.tileSize = tileSize
        this.vips = NativeLibraryDetector.getVipsLibrary()

        if (vips) {
            log.info("JnaStreamingImageTiler initialized with native libvips")
        } else {
            log.info("JnaStreamingImageTiler: libvips not available, will use fallback")
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

        try {
            return tileWithVipsJna(imageInputStream, tilerSink, minLevel, maxLevel)
        } catch (Exception e) {
            log.error("JNA tiling failed, trying fallback", e)
            // Reset the input stream if possible
            if (imageInputStream.markSupported()) {
                imageInputStream.reset()
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel)
        }
    }

    private ImageTilerResults tileWithVipsJna(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        InputStreamVipsSource vipsSource = null
        Pointer inputImage = null
        File tempOutDir = null

        try {
            // Create streaming source from InputStream - avoids loading entire image into memory
            vipsSource = new InputStreamVipsSource(vips, imageInputStream)

            // Load image from source - this streams the data
            inputImage = vips.vips_image_new_from_source(vipsSource.getSource(), "", null)
            if (inputImage == null || inputImage == Pointer.NULL) {
                String error = vips.vips_error_buffer()
                vips.vips_error_clear()
                throw new IOException("Failed to load image from source with libvips: ${error}")
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition())

            // Create temp directory for tiles output
            // Unfortunately, vips_dzsave doesn't have a memory-only mode, so we still need temp files
            tempOutDir = File.createTempFile('tile-out-jna-', '', new File(System.getProperty('java.io.tmpdir')))
            tempOutDir.delete()
            tempOutDir.mkdirs()

            File tilesBase = new File(tempOutDir, 'tiles')

            // Call vips_dzsave to generate tiles
            // Parameters: tile-size, overlap, suffix, depth, layout
            int result = vips.vips_dzsave(inputImage, tilesBase.absolutePath,
                    "tile-size", Integer.toString(tileSize),
                    "overlap", "0",
                    "suffix", ".png",
                    "depth", "onetile",
                    "layout", "google",
                    null)

            if (result != 0) {
                String error = vips.vips_error_buffer()
                vips.vips_error_clear()
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

        } finally {
            // Clean up vips objects
            if (inputImage != null && inputImage != Pointer.NULL) {
                vips.g_object_unref(inputImage)
            }

            // Clean up source
            if (vipsSource != null) {
                vipsSource.close()
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
