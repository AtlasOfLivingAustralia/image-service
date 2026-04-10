package au.org.ala.images.tiling;

import au.org.ala.images.jna.InputStreamVipsSource;
import au.org.ala.images.jna.NativeLibraryDetector;
import au.org.ala.images.jna.VipsLibrary;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Experimental JNA-based tiler that uses libvips directly via JNA.
 * This avoids process spawning overhead and can be more efficient for high-throughput scenarios.
 *
 * Automatically falls back to process-based approach if libvips is not available.
 */
public class JnaStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(JnaStreamingImageTiler.class);

    private final VipsLibrary vips;
    private final IImageTiler fallbackTiler;
    private final int tileSize;

    public JnaStreamingImageTiler(IImageTiler fallbackTiler) {
        this(fallbackTiler, 256);
    }

    public JnaStreamingImageTiler(IImageTiler fallbackTiler, int tileSize) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = tileSize;
        this.vips = NativeLibraryDetector.getVipsLibrary();

        if (vips != null) {
            log.info("JnaStreamingImageTiler initialized with native libvips");
        } else {
            log.info("JnaStreamingImageTiler: libvips not available, will use fallback");
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        // Fallback if libvips not available
        if (vips == null) {
            log.debug("Using fallback tiler");
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }

        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        try {
            return tileWithVipsJna(imageInputStream, tilerSink, minLevel, maxLevel);
        } catch (Exception e) {
            log.error("JNA tiling failed, trying fallback", e);
            if (fallbackTiler == null) {
                throw e;
            }
            // Reset the input stream if possible
            if (imageInputStream.markSupported()) {
                imageInputStream.reset();
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }
    }

    private ImageTilerResults tileWithVipsJna(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        InputStreamVipsSource vipsSource = null;
        Pointer inputImage = null;
        File tempOutDir = null;

        try {
            // Create streaming source from InputStream - avoids loading entire image into memory
            vipsSource = new InputStreamVipsSource(vips, imageInputStream);

            // Load image from source - this streams the data
            inputImage = vips.vips_image_new_from_source(vipsSource.getSource(), "", (Object) null);
            if (inputImage == null || inputImage == Pointer.NULL) {
                String error = vips.vips_error_buffer();
                vips.vips_error_clear();
                throw new IOException("Failed to load image from source with libvips: " + error);
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition());

            // Create temp directory for tiles output
            // Unfortunately, vips_dzsave doesn't have a memory-only mode, so we still need temp files
            tempOutDir = File.createTempFile("tile-out-jna-", "", new File(System.getProperty("java.io.tmpdir")));
            tempOutDir.delete();
            tempOutDir.mkdirs();

            File tilesBase = new File(tempOutDir, "tiles");

            // Call vips_dzsave to generate tiles
            // Parameters: tile-size, overlap, suffix, depth, layout
            // Enum values: depth: onetile=1, layout: google=2
            int result = vips.vips_dzsave(inputImage, tilesBase.getAbsolutePath(),
                    "tile-size", tileSize,
                    "overlap", 0,
                    "suffix", ".png",
                    "depth", 1,
                    "layout", 2,
                    null);

            if (result != 0) {
                String error = vips.vips_error_buffer();
                vips.vips_error_clear();
                throw new IOException("vips_dzsave failed: " + error);
            }

            // Parse the generated tiles and copy them to the tiler sink
            // For 'google' layout, tiles are directly in the base directory
            File tilesDir = tilesBase;
            if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                // Some versions/layouts might still use _files suffix
                tilesDir = new File(tempOutDir, "tiles_files");
            }

            if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                throw new IOException("vips_dzsave did not create expected tiles directory: " + tilesBase.getAbsolutePath() + " or " + tilesDir.getAbsolutePath());
            }

            // Copy tiles from vips output to tiler sink (same logic as process-based version)
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

        } finally {
            // Clean up vips objects
            if (inputImage != null && inputImage != Pointer.NULL) {
                vips.g_object_unref(inputImage);
            }

            // Clean up source
            if (vipsSource != null) {
                vipsSource.close();
            }

            // Clean up temp directory
            if (tempOutDir != null) {
                deleteDirectory(tempOutDir);
            }
        }
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
