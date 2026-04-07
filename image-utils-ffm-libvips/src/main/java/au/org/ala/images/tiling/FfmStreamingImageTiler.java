package au.org.ala.images.tiling;

import au.org.ala.images.ffm.InputStreamVipsSourceFFM;
import au.org.ala.images.ffm.NativeLibraryDetectorFFM;
import au.org.ala.images.ffm.VipsLibraryFFM;
import com.google.common.io.ByteSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * FFM-based tiler that uses libvips directly via the Foreign Function & Memory API.
 */
public class FfmStreamingImageTiler implements IImageTiler {

    private static final Logger log = LoggerFactory.getLogger(FfmStreamingImageTiler.class);

    private final VipsLibraryFFM vips;
    private final IImageTiler fallbackTiler;
    private final int tileSize;

    public FfmStreamingImageTiler(IImageTiler fallbackTiler) {
        this(fallbackTiler, 256);
    }

    public FfmStreamingImageTiler(IImageTiler fallbackTiler, int tileSize) {
        this.fallbackTiler = fallbackTiler;
        this.tileSize = tileSize;
        this.vips = NativeLibraryDetectorFFM.getVipsLibrary();

        if (vips != null) {
            log.info("FfmStreamingImageTiler initialized with native libvips (FFM)");
        } else {
            log.info("FfmStreamingImageTiler: libvips not available, will use fallback");
        }
    }

    @Override
    public ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        if (vips == null) {
            log.debug("Using fallback tiler");
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }

        if (minLevel < 0 || maxLevel < 0 || minLevel > maxLevel) {
            throw new IllegalArgumentException("Invalid min/max levels");
        }

        if (imageInputStream.markSupported()) {
            imageInputStream.mark(Integer.MAX_VALUE);
        }

        try {
            return tileWithVipsFFM(imageInputStream, tilerSink, minLevel, maxLevel);
        } catch (Exception e) {
            log.error("FFM tiling failed, trying fallback", e);
            if (imageInputStream.markSupported()) {
                try {
                    imageInputStream.reset();
                } catch (IOException resetEx) {
                    log.warn("Failed to reset image input stream after FFM tiling failure; proceeding with fallback anyway", resetEx);
                }
            }
            return fallbackTiler.tileImage(imageInputStream, tilerSink, minLevel, maxLevel);
        }
    }

    private ImageTilerResults tileWithVipsFFM(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException {
        InputStreamVipsSourceFFM vipsSource = null;
        MemorySegment inputImage = null;
        Path tempOutDir = null;

        try {
            vipsSource = new InputStreamVipsSourceFFM(vips, imageInputStream);
            inputImage = vips.vipsImageNewFromSource(vipsSource.getSource(), "");
            if (inputImage == null || inputImage.address() == 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("Failed to load image from source with libvips: " + error);
            }

            log.trace("Loaded image from VipsSource, streamed {} bytes", vipsSource.getPosition());

            tempOutDir = Files.createTempDirectory("tile-out-ffm-");
            File tilesBase = tempOutDir.resolve("tiles").toFile();

            int result = vips.vipsDzsave(inputImage, tilesBase.getAbsolutePath());
            if (result != 0) {
                String error = vips.vipsErrorBuffer();
                vips.vipsErrorClear();
                throw new IOException("vips_dzsave failed: " + error);
            }

            File tilesDir = tempOutDir.resolve("tiles_files").toFile();
            if (!tilesDir.exists() || !tilesDir.isDirectory()) {
                throw new IOException("vips_dzsave did not create expected tiles directory: " + tilesDir.getAbsolutePath());
            }

            int maxZoomLevel = 0;
            File[] levelDirs = tilesDir.listFiles(File::isDirectory);
            if (levelDirs != null) {
                for (File levelDir : levelDirs) {
                    int level = Integer.parseInt(levelDir.getName());
                    if (level < minLevel || level > maxLevel) {
                        continue;
                    }
                    maxZoomLevel = Math.max(maxZoomLevel, level);

                    TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);

                    File[] colDirs = levelDir.listFiles(File::isDirectory);
                    if (colDirs != null) {
                        for (File colDir : colDirs) {
                            int col = Integer.parseInt(colDir.getName());
                            TilerSink.ColumnSink columnSink = levelSink.getColumnSink(col, 0, Integer.MAX_VALUE);

                            File[] rowFiles = colDir.listFiles(File::isFile);
                            if (rowFiles != null) {
                                for (File rowFile : rowFiles) {
                                    int row = Integer.parseInt(rowFile.getName().replace(".png", "").replace(".jpg", ""));
                                    ByteSink tileSink = columnSink.getTileSink(row);
                                    com.google.common.io.Files.asByteSource(rowFile).copyTo(tileSink);
                                }
                            }
                        }
                    }
                }
            }

            return new ImageTilerResults(true, maxZoomLevel + 1);

        } catch (Throwable e) {
            if (e instanceof IOException) throw (IOException) e;
            throw new IOException("Error tiling image with FFM", e);
        } finally {
            if (inputImage != null && inputImage.address() != 0) {
                try {
                    vips.gObjectUnref(inputImage);
                } catch (Throwable e) {
                    log.warn("Error unreffing input image", e);
                }
            }
            if (vipsSource != null) {
                vipsSource.close();
            }
            if (tempOutDir != null) {
                deleteDirectory(tempOutDir);
            }
        }
    }

    private void deleteDirectory(Path path) {
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        } catch (IOException e) {
            log.warn("Failed to delete temp directory: " + path, e);
        }
    }
}
