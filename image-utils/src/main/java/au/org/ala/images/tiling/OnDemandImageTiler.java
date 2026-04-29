package au.org.ala.images.tiling;

import au.org.ala.images.util.DefaultImageReaderSelectionStrategy;
import com.google.common.io.ByteSink;
import org.apache.commons.io.IOUtils;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * On-demand tile generator - generates a single tile at specific coordinates without
 * pre-generating the entire pyramid.
 *
 * Use Cases:
 * - Web tile servers with caching (tiles generated on first request, then cached)
 */
public class OnDemandImageTiler implements IOnDemandImageTiler {

    private static final Logger log = LoggerFactory.getLogger(OnDemandImageTiler.class);

    private final int tileSize;
    private final TileFormat tileFormat;
    private final Color tileBackgroundColor;
    private final boolean padTiles;
    private final ZoomFactorStrategy zoomFactorStrategy;

    private static final GraphicsEnvironment GRAPHICS_ENV =
            GraphicsEnvironment.getLocalGraphicsEnvironment();

    public OnDemandImageTiler(ImageTilerConfig config) {
        if (config != null) {
            this.tileSize = config.getTileSize();
            this.tileFormat = config.getTileFormat();
            this.tileBackgroundColor = config.getTileBackgroundColor();
            this.padTiles = config.isPadTiles();
            this.zoomFactorStrategy = config.getZoomFactorStrategy();
        } else {
            this.tileSize = 256;
            this.tileFormat = TileFormat.JPEG;
            this.tileBackgroundColor = Color.gray;
            this.padTiles = true;
            this.zoomFactorStrategy = new DefaultZoomFactorStrategy(this.tileSize);
        }
    }

    /**
     * Generate a single tile at the specified coordinates and write it to the TilerSink.
     * This method combines tile generation with immediate writing to the sink.
     *
     * @param imageInputStream Input stream for the source image
     * @param tilerSink Sink to write the generated tile to
     * @param level Zoom level (0 = most zoomed out, higher = more zoomed in)
     * @param x Tile X coordinate at this zoom level (column)
     * @param y Tile Y coordinate at this zoom level (row)
     * @return TileGenerationResult indicating success or failure reason
     */
    @Override
    public TileGenerationResult generateTile(InputStream imageInputStream, TilerSink tilerSink,
                                                       int level, int x, int y) {
        long startTime = System.nanoTime();

        try (ImageInputStream iis = ImageIO.createImageInputStream(imageInputStream)) {
            if (iis == null) {
                IOUtils.consume(imageInputStream);
                return TileGenerationResult.notAnImage();
            }

            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                IOUtils.consume(imageInputStream);
                return TileGenerationResult.notAnImage();
            }

            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                IOUtils.consume(imageInputStream);
                return TileGenerationResult.noImageReader();
            }

            try {
                reader.setInput(iis, true, false);

                // Get image dimensions
                int imageWidth = reader.getWidth(0);
                int imageHeight = reader.getHeight(0);

                // Calculate zoom pyramid
                int[] pyramid = zoomFactorStrategy.getZoomFactors(imageHeight, imageWidth);

                // Validate level
                if (level < 0 || level >= pyramid.length) {
                    log.warn("Invalid level {} (valid range: 0-{})", level, pyramid.length - 1);
                    return TileGenerationResult.invalidLevel(level, pyramid.length);
                }

                int subsample = pyramid[level];

                // Calculate dimensions at this zoom level
                int levelWidth = (int) Math.ceil((double) imageWidth / subsample);
                int levelHeight = (int) Math.ceil((double) imageHeight / subsample);

                // Calculate tile bounds at this level
                int tilesX = (int) Math.ceil((double) levelWidth / tileSize);
                int tilesY = (int) Math.ceil((double) levelHeight / tileSize);

                // Validate tile coordinates
                if (x < 0 || x >= tilesX || y < 0 || y >= tilesY) {
                    log.warn("Tile coordinates ({},{}) out of bounds for level {} (valid: 0-{}, 0-{})",
                            x, y, level, tilesX - 1, tilesY - 1);
                    return TileGenerationResult.outOfBounds(level, x, y, tilesX, tilesY);
                }

                BufferedImage tile = generateTileInternal(reader, imageWidth, imageHeight, subsample, x, y);

                try {
                    // Write to sink
                    TilerSink.LevelSink levelSink = tilerSink.getLevelSink(level);
                    TilerSink.ColumnSink columnSink = levelSink.getColumnSink(x, 0, 1);
                    ByteSink byteSink = columnSink.getTileSink(y);

                    // Write the tile
                    TilePadding.writeImage(byteSink, tile, tileFormat);

                    long endTime = System.nanoTime();
                    log.debug("Generated and wrote tile ({},{}) at level {} in {} ms",
                            x, y, level, (endTime - startTime) / 1_000_000);

                    return TileGenerationResult.success();

                } finally {
                    tile.flush();
                }

            } catch (IOException e) {
                log.error("I/O error reading image for tile generation", e);
                return TileGenerationResult.ioError(e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error during tile generation", e);
                return TileGenerationResult.internalError(e.getMessage());
            } finally {
                reader.dispose();
            }
        } catch (IOException e) {
            log.error("Failed to create ImageInputStream", e);
            return TileGenerationResult.ioError(e.getMessage());
        }
    }


    /**
     * Internal method to generate a single tile using subsampling and region reading.
     *
     * Strategy:
     * 1. Calculate the source region needed for this tile (in original image coordinates)
     * 2. Read ONLY that region with subsampling (minimizes I/O and memory)
     * 3. Resize the region to exact tile dimensions if needed
     * 4. Render to output tile format
     */
    private BufferedImage generateTileInternal(ImageReader reader, int imageWidth, int imageHeight,
                                              int subsample, int tileX, int tileY) throws IOException {
        int subsampledImageWidth;
        if (imageWidth % subsample == 0) {
            subsampledImageWidth = imageWidth / subsample;
        } else {
            subsampledImageWidth = (int) Math.ceil((double) imageWidth / subsample);
        }
        int subsampledImageHeight;
        if (imageHeight % subsample == 0) {
            subsampledImageHeight = imageHeight / subsample;
        } else {
            subsampledImageHeight = (int) Math.ceil((double) imageHeight / subsample);
        }

        int subsampledTileSize = tileSize * subsample;

        // Calculate tile position at the source level
        int tileLeftAtLevel = tileX * tileSize;
        int tileTopAtLevel = Math.max(0, subsampledImageHeight - (tileY+1) * tileSize);
        int tileRightAtLevel = Math.min(tileLeftAtLevel + tileSize, subsampledImageWidth);
        int tileBottomAtLevel = subsampledImageHeight - tileY * tileSize;

        // Calculate source region in ORIGINAL image coordinates
        int srcX = tileLeftAtLevel * subsample;
        int srcY = tileTopAtLevel * subsample;
        int srcWidth = Math.min((tileRightAtLevel - tileLeftAtLevel) * subsample, imageWidth - srcX);
        int srcHeight = (tileBottomAtLevel - tileTopAtLevel) * subsample; // TODO Math.min((tileBottomAtLevel - tileTopAtLevel) * subsample, imageHeight - srcY);

        log.debug("Reading region: srcX={}, srcY={}, srcWidth={}, srcHeight={}, subsample={}",
                srcX, srcY, srcWidth, srcHeight, subsample);

        // Read ONLY the required region with subsampling
        ImageReadParam params = reader.getDefaultReadParam();
        params.setSourceRegion(new Rectangle(srcX, srcY, srcWidth, srcHeight));
        params.setSourceSubsampling(subsample, subsample, 0, 0);

        BufferedImage sourceRegion = reader.read(0, params);

        // Calculate target dimensions for this tile
        int targetWidth = tileRightAtLevel - tileLeftAtLevel;
        int targetHeight = tileBottomAtLevel - tileTopAtLevel;

        log.debug("Source region: {}x{}, target: {}x{}", 
                sourceRegion.getWidth(), sourceRegion.getHeight(), targetWidth, targetHeight);

        // Resize if needed (handles edge cases where subsampling doesn't align perfectly)
        BufferedImage resized;
        if (sourceRegion.getWidth() != targetWidth || sourceRegion.getHeight() != targetHeight) {
            resized = Scalr.resize(sourceRegion, Scalr.Method.QUALITY, targetWidth, targetHeight);
            sourceRegion.flush();
        } else {
            resized = sourceRegion;
        }

        // Create output tile with proper format
        try {
            return TilePadding.materializeTile(
                    resized,
                    targetWidth,
                    targetHeight,
                    tileSize,
                    tileFormat,
                    tileBackgroundColor,
                    padTiles
            );
        } finally {
            resized.flush();
        }
    }

    /**
     * Get information about the tile pyramid for an image without generating tiles.
     */
    public TilePyramidInfo getPyramidInfo(InputStream imageInputStream) throws IOException {
        try (ImageInputStream iis = ImageIO.createImageInputStream(imageInputStream)) {
            if (iis == null) {
                throw new IOException("Failed to create ImageInputStream");
            }

            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                throw new IOException("No image readers for image");
            }

            ImageReader reader = DefaultImageReaderSelectionStrategy.INSTANCE.selectImageReader(readers);
            if (reader == null) {
                throw new IOException("No suitable image reader selected");
            }

            try {
                reader.setInput(iis, true, false);

                int imageWidth = reader.getWidth(0);
                int imageHeight = reader.getHeight(0);

                int[] pyramid = zoomFactorStrategy.getZoomFactors(imageHeight, imageWidth);

                return new TilePyramidInfo(imageWidth, imageHeight, pyramid, tileSize);

            } finally {
                reader.dispose();
            }
        }
    }
}
