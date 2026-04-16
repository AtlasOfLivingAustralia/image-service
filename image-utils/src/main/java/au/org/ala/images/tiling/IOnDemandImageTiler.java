package au.org.ala.images.tiling;

import com.google.errorprone.annotations.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for on-demand tile generation.
 */
@ThreadSafe
public interface IOnDemandImageTiler {

    /**
     * Generate a single tile and write it to the sink.
     *
     * @param imageInputStream Input stream for the source image
     * @param tilerSink Sink to write the tile to
     * @param level Zoom level
     * @param x Tile X coordinate
     * @param y Tile Y coordinate
     * @return TileGenerationResult indicating success or failure
     */
    TileGenerationResult generateTile(InputStream imageInputStream, TilerSink tilerSink, int level, int x, int y);
}
