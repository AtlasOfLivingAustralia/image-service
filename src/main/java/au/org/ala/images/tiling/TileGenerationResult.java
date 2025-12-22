package au.org.ala.images.tiling;

import java.awt.image.BufferedImage;

/**
 * Result of a tile generation operation, indicating success or failure reason.
 * 
 * This allows callers to distinguish between different failure modes without
 * relying on exceptions or null checks.
 */
public class TileGenerationResult {
    
    private final Status status;
    private final BufferedImage tile;
    private final String message;
    
    /**
     * Status codes for tile generation operations.
     */
    public enum Status {
        /** Tile was successfully generated */
        SUCCESS,
        
        /** The requested tile coordinates are outside the image bounds */
        OUT_OF_BOUNDS,
        
        /** The requested zoom level is invalid (negative or exceeds pyramid depth) */
        INVALID_LEVEL,
        
        /** The input stream does not contain a valid image format */
        NOT_AN_IMAGE,
        
        /** No suitable image reader could be found for the image format */
        NO_IMAGE_READER,
        
        /** An I/O error occurred while reading the image */
        IO_ERROR,
        
        /** An unexpected error occurred during tile generation */
        INTERNAL_ERROR
    }
    
    private TileGenerationResult(Status status, BufferedImage tile, String message) {
        this.status = status;
        this.tile = tile;
        this.message = message;
    }
    
    /**
     * Create a successful result with the generated tile.
     */
    public static TileGenerationResult success(BufferedImage tile) {
        return new TileGenerationResult(Status.SUCCESS, tile, null);
    }
    
    /**
     * Create a failure result for out-of-bounds coordinates.
     */
    public static TileGenerationResult outOfBounds(int level, int x, int y, int maxX, int maxY) {
        String message = String.format(
            "Tile coordinates (%d,%d) out of bounds for level %d (valid: 0-%d, 0-%d)",
            x, y, level, maxX - 1, maxY - 1);
        return new TileGenerationResult(Status.OUT_OF_BOUNDS, null, message);
    }
    
    /**
     * Create a failure result for invalid zoom level.
     */
    public static TileGenerationResult invalidLevel(int level, int maxLevel) {
        String message = String.format(
            "Invalid level %d (valid range: 0-%d)", level, maxLevel - 1);
        return new TileGenerationResult(Status.INVALID_LEVEL, null, message);
    }
    
    /**
     * Create a failure result when input is not a valid image.
     */
    public static TileGenerationResult notAnImage() {
        return new TileGenerationResult(Status.NOT_AN_IMAGE, null, 
            "Input stream does not contain a valid image format");
    }
    
    /**
     * Create a failure result when no image reader is available.
     */
    public static TileGenerationResult noImageReader() {
        return new TileGenerationResult(Status.NO_IMAGE_READER, null, 
            "No suitable image reader found for the image format");
    }
    
    /**
     * Create a failure result for I/O errors.
     */
    public static TileGenerationResult ioError(String message) {
        return new TileGenerationResult(Status.IO_ERROR, null, message);
    }
    
    /**
     * Create a failure result for unexpected internal errors.
     */
    public static TileGenerationResult internalError(String message) {
        return new TileGenerationResult(Status.INTERNAL_ERROR, null, message);
    }
    
    /**
     * Get the status of the tile generation operation.
     */
    public Status getStatus() {
        return status;
    }
    
    /**
     * Check if the tile was successfully generated.
     */
    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }
    
    /**
     * Get the generated tile (only valid if status is SUCCESS).
     * 
     * @return The generated tile, or null if generation failed
     */
    public BufferedImage getTile() {
        return tile;
    }
    
    /**
     * Get a descriptive error message (only valid if status is not SUCCESS).
     * 
     * @return Error message, or null if successful
     */
    public String getMessage() {
        return message;
    }
    
    /**
     * Check if the failure was due to invalid coordinates or level
     * (as opposed to image reading errors).
     */
    public boolean isCoordinateError() {
        return status == Status.OUT_OF_BOUNDS || status == Status.INVALID_LEVEL;
    }
    
    /**
     * Check if the failure was due to image reading/format issues.
     */
    public boolean isImageError() {
        return status == Status.NOT_AN_IMAGE || 
               status == Status.NO_IMAGE_READER || 
               status == Status.IO_ERROR;
    }
    
    @Override
    public String toString() {
        if (isSuccess()) {
            return String.format("TileGenerationResult[SUCCESS, tile=%dx%d]", 
                tile.getWidth(), tile.getHeight());
        } else {
            return String.format("TileGenerationResult[%s, message=%s]", status, message);
        }
    }
}

