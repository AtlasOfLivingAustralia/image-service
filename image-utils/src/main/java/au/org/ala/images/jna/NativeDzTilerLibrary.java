package au.org.ala.images.jna;

import com.sun.jna.Callback;
import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

/**
 * JNA interface for the lightweight native dzsave-like tiler bridge.
 */
public interface NativeDzTilerLibrary extends Library {

    interface TileCallback extends Callback {
        int invoke(int level, int x, int y, String contentType, Pointer data, long length, Pointer userData);
    }

    int ala_vips_google_tms_tiles_from_source(
            Pointer inputSource,
            int[] subsamples,
            int levelCount,
            int tileSize,
            int minLevel,
            int maxLevel,
            String suffix,
            int jpegQuality,
            int pngCompression,
            boolean padTiles,
            double backgroundRed,
            double backgroundGreen,
            double backgroundBlue,
            TileCallback callback,
            Pointer userData,
            PointerByReference errorOut
    );

    @Deprecated
    default int ala_vips_google_tms_tiles_from_file(
            Pointer inputSource,
            int[] subsamples,
            int levelCount,
            int tileSize,
            int minLevel,
            int maxLevel,
            String suffix,
            int jpegQuality,
            int pngCompression,
            boolean padTiles,
            double backgroundRed,
            double backgroundGreen,
            double backgroundBlue,
            TileCallback callback,
            Pointer userData,
            PointerByReference errorOut
    ) {
        return ala_vips_google_tms_tiles_from_source(
                inputSource,
                subsamples,
                levelCount,
                tileSize,
                minLevel,
                maxLevel,
                suffix,
                jpegQuality,
                pngCompression,
                padTiles,
                backgroundRed,
                backgroundGreen,
                backgroundBlue,
                callback,
                userData,
                errorOut
        );
    }

    void ala_vips_google_tms_free_error(Pointer errorMessage);
}
