package au.org.ala.images.tiling;

import com.google.common.io.ByteSink;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class TilePadding {

    private TilePadding() {
    }

    public static boolean requiresPadding(boolean padTiles, int tileSize, int width, int height) {
        return padTiles && (width < tileSize || height < tileSize);
    }

    public static BufferedImage padTile(BufferedImage tile, int tileSize, TileFormat tileFormat, Color tileBackgroundColor) {
        if (!requiresPadding(true, tileSize, tile.getWidth(), tile.getHeight())) {
            return tile;
        }

        BufferedImage padded = new BufferedImage(
                tileSize,
                tileSize,
                tileFormat == TileFormat.PNG ? BufferedImage.TYPE_4BYTE_ABGR : BufferedImage.TYPE_3BYTE_BGR
        );
        Graphics2D graphics = padded.createGraphics();
        try {
            if (tileFormat != TileFormat.PNG) {
                graphics.setColor(tileBackgroundColor != null ? tileBackgroundColor : new Color(221, 221, 221));
                graphics.fillRect(0, 0, tileSize, tileSize);
            }
            graphics.drawImage(tile, 0, tileSize - tile.getHeight(), null);
        } finally {
            graphics.dispose();
        }
        return padded;
    }

    public static BufferedImage materializeTile(BufferedImage tile,
                                                int actualWidth,
                                                int actualHeight,
                                                int tileSize,
                                                TileFormat tileFormat,
                                                Color tileBackgroundColor,
                                                boolean padTiles) {
        int outputWidth = requiresPadding(padTiles, tileSize, actualWidth, actualHeight) ? tileSize : actualWidth;
        int outputHeight = requiresPadding(padTiles, tileSize, actualWidth, actualHeight) ? tileSize : actualHeight;
        BufferedImage output = new BufferedImage(
                outputWidth,
                outputHeight,
                tileFormat == TileFormat.PNG ? BufferedImage.TYPE_4BYTE_ABGR : BufferedImage.TYPE_3BYTE_BGR
        );
        Graphics2D graphics = output.createGraphics();
        try {
            if (tileFormat != TileFormat.PNG && (padTiles || outputWidth != actualWidth || outputHeight != actualHeight)) {
                graphics.setColor(tileBackgroundColor != null ? tileBackgroundColor : new Color(221, 221, 221));
                graphics.fillRect(0, 0, outputWidth, outputHeight);
            }
            if (tile != null) {
                graphics.drawImage(tile, 0, outputHeight - actualHeight, null);
            }
        } finally {
            graphics.dispose();
        }
        return output;
    }

    public static void writeImage(ByteSink tileSink, BufferedImage image, TileFormat tileFormat) throws IOException {
        String format = tileFormat == TileFormat.PNG ? "png" : "jpeg";
        try (OutputStream tileStream = tileSink.openStream()) {
            if (!ImageIO.write(image, format, tileStream)) {
                throw new IOException("Failed to write tile");
            }
        }
    }

    public static void writeEncodedTile(ByteSink tileSink,
                                        byte[] encodedTile,
                                        int tileWidth,
                                        int tileHeight,
                                        int tileSize,
                                        TileFormat tileFormat,
                                        Color tileBackgroundColor,
                                        boolean padTiles) throws IOException {
        if (!requiresPadding(padTiles, tileSize, tileWidth, tileHeight)) {
            try (OutputStream outputStream = tileSink.openStream()) {
                outputStream.write(encodedTile);
            }
            return;
        }

        BufferedImage decoded = ImageIO.read(new ByteArrayInputStream(encodedTile));
        if (decoded == null) {
            throw new IOException("Failed to decode tile for padding");
        }

        BufferedImage padded = padTile(decoded, tileSize, tileFormat, tileBackgroundColor);
        try {
            writeImage(tileSink, padded, tileFormat);
        } finally {
            if (padded != decoded) {
                padded.flush();
            }
            decoded.flush();
        }
    }
}
