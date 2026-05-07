package au.org.ala.images.tiling;

/**
 * Information about the tile pyramid structure.
 */
public class TilePyramidInfo {
    private final int imageWidth;
    private final int imageHeight;
    private final int[] pyramid;
    private final int tileSize;

    public TilePyramidInfo(int imageWidth, int imageHeight, int[] pyramid, int tileSize) {
        this.imageWidth = imageWidth;
        this.imageHeight = imageHeight;
        this.pyramid = pyramid;
        this.tileSize = tileSize;
    }

    public int getImageWidth() {
        return imageWidth;
    }

    public int getImageHeight() {
        return imageHeight;
    }

    public int getLevels() {
        return pyramid.length;
    }

    public int getSubsampleForLevel(int level) {
        return pyramid[level];
    }

    public int getTilesXForLevel(int level) {
        int subsample = pyramid[level];
        int levelWidth = (int) Math.ceil((double) imageWidth / subsample);
        return (int) Math.ceil((double) levelWidth / tileSize);
    }

    public int getTilesYForLevel(int level) {
        int subsample = pyramid[level];
        int levelHeight = (int) Math.ceil((double) imageHeight / subsample);
        return (int) Math.ceil((double) levelHeight / tileSize);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Image: %dx%d, Levels: %d, TileSize: %d\n", 
                imageWidth, imageHeight, pyramid.length, tileSize));
        for (int i = 0; i < pyramid.length; i++) {
            sb.append(String.format("  Level %d: subsample=%d, tiles=%dx%d\n",
                    i, pyramid[i], getTilesXForLevel(i), getTilesYForLevel(i)));
        }
        return sb.toString();
    }
}
