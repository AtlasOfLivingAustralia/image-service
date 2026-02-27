package au.org.ala.images.tiling;

import au.org.ala.images.util.FileByteSinkFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for image tilers that can tile images into smaller tiles for various zoom levels.
 */
public interface IImageTiler {

    default ImageTilerResults tileImage(File imageFile, File destinationDirectory) throws IOException, InterruptedException {
        return tileImage(FileUtils.openInputStream(imageFile), new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(destinationDirectory)));
    }

    default ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink) throws IOException, InterruptedException {
        return tileImage(imageInputStream, tilerSink, 0, Integer.MAX_VALUE);
    }

    /**
     * Tile the image from the input stream and write tiles to the provided sink.
     * @param imageInputStream An input stream of the image to be tiled.  Should be bufferable and support mark/reset.
     * @param tilerSink The sink to write the tiles to.
     * @param minLevel The minimum zoom level to generate.
     * @param maxLevel The maximum zoom level to generate.
     * @return The results of the tiling operation.
     * @throws IOException If an error occurs during tiling.
     * @throws InterruptedException If the tiling operation is interrupted.
     */
    ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException;
}
