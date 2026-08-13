package au.org.ala.images.thumb;

import au.org.ala.images.util.ByteSinkFactory;
import com.google.common.io.ByteSource;
import com.google.errorprone.annotations.ThreadSafe;

import java.io.IOException;
import java.util.List;

/**
 * Interface for image thumbnailers that can generate thumbnails of various sizes.
 */
@ThreadSafe
public interface IImageThumbnailer {

    /**
     * Generate thumbnails from a byte source using the provided byte sink factory.
     * @param imageBytes The source image bytes.
     * @param byteSinkFactory Factory for creating byte sinks for each thumbnail.
     * @param thumbDefs List of thumbnail definitions specifying sizes and formats.
     * @return List of thumbnailing results.
     * @throws IOException If an error occurs during thumbnail generation.
     */
    List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException;
}
