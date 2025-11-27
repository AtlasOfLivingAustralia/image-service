package au.org.ala.images.metadata;

import au.org.ala.images.util.FastByteArrayInputStream;
import com.google.common.io.ByteSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

public class MetadataExtractor {

    public static final Logger log = LoggerFactory.getLogger(MetadataExtractor.class);

    private static List<AbstractMetadataParser> _REGISTRY;

    static {
        _REGISTRY = new ArrayList<AbstractMetadataParser>();

        register(new ImageMetadataExtractor());
        register(new MP3MetadataParser());
        register(new MP4MetadataExtractor());
    }

    private static void register(AbstractMetadataParser parser) {
        _REGISTRY.add(parser);
    }

    public Map<String, String> readMetadata(File file) throws IOException {
        return readMetadata(FileUtils.readFileToByteArray(file), file.getName());
    }

    /**
     * Reads metadata from a byte array
     * @param bytes The source bytes
     * @param filename The name of the file (used for content type detection)
     * @return A map of metadata key value pairs
     */
    public Map<String, String> readMetadata(byte[] bytes, String filename) {
        return readMetadata(ByteSource.wrap(bytes), filename);
    }

    /**
     * Reads metadata from a byte source
     * @param byteSource The source of the bytes
     * @param filename The name of the file (used for content type detection)
     * @return A map of metadata key value pairs
     */
    public Map<String, String> readMetadata(ByteSource byteSource, String filename) {
        try {
            return readMetadata(byteSource.openBufferedStream(), filename);
        } catch (IOException ex) {
            log.error("Exception occurred opening stream for reading metadata", ex);
            return new HashMap<>();
        }
    }

    /**
     * Reads metadata from an input stream
     * @param inputStream This stream will be closed after reading
     * @param filename The name of the file (used for content type detection)
     * @return A map of metadata key value pairs
     */
    public Map<String, String> readMetadata(InputStream inputStream, String filename) {
        Map<String, String> map = new HashMap<>();
        try (BufferedInputStream bis = inputStream instanceof BufferedInputStream ?
                (BufferedInputStream) inputStream :
                new BufferedInputStream(inputStream)) {
            // Mark the stream before content type detection so we can reset it for the parser
            // Choose a reasonable upper bound for sniffing; 64KB should cover most detectors
            bis.mark(64 * 1024);
            String contentType = detectContentTypeInternal(bis, filename);
            if (StringUtils.isNotEmpty(contentType)) {
                for (AbstractMetadataParser p : _REGISTRY) {
                    Matcher m = p.getContentTypePattern().matcher(contentType);
                    if (m.matches()) {
                        // Reset to the mark so the parser sees the stream from the beginning
                        bis.reset();
                        p.extractMetadata(bis, map);
                        break;
                    }
                }
            }
            // Consume the stream to allow eg S3 to reuse underlying connections
            IOUtils.consume(bis);
        } catch (Exception ex) {
            log.error("Exception occurred reading metadata", ex);
        }
        return map;
    }

    public String detectContentType(ByteSource byteSource, String filename) {
        try (InputStream bis = byteSource.openBufferedStream()) {
            String result = detectContentTypeInternal(bis, filename);
            IOUtils.consume(bis);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred detecting content type", ex);
        }
        return null;
    }

    public String detectContentType(byte[] bytes, String filename) {
        try (InputStream bais = new FastByteArrayInputStream(bytes);
             InputStream bis = new BufferedInputStream(bais)) {
            return detectContentTypeInternal(bis, filename);
        } catch (Exception ex) {
            log.error("Exception occurred detecting content type", ex);
        }
        return null;
    }

    private String detectContentTypeInternal(InputStream inputStream, String filename) throws IOException {
        AutoDetectParser parser = new AutoDetectParser();
        Detector detector = parser.getDetector();

        Metadata md = new Metadata();
        if (filename != null) {
            md.add(TikaCoreProperties.RESOURCE_NAME_KEY, filename);
        }
        MediaType mediaType = detector.detect(inputStream, md);
        return mediaType.toString();
    }

}
