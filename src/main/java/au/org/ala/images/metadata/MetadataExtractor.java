package au.org.ala.images.metadata;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
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
    }

    private static void register(AbstractMetadataParser parser) {
        _REGISTRY.add(parser);
    }

    public Map<String, String> readMetadata(File file) throws IOException {
        return readMetadata(FileUtils.readFileToByteArray(file), file.getName());
    }

    public Map<String, String> readMetadata(byte[] bytes, String filename) {
        Map<String, String> map = new HashMap<String, String>();
        if (bytes != null && bytes.length > 0) {
            String contentType = detectContentType(bytes, filename);
            if (StringUtils.isNotEmpty(contentType)) {
                for (AbstractMetadataParser p : _REGISTRY) {
                    Matcher m = p.getContentTypePattern().matcher(contentType);
                    if (m.matches()) {
                        p.extractMetadata(bytes, map);
                        break;
                    }
                }
            }
        }
        return map;
    }

    public String detectContentType(byte[] bytes, String filename) {
        try (InputStream bais = new ByteArrayInputStream(bytes);
             InputStream bis = new BufferedInputStream(bais)) {

            AutoDetectParser parser = new AutoDetectParser();
            Detector detector = parser.getDetector();

            Metadata md = new Metadata();
            if (filename != null) {
                md.add(Metadata.RESOURCE_NAME_KEY, filename);
            }
            MediaType mediaType = detector.detect(bis, md);
            return mediaType.toString();
        } catch (Exception ex) {
            log.error("Exception occurred detecting content type", ex);
        }
        return null;
    }

}
