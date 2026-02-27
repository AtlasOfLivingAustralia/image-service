package au.org.ala.images.metadata;

import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mp4.MP4Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

public class MP4MetadataExtractor extends AbstractMetadataParser {

    private static final Logger log = LoggerFactory.getLogger(MP4MetadataExtractor.class);

    @Override
    public Pattern getContentTypePattern() {
        return Pattern.compile("^(audio|application|video)/mp4$");
    }

    @Override
    public void extractMetadata(byte[] bytes, Map<String, String> metadata) {
        try (InputStream input = new UnsynchronizedByteArrayInputStream(bytes)) {
            extractMetadata(input, metadata);
        } catch (IOException e) {
            // This should not happen with UnsynchronizedByteArrayInputStream
        }
    }

    @Override
    public void extractMetadata(InputStream unopenedStream, Map<String, String> md) {
        // Do not close the provided stream here; the caller owns the lifecycle.
        InputStream input = unopenedStream;
        try {
            ContentHandler handler = new DefaultHandler();
            Metadata metadata = new Metadata();
            Parser parser = new MP4Parser();
            ParseContext parseCtx = new ParseContext();
            parser.parse(input, handler, metadata, parseCtx);

            // List all metadata
            String[] metadataNames = metadata.names();

            for(String name : metadataNames) {
                md.put(name, metadata.get(name));
            }

        } catch (Exception e) {
            log.error("Exception extracting MP4 metadata", e);
        }
    }
}
