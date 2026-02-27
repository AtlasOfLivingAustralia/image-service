package au.org.ala.images.metadata;

import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mp3.Mp3Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A {@link au.org.ala.images.metadata.AbstractMetadataParser} that can extract tags from MP3 audio files
 */
public class MP3MetadataParser extends AbstractMetadataParser {

    public static final Logger log = LoggerFactory.getLogger(MP3MetadataParser.class);

    @Override
    public Pattern getContentTypePattern() {
        return Pattern.compile("^audio/mp3$|^audio/mpeg$");
    }

    @Override
    public void extractMetadata(byte[] bytes, Map<String, String> md) {
            try (InputStream input = new UnsynchronizedByteArrayInputStream(bytes)) {
                extractMetadata(input, md);
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
            Parser parser = new Mp3Parser();
            ParseContext parseCtx = new ParseContext();
            parser.parse(input, handler, metadata, parseCtx);

            // List all metadata
            String[] metadataNames = metadata.names();

            for(String name : metadataNames){
                md.put(name, metadata.get(name));
            }

        } catch (Exception e) {
            log.error("Exception extracting MP3 metadata", e);
        }
    }
}
