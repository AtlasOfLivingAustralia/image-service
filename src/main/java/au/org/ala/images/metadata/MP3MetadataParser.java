package au.org.ala.images.metadata;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mp3.Mp3Parser;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A {@link au.org.ala.images.metadata.AbstractMetadataParser} that can extract tags from MP3 audio files
 */
public class MP3MetadataParser extends AbstractMetadataParser {
    @Override
    public Pattern getContentTypePattern() {
        return Pattern.compile("^audio/mp3$|^audio/mpeg$");
    }

    @Override
    public void extractMetadata(byte[] bytes, Map<String, String> md) {
        try {
            InputStream input = new ByteArrayInputStream(bytes);
            ContentHandler handler = new DefaultHandler();
            Metadata metadata = new Metadata();
            Parser parser = new Mp3Parser();
            ParseContext parseCtx = new ParseContext();
            parser.parse(input, handler, metadata, parseCtx);
            input.close();

            // List all metadata
            String[] metadataNames = metadata.names();

            for(String name : metadataNames){
                md.put(name, metadata.get(name));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
