package au.org.ala.images.metadata;

import java.util.Map;
import java.util.regex.Pattern;

public abstract class AbstractMetadataParser {

    public abstract Pattern getContentTypePattern();

    public abstract void extractMetadata(byte[] bytes, Map<String, String> metadata);

}
