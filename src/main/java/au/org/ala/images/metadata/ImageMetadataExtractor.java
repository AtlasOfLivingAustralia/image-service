package au.org.ala.images.metadata;

import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A {@link au.org.ala.images.metadata.AbstractMetadataParser} that can extract metadata from most image types
 */
public class ImageMetadataExtractor extends AbstractMetadataParser {

    protected Logger logger = LoggerFactory.getLogger("ImageMetadataExtractor");

    @Override
    public Pattern getContentTypePattern() {
        return Pattern.compile("^image/(.*)$");
    }

    @Override
    public void extractMetadata(byte[] bytes, Map<String, String> md) {
        BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(bytes));
        try {
            Metadata metadata = ImageMetadataReader.readMetadata(bis, bytes.length);
            for (Directory directory : metadata.getDirectories()) {
                for (Tag tag : directory.getTags()) {
                    String key = tag.getTagName();
                    if (md.containsKey(key)) {
                        key = String.format("%s (%s)", tag.getTagName(), tag.getDirectoryName());
                    }
                    String value = directory.getDescription(tag.getTagType());
                    if (StringUtils.isNotEmpty(value) && value.startsWith("[") && value.endsWith("bytes]")) {
                        byte[] tagBytes = directory.getByteArray(tag.getTagType());
                        value = Base64.encodeBase64String(tagBytes);
                    }
                    md.put(key, value);
                }
            }
        } catch (Exception ex) {
            logger.debug(ex.getMessage(), ex);
        }
    }
}
