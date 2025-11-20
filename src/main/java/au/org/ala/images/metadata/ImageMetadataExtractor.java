package au.org.ala.images.metadata;

import au.org.ala.images.util.FastByteArrayInputStream;
import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A {@link au.org.ala.images.metadata.AbstractMetadataParser} that can extract metadata from most image types
 */
public class ImageMetadataExtractor extends AbstractMetadataParser {

    protected Logger logger = LoggerFactory.getLogger(ImageMetadataExtractor.class);

    @Override
    public Pattern getContentTypePattern() {
        return Pattern.compile("^image/(.*)$");
    }

    @Override
    public void extractMetadata(byte[] bytes, Map<String, String> md) {
        BufferedInputStream bis = new BufferedInputStream(new FastByteArrayInputStream(bytes));
        extractMetadata(bis, md);
    }

    @Override
    public void extractMetadata(InputStream inputStream, Map<String, String> md) {
        try {
            Metadata metadata = ImageMetadataReader.readMetadata(inputStream);
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
