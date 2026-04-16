package au.org.ala.images

import groovy.util.logging.Slf4j
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils

@Slf4j
class DefaultStoragePathStrategy implements StoragePathStrategy {

    boolean forceUnixSeparator
    boolean forceAbsolutePath
    List<String> prefixes

    DefaultStoragePathStrategy(List<String> prefixes, boolean forceUnixSeparator, boolean forceAbsolutePath = true) {
        this.prefixes = prefixes.findAll() // remove empty
        this.forceUnixSeparator = forceUnixSeparator
        this.forceAbsolutePath = forceAbsolutePath
    }

    DefaultStoragePathStrategy(String prefix, boolean forceUnixSeparator, boolean forceAbsolutePath = true) {
        this(Arrays.asList(StringUtils.split(prefix, forceUnixSeparator ? '/' as char : File.separatorChar)), forceUnixSeparator, forceAbsolutePath)
    }

    @Override
    String basePath() {
        def l = new ArrayList(prefixes)
        generatePath(l)
    }

    @Override
    String createPathFromUUID(String uuid, String... postfix) {
        def l = new ArrayList(prefixes)
        computeAndAppendLocalDirectoryPath(uuid, l)
        l.addAll(postfix)
        l.findAll()
        return generatePath(l)
    }

    String generatePath(List<String> l) {
        def result
        if (forceUnixSeparator) {
            result = ensureAbsoluteIfRequired(FilenameUtils.normalize(l.join('/'), true), '/')
        } else {
            // use system separator instead
            result = ensureAbsoluteIfRequired(FilenameUtils.normalize(l.join(File.separator)), File.separator)
        }
        log.debug('Generated path {}', result)
        return result
    }

    String ensureAbsoluteIfRequired(String path, String separator) {
        if (forceAbsolutePath && !path.startsWith(separator)) {
            return separator + path
        } else {
            return path
        }
    }

    private static void computeAndAppendLocalDirectoryPath(String uuid, List bits) {
        for (int i = 1; i <= 4; ++i) {
            bits << uuid.charAt(uuid.length() - i);
        }
        bits << uuid // each image gets it's own directory
    }
}
