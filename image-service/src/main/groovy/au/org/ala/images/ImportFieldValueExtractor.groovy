package au.org.ala.images

import java.util.regex.Pattern

class ImportFieldValueExtractor {

    private static Map<String, Pattern> _regexCache

    static {
        _regexCache = [:]
    }

    static String extractValue(ImportFieldDefinition definition, File file) {
        switch (definition.fieldType) {
            case ImportFieldType.Literal:
                return definition.value
            case ImportFieldType.FilenameRegex:
                def pattern = getPattern(definition.value)
                def m = pattern.matcher(file.name)
                if (m.matches() && m.groupCount() > 0) {
                    return m.group(1)
                }
                break;
            default:
                throw new RuntimeException("Unhandled Field Type: ${definition.fieldType}")
        }
        return ""
    }

    private static synchronized Pattern getPattern(String regex) {
        if (_regexCache.containsKey(regex)) {
            return _regexCache[regex]
        }
        def pattern = Pattern.compile(regex)
        _regexCache[regex] = pattern
        return pattern
    }
}
