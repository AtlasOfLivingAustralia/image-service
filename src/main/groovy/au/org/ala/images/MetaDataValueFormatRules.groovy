package au.org.ala.images

import java.util.regex.Pattern

class MetaDataValueFormatRules  {

    def grailsApplication

    public MetaDataValueFormatRules(grailsApplication){
        this.grailsApplication = grailsApplication
    }

    private List<MetaDataValueFormatter> RULES = [
        MatchName("occurrenceId") { "<a href=\"${grailsApplication.config.getProperty('biocache.baseURL')}/occurrences/${it}\">${it}</a>" },
        MatchName("dataResourceUid") { "<a href=\"${grailsApplication.config.getProperty('collectory.baseURL')}/public/show/${it}\">${it}</a>" },
        MatchName("scientificName") { "<a href=\"${grailsApplication.config.getProperty('bie.baseURL')}/search?q=${it?.encodeAsURL()}\">${it}</a>"},
        MatchValueRegex("http://.*") { "<a href=\"${it}\">${it}</a>" },
        MatchValueRegex("https://.*") { "<a href=\"${it}\">${it}</a>" }
    ]

    public String formatValue(ImageMetaDataItem md) {
        def formatter = RULES.find { it.canFormat(md) }
        if (formatter) {
            return formatter.format(md)
        }
        return md.value
    }

    private static MetaDataValueFormatter MatchName(String name, Closure<String> func) {
        return new NameMatchingFormatter(name, func)
    }

    private static MetaDataValueFormatter MatchValueRegex(String regex, Closure<String> func) {
        return new ValueRegexMatchingFormatter(regex, func)
    }

}

public interface MetaDataValueFormatter {
    boolean canFormat(ImageMetaDataItem md)
    String format(ImageMetaDataItem md)
}

public abstract class MetaDataValueFormatterBase implements MetaDataValueFormatter {

    private Closure _formatFunc

    public MetaDataValueFormatterBase(Closure<String> formattingFunc) {
        _formatFunc = formattingFunc
    }

    @Override
    public String format(ImageMetaDataItem md) {
        if (_formatFunc) {
            return _formatFunc(md?.value)
        } else {
            return md?.value
        }
    }


}

class ValueRegexMatchingFormatter extends MetaDataValueFormatterBase {


    private Pattern _pattern

    public ValueRegexMatchingFormatter(String regex, Closure<String> formattingFunc) {
        super(formattingFunc)
        _pattern = Pattern.compile(regex)
    }

    @Override
    boolean canFormat(ImageMetaDataItem md) {
        def matcher = _pattern.matcher(md.value)
        return matcher.matches()
    }

}

class NameMatchingFormatter extends MetaDataValueFormatterBase {

    private String _name

    public NameMatchingFormatter(String name, Closure<String> formattingFunc) {
        super(formattingFunc)
        _name = name
    }

    @Override
    boolean canFormat(ImageMetaDataItem md) {
        return md?.name?.equalsIgnoreCase(_name)
    }

}
