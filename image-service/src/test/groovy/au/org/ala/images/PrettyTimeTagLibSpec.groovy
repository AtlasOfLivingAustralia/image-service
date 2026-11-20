package au.org.ala.images

import grails.testing.web.taglib.TagLibUnitTest
import org.joda.time.DateTime
import org.ocpsoft.prettytime.PrettyTime
import spock.lang.Specification

class PrettyTimeTagLibSpec extends Specification implements TagLibUnitTest<PrettyTimeTagLib> {

    void 'display accepts Date Long and Joda DateTime values'() {
        given:
        Date date = new Date(System.currentTimeMillis() - 60_000)

        expect:
        renderPrettyTime(date) == renderPrettyTime(date.time)
        renderPrettyTime(date) == renderPrettyTime(new DateTime(date))
    }

    void 'display uses the request locale and supports capitalization'() {
        given:
        request.addPreferredLocale(Locale.FRENCH)
        Date date = new Date(System.currentTimeMillis() - 60_000)
        String expected = new PrettyTime(Locale.FRENCH).format(date).trim()

        expect:
        renderPrettyTime(date) == expected
        renderPrettyTime(date, [capitalize: true]) == expected.capitalize()
    }

    void 'display optionally appends a formatted time'() {
        given:
        Date date = new Date(System.currentTimeMillis() - 60_000)
        String relative = new PrettyTime(request.locale).format(date).trim()

        expect:
        renderPrettyTime(date, [showTime: true, format: 'yyyy']) == "${relative}, ${date.format('yyyy')}"
    }

    void 'display optionally wraps the result in an HTML5 time element'() {
        given:
        Date date = new Date(System.currentTimeMillis() - 60_000)
        String relative = new PrettyTime(request.locale).format(date).trim()
        tagLib.metaClass.getG = {
            new Expando(formatDate: { Map ignored -> 'formatted-date' })
        }

        when:
        String result = renderPrettyTime(date, [html5wrapper: true])

        then:
        result == "<time datetime=\"formatted-date\" title=\"formatted-date\">${relative}</time>"
    }

    void 'display renders nothing when the date is absent'() {
        expect:
        renderPrettyTime(null) == ''
    }

    private String renderPrettyTime(Object date, Map options = [:]) {
        tagLib.display([date: date] + options).toString()
    }
}
