package au.org.ala.images

import grails.testing.services.ServiceUnitTest
import spock.lang.Specification

class SanitiserServiceSpec extends Specification implements ServiceUnitTest<SanitiserService>{


    void "test output == sanitised(input)"(String input, String output) {

        expect:
        output == service.sanitise(input)

        where:
        input                                                        | output
        'Some Guy < some.guy@example.org >'                          | 'Some Guy &lt; some.guy&#64;example.org &gt;'
        '<a href="https://hello">A</a>'                              | '<a href="https://hello" rel="nofollow">A</a>'
        '<a href="https://hello" onclick="javascript:alert(1)">A</a>'| '<a href="https://hello" rel="nofollow">A</a>'
        '<a href="https://hello" onclick="javascript:alert(1)">'     | '<a href="https://hello" rel="nofollow"></a>'
        '\\<a onmouseover=alert(document.cookie)\\>xss link\\</a\\>' | '\\xss link\\'
        '<p>hello <b>there</b></p> <p>How <i>are</i> you?</p>'       | 'hello <b>there</b> How <i>are</i> you?'
    }

    void "test output == sanitised(input, imageId, key)"(String input, String output) {

        expect:
        output == service.sanitise(input, '1234-1234-1234', 'creator')

        where:
        input                                                        | output
        'Some Guy < some.guy@example.org >'                          | 'Some Guy &lt; some.guy&#64;example.org &gt;'
        '<a href="https://hello">A</a>'                              | '<a href="https://hello" rel="nofollow">A</a>'
        '<a href="https://hello" onclick="javascript:alert(1)">A</a>'| '<a href="https://hello" rel="nofollow">A</a>'
        '<a href="https://hello" onclick="javascript:alert(1)">'     | '<a href="https://hello" rel="nofollow"></a>'
        '\\<a onmouseover=alert(document.cookie)\\>xss link\\</a\\>' | '\\xss link\\'
        '<p>hello <b>there</b></p> <p>How <i>are</i> you?</p>'       | 'hello <b>there</b> How <i>are</i> you?'
    }

    void "test sanitisation with truncation"(String input, String output) {
        expect:

        output == service.truncateAndSanitise(input, 10)

        where:
        input                                                                                | output
        '\'"&&&&&&&&&&"\''                                                                   | '&#39;&#34;&amp;&amp;&amp;&amp;&amp;...'
        'hello <b>there</b></p> <p>How <i>are</i> you?'                                      | 'hello <b>t...</b>'
        '<p>hello <b>there</b></p> <p>How <i>are</i> you?</p>'                               | 'hello <b>t...</b>'
        '<p>hello <b>there</b> How <i>are</i> you?</p>'                                      | 'hello <b>t...</b>'
        '<a href="https://example.org">hello <b>there</b> How <i>are</i> you?</a>'           | '<a href="https://example.org" rel="nofollow">hello <b>t...</b></a>'
        '<a onmouseover=alert(document.cookie)\\>hello <b>there</b> How <i>are</i> you?</a>' | 'hello <b>t...</b>'
    }

    void "test sanitisation with truncation and context"(String input, String output) {
        expect:

        output == service.truncateAndSanitise(input, '1234-1234-1234-1234', 'creator', 10)

        where:
        input                                                                                | output
        '\'"&&&&&&&&&&"\''                                                                   | '&#39;&#34;&amp;&amp;&amp;&amp;&amp;...'
        'hello <b>there</b></p> <p>How <i>are</i> you?'                                      | 'hello <b>t...</b>'
        '<p>hello <b>there</b></p> <p>How <i>are</i> you?</p>'                               | 'hello <b>t...</b>'
        '<p>hello <b>there</b> How <i>are</i> you?</p>'                                      | 'hello <b>t...</b>'
        '<a href="https://example.org">hello <b>there</b> How <i>are</i> you?</a>'           | '<a href="https://example.org" rel="nofollow">hello <b>t...</b></a>'
        '<a onmouseover=alert(document.cookie)\\>hello <b>there</b> How <i>are</i> you?</a>' | 'hello <b>t...</b>'
    }
}
