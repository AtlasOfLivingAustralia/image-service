package au.org.ala.images

import spock.lang.Specification

class RangeSpec extends Specification {

    def "can parse a byte range"() {
        setup:
        def rangeHeader = "bytes=200-1000"
        def totalLength = 2000

        when:
        def ranges = Range.decodeRange(rangeHeader, totalLength)

        then:
        ranges.size() == 1
        ranges[0].start == 200
        ranges[0].end == 1000
        ranges[0].start() == 200
        ranges[0].end() == 1000
        ranges[0].length() == 801
    }

    def "can parse byte ranges"() {
        setup:
        def rangeHeader = " bytes=200-1000, 2000-6576, 19000-,  -1000 "
        def totalLength = 20000

        when:
        def ranges = Range.decodeRange(rangeHeader, totalLength)

        then:
        ranges.size() == 4
        ranges[0].start == 200
        ranges[0].end == 1000
        ranges[0].start() == 200
        ranges[0].end() == 1000
        ranges[0].length() == 801

        ranges[1].start == 2000
        ranges[1].end == 6576
        ranges[1].start() == 2000
        ranges[1].end() == 6576
        ranges[1].length() == 4577

        ranges[2].start == 19000
        ranges[2].end == null
        ranges[2].start() == 19000
        ranges[2].end() == 19999
        ranges[2].length() == 1000

        ranges[3].suffixLength == 1000
        ranges[3].start == null
        ranges[3].end == null
        ranges[3].start() == 19000
        ranges[3].end() == 19999
        ranges[3].length() == 1000
    }

    def "empty range header throws"() {
        setup:
        def totalLength = 1000

        when:
        Range.decodeRange('bytes=', totalLength)

        then:
        thrown(Range.InvalidRangeHeaderException)

        when:
        def ranges = Range.decodeRange('', totalLength)

        then:
        ranges.size() == 1
        ranges[0].empty == true
    }

}
