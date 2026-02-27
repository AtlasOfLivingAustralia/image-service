package au.org.ala.images

import groovy.transform.Canonical
import org.apache.commons.io.input.BoundedInputStream

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * Represents an HTTP byte range request.
 */
@Canonical()
class Range {

    private static final String byteRangeSetRegex = '(\\s*((?<byteRangeSpec>(?<firstBytePos>\\d+)-(?<lastBytePos>\\d+)?)|(?<suffixByteRangeSpec>-(?<suffixLength>\\d+)))\\s*(,|$))'
    private static final String byteRangesSpecifierRegex = 'bytes=(?<byteRangeSet>' + byteRangeSetRegex + '+)'
    private static final Pattern byteRangeSetPattern = Pattern.compile(byteRangeSetRegex)
    private static final Pattern byteRangesSpecifierPattern = Pattern.compile(byteRangesSpecifierRegex)

    private static final InvalidRangeHeaderException INVALID_RANGE_HEADER_EXCEPTION = new InvalidRangeHeaderException("Invalid range header")


    private Long totalLength
    private Long start // inclusive
    private Long end // inclusive
    private Long suffixLength

    long start() {
        if (start != null) {
            start
        } else if (suffixLength != null) {
            totalLength - suffixLength
        } else {
            0
        }
    }

    long end() {
        if (end != null && end < (totalLength - 1)) {
            end
        } else {
            totalLength - 1
        }
    }

    boolean validate() {
        boolean valid = true
        if (start != null) {
            valid &= start >= 0
        }
        if (end != null) {
            valid &= end >= 0
        }
        if (suffixLength != null) {
            valid &= suffixLength >= 0
            valid &= start == null && end == null
        }
        if (start != null && end != null) {
            valid &= start <= end
        }
        return valid
    }

    long length() {
        if (end != null && start != null && end < totalLength) {
            end - start + 1
        } else if (start != null) {
            totalLength - start
        } else if (suffixLength != null) {
            suffixLength
        } else {
            totalLength
        }
    }

    /**
     * Whether this range request contains no defined range
     * @return
     */
    boolean isEmpty() {
        (start == null && suffixLength == null)
    }

    String contentRangeHeader() {
//        content-range: bytes 5793340-5793343/5793344
        if (!empty) {
            "bytes ${start()}-${end()}/$totalLength"
        }
        else {
            null
        }
    }

    static List<Range> decodeRange(String rangeHeader, long totalLength) {

        if (!rangeHeader) return [emptyRange(totalLength)]

        List<Range> ranges = new ArrayList<>()
        Matcher byteRangesSpecifierMatcher = byteRangesSpecifierPattern.matcher(rangeHeader?.trim())
        if (byteRangesSpecifierMatcher.matches()) {
            String byteRangeSet = byteRangesSpecifierMatcher.group("byteRangeSet")
            Matcher byteRangeSetMatcher = byteRangeSetPattern.matcher(byteRangeSet)
            while (byteRangeSetMatcher.find()) {
                Range range = new Range()
                range.totalLength = totalLength
                if (byteRangeSetMatcher.group("byteRangeSpec") != null) {
                    String start = byteRangeSetMatcher.group("firstBytePos")
                    String end = byteRangeSetMatcher.group("lastBytePos")
                    range.start = Long.valueOf(start)
                    range.end = end == null ? null : Long.valueOf(end)
                } else if (byteRangeSetMatcher.group("suffixByteRangeSpec") != null) {
                    range.suffixLength = Long.valueOf(byteRangeSetMatcher.group("suffixLength"))
                } else {
                    throw INVALID_RANGE_HEADER_EXCEPTION
                }
                if (!range.validate()) {
                    throw INVALID_RANGE_HEADER_EXCEPTION
                }
                ranges.add(range)
            }
        } else {
            throw INVALID_RANGE_HEADER_EXCEPTION
        }
        return ranges
    }

    static Range emptyRange(long totalLength) {
        return new Range(totalLength: totalLength)
    }

    InputStream wrapInputStream(InputStream inputStream) {
        InputStream is = inputStream
        if (!this.empty) {
            def start = this.start()
            // TODO does this need to be inside a use block?
            inputStream.skip(start)
            def end = this.end()
            if (end < totalLength - 1) {
                is = new BoundedInputStream(is, this.length())
            }
        }
        return is
    }

    static final class InvalidRangeHeaderException extends RuntimeException {
        InvalidRangeHeaderException(String message) {
            super(message)
        }

        @Override
        Throwable fillInStackTrace() {
            return this
        }
    }

    @Override
    String toString() {
        return "Range(${start()}..${end()})"
    }
}
