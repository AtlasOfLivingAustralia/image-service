package au.org.ala.images.jna

import com.google.common.io.ByteSource
import com.sun.jna.Pointer
import spock.lang.Specification
import groovy.util.logging.Slf4j

@Slf4j
class InputStreamVipsSourceSpec extends Specification {

    static class LimitingMarkInputStream extends FilterInputStream {
        int readlimit = -1
        int bytesSinceMark = 0
        LimitingMarkInputStream(InputStream delegateStream) { super(delegateStream) }
        @Override boolean markSupported() { true }
        @Override void mark(int rl) { this.readlimit = rl; this.bytesSinceMark = 0; super.mark(rl) }
        @Override int read(byte[] b, int off, int len) { int n = super.read(b, off, len); if (n > 0 && readlimit >= 0) bytesSinceMark += n; return n }
        @Override int read() { int ch = super.read(); if (ch >= 0 && readlimit >= 0) bytesSinceMark++; return ch }
        @Override void reset() throws IOException { if (readlimit >= 0 && bytesSinceMark > readlimit) throw new IOException("Mark invalid due to readlimit"); super.reset(); this.bytesSinceMark = 0 }
    }

    def "test InputStreamVipsSource with ByteSource supports seeking to 0"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return
        }
        VipsLibrary vips = NativeLibraryDetector.getVipsLibrary()
        byte[] data = new byte[100]
        new Random().nextBytes(data)
        ByteSource byteSource = ByteSource.wrap(data)

        when:
        def source = new InputStreamVipsSource(vips, byteSource)
        
        // Read some bytes
        Pointer buf = new com.sun.jna.Memory(10)
        long read1 = source.handleRead(buf, 10)
        
        // Seek back to 0
        long seekResult = source.handleSeek(0, 0)
        
        // Read again
        long read2 = source.handleRead(buf, 10)
        byte[] readData = buf.getByteArray(0, 10)

        then:
        read1 == 10
        seekResult == 0
        read2 == 10
        readData == data[0..9] as byte[]
        
        cleanup:
        source?.close()
    }

    def "test handleRead caps the requested length by BUFFER_SIZE"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return
        }
        VipsLibrary vips = NativeLibraryDetector.getVipsLibrary()
        byte[] data = new byte[1024 * 1024] // 1MB
        new Random().nextBytes(data)
        ByteSource byteSource = ByteSource.wrap(data)
        def source = new InputStreamVipsSource(vips, byteSource)
        
        // Use a large request (1MB)
        long requested = 1024 * 1024
        Pointer buf = new com.sun.jna.Memory(requested)

        when:
        long readCount = source.handleRead(buf, requested)

        then:
        // BUFFER_SIZE is 64KB
        readCount <= 64 * 1024
        readCount > 0
        
        cleanup:
        source?.close()
    }

    def "test handleSeek re-marks the stream when reopening from ByteSource"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return
        }
        VipsLibrary vips = NativeLibraryDetector.getVipsLibrary()
        byte[] data = "0123456789".getBytes()
        def callCount = 0
        ByteSource byteSource = new ByteSource() {
            @Override
            InputStream openStream() throws IOException {
                callCount++
                return new ByteArrayInputStream(data) {
                    boolean marked = false
                    @Override
                    void mark(int readlimit) {
                        marked = true
                        super.mark(readlimit)
                    }
                    @Override
                    void reset() throws IOException {
                        if (!marked) throw new IOException("Not marked!")
                        super.reset()
                    }
                }
            }
        }

        // We can't use the regular constructor because it calls mark() immediately, 
        // and we want to control when it reopens.
        // Actually, let's just use it and check initial state.
        def source = new InputStreamVipsSource(vips, byteSource)

        // Constructor would have opened stream and marked it (callCount = 1)
        assert callCount == 1
        assert ((ByteArrayInputStream)source.inputStream).marked == true

        when: "Seeking backward triggers ByteSource reopening (if we force it)"
        source.position = 5
        // Use a dummy stream that doesn't support marks to force reopen when seeking back
        def originalStream = source.inputStream
        source.inputStream = new InputStream() {
            @Override int read() { return 0 }
            @Override boolean markSupported() { return false }
            @Override void close() { originalStream.close() }
        }

        // Now seek to 0. It will skip reset because markSupported is false.
        // It will fall back to ByteSource reopen.
        source.handleSeek(0, 0)

        then: "Stream is reopened"
        callCount == 2
        source.position == 0

        when: "Seeking again (should use reset on the new stream)"
        source.position = 5
        source.handleSeek(0, 0)

        then: "Reset succeeds if mark() was called"
        // If mark() was NOT called, reset() will throw "Not marked!"
        // handleSeek will catch it and reopen ByteSource a THIRD time.
        callCount == 2
        source.position == 0

        cleanup:
        source?.close()
    }

    def "test InputStreamVipsSource with InputStream respects READ_LIMIT"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            return
        }
        VipsLibrary vips = NativeLibraryDetector.getVipsLibrary()
        byte[] data = new byte[100]
        ByteSource byteSource = ByteSource.wrap(data)

        // Using custom InputStream that enforces readlimit strictly

        // Temporarily set a small read limit
        int oldLimit = InputStreamVipsSource.READ_LIMIT
        InputStreamVipsSource.READ_LIMIT = 5

        when:
        InputStream limitingStream = new LimitingMarkInputStream(byteSource.openStream())
        def source2 = new InputStreamVipsSource(vips, limitingStream)
        Pointer buf = new com.sun.jna.Memory(10)
        source2.handleRead(buf, 10)
        long seekResult = source2.handleSeek(0, 0)

        then:
        seekResult == -1
        
        cleanup:
        InputStreamVipsSource.READ_LIMIT = oldLimit
        source2?.close()
    }
}
