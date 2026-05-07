package au.org.ala.images.ffm

import com.google.common.io.ByteSource
import spock.lang.Specification
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.SymbolLookup

class InputStreamVipsSourceFFMSpec extends Specification {

    def setField(obj, fieldName, value) {
        def field = obj.class.getDeclaredField(fieldName)
        field.setAccessible(true)
        field.set(obj, value)
    }

    def "test handleRead caps the requested length by BUFFER_SIZE"() {
        given:
        byte[] data = new byte[1024 * 1024] // 1MB
        new Random().nextBytes(data)
        ByteSource byteSource = ByteSource.wrap(data)
        
        // Use reflection to create instance without calling constructor
        def source = sun.reflect.ReflectionFactory.getReflectionFactory()
                .newConstructorForSerialization(InputStreamVipsSourceFFM.class, Object.class.getConstructor())
                .newInstance()
        
        // Manually initialize required fields for handleRead
        source.inputStream = byteSource.openStream()
        setField(source, 'javaBuffer', new byte[64 * 1024]) // Match BUFFER_SIZE
        source.closed = false
        
        // Use a large request (1MB)
        long requested = 1024 * 1024
        Arena arena = Arena.ofShared()
        MemorySegment buf = arena.allocate(requested)

        when:
        // MemorySegment source, MemorySegment buffer, long length, MemorySegment userData
        long readCount = source.handleRead(MemorySegment.NULL, buf, requested, MemorySegment.NULL)

        then:
        // BUFFER_SIZE is 64KB
        readCount <= 64 * 1024
        readCount > 0
        
        cleanup:
        source?.inputStream?.close()
        arena?.close()
    }

    def "test handleSeek re-marks the stream when reopening from ByteSource"() {
        given:
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

        // Use reflection to create instance without calling constructor
        def source = sun.reflect.ReflectionFactory.getReflectionFactory()
                .newConstructorForSerialization(InputStreamVipsSourceFFM.class, Object.class.getConstructor())
                .newInstance()

        setField(source, 'byteSource', byteSource)
        source.inputStream = byteSource.openStream()
        source.position = 0L
        source.closed = false
        source.inputStream.mark(10 * 1024 * 1024) // READ_LIMIT

        // Constructor would have opened stream and marked it (callCount = 1)
        assert callCount == 1
        assert ((ByteArrayInputStream)source.inputStream).marked == true

        when: "Seeking backward triggers ByteSource reopening (if we force it)"
        source.position = 5
        // Use a dummy stream that doesn't support marks to force reopen
        def originalStream = source.inputStream
        source.inputStream = new InputStream() {
            @Override int read() { return 0 }
            @Override boolean markSupported() { return false }
            @Override void close() { originalStream.close() }
        }

        // Now seek to 0. It will skip reset because markSupported is false.
        // It will fall back to ByteSource reopen.
        source.handleSeek(MemorySegment.NULL, 0, 0, MemorySegment.NULL)

        then: "Stream is reopened"
        callCount == 2
        source.position == 0

        when: "Seeking again (should use reset on the new stream)"
        source.position = 5
        source.handleSeek(MemorySegment.NULL, 0, 0, MemorySegment.NULL)

        then: "Reset succeeds if mark() was called"
        // If mark() was NOT called, reset() will throw "Not marked!"
        // handleSeek will catch it and reopen ByteSource a THIRD time.
        callCount == 2
        source.position == 0
    }
}
