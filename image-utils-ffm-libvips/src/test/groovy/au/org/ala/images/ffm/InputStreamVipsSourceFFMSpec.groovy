package au.org.ala.images.ffm

import com.google.common.io.ByteSource
import spock.lang.Specification
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.SymbolLookup

class InputStreamVipsSourceFFMSpec extends Specification {

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
        source.javaBuffer = new byte[64 * 1024] // Match BUFFER_SIZE
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
}
