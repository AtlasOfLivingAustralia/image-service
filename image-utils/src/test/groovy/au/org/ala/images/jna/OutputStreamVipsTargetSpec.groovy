package au.org.ala.images.jna

import com.sun.jna.Memory
import com.sun.jna.Pointer
import spock.lang.Specification
import groovy.util.logging.Slf4j

@Slf4j
class OutputStreamVipsTargetSpec extends Specification {

    def "test bytesWritten tracking"() {
        given:
        if (!NativeLibraryDetector.isVipsAvailable()) {
            log.warn "libvips not available, skipping OutputStreamVipsTargetSpec"
            return
        }
        
        def vips = NativeLibraryDetector.getVipsLibrary()
        def out = new ByteArrayOutputStream()
        def target = new OutputStreamVipsTarget(vips, out)
        
        // Directly call handleWrite to simulate libvips writing
        def data = "Hello World".getBytes()
        def buffer = new Memory(data.length)
        buffer.write(0, data, 0, data.length)
        
        when:
        target.handleWrite(buffer, data.length)
        
        then:
        target.getBytesWritten() == data.length
        out.size() == data.length
        out.toByteArray() == data
        
        when: "Writing more data"
        def moreData = " More Data".getBytes()
        def moreBuffer = new Memory(moreData.length)
        moreBuffer.write(0, moreData, 0, moreData.length)
        target.handleWrite(moreBuffer, moreData.length)
        
        then:
        target.getBytesWritten() == data.length + moreData.length
        out.size() == data.length + moreData.length
        
        cleanup:
        target?.close()
    }
}
