package au.org.ala.images.ffm

import spock.lang.Specification
import java.lang.foreign.*
import java.lang.invoke.*

class VipsLibraryFFMSpec extends Specification {

    private Arena arena
    private Linker linker

    def setup() {
        arena = Arena.ofShared()
        linker = Linker.nativeLinker()
    }

    def cleanup() {
        if (arena != null) {
            arena.close()
        }
    }

    def "vipsInit passes correct arguments"() {
        given:
        def recordedArgs = []
        def impl = new Object() {
            int apply(MemorySegment name) {
                recordedArgs << name.reinterpret(1024).getUtf8String(0)
                return 0
            }
        }
        MethodHandle mh = MethodHandles.lookup().findVirtual(impl.class, "apply", MethodType.methodType(int.class, MemorySegment.class)).bindTo(impl)
        MemorySegment stub = linker.upcallStub(mh, VipsLibraryFFM.FD_vips_init, arena)

        def mockLookup = createMockLookup(["vips_init": stub])
        def vips = new VipsLibraryFFM(mockLookup, mockLookup, mockLookup)

        when:
        vips.vipsInit("test-app")

        then:
        recordedArgs == ["test-app"]
    }

    def "vipsImageNewFromBuffer passes correct arguments"() {
        given:
        def recordedArgs = []
        def impl = new Object() {
            MemorySegment apply(MemorySegment buf, long len, MemorySegment options) {
                recordedArgs << [buf: buf, len: len, options: options.reinterpret(1024).getUtf8String(0)]
                return MemorySegment.ofAddress(1234)
            }
        }
        MethodType mt = MethodType.methodType(MemorySegment.class, [MemorySegment.class, long.class, MemorySegment.class] as Class[])
        MethodHandle mh = MethodHandles.lookup().findVirtual(impl.class, "apply", mt).bindTo(impl)
        MemorySegment stub = linker.upcallStub(mh, VipsLibraryFFM.FD_vips_image_new_from_buffer, arena)

        def mockLookup = createMockLookup(["vips_image_new_from_buffer": stub])
        def vips = new VipsLibraryFFM(mockLookup, mockLookup, mockLookup)
        def myBuf = arena.allocate(10)

        when:
        vips.vipsImageNewFromBuffer(myBuf, 10, "some-options")

        then:
        recordedArgs.size() == 1
        recordedArgs[0].buf == myBuf
        recordedArgs[0].len == 10L
        recordedArgs[0].options == "some-options"
    }

    def "vipsThumbnailImage passes correct arguments"() {
        given:
        def recordedArgs = []
        def impl = new Object() {
            int apply(MemorySegment input, MemorySegment outputPtr, int width, MemorySegment options) {
                recordedArgs << [input: input, outputPtr: outputPtr, width: width, options: options]
                return 0
            }
        }
        MethodType mt = MethodType.methodType(int.class, [MemorySegment.class, MemorySegment.class, int.class, MemorySegment.class] as Class[])
        MethodHandle mh = MethodHandles.lookup().findVirtual(impl.class, "apply", mt).bindTo(impl)
        MemorySegment stub = linker.upcallStub(mh, VipsLibraryFFM.FD_vips_thumbnail_image, arena)

        def mockLookup = createMockLookup(["vips_thumbnail_image": stub])
        def vips = new VipsLibraryFFM(mockLookup, mockLookup, mockLookup)
        def image = MemorySegment.ofAddress(111)
        def outPtr = MemorySegment.ofAddress(222)

        when:
        vips.vipsThumbnailImage(image, outPtr, 100)

        then:
        recordedArgs.size() == 1
        recordedArgs[0].input == image
        recordedArgs[0].outputPtr == outPtr
        recordedArgs[0].width == 100
        recordedArgs[0].options.address() == 0
    }

    def "vipsImageWriteToBuffer passes correct arguments"() {
        given:
        def recordedArgs = []
        def impl = new Object() {
            int apply(MemorySegment image, MemorySegment bufPtr, MemorySegment lenPtr, MemorySegment suffix, MemorySegment options) {
                recordedArgs << [image: image, bufPtr: bufPtr, lenPtr: lenPtr, suffix: suffix.reinterpret(1024).getUtf8String(0)]
                return 0
            }
        }
        MethodType mt = MethodType.methodType(int.class, [MemorySegment.class, MemorySegment.class, MemorySegment.class, MemorySegment.class, MemorySegment.class] as Class[])
        MethodHandle mh = MethodHandles.lookup().findVirtual(impl.class, "apply", mt).bindTo(impl)
        MemorySegment stub = linker.upcallStub(mh, VipsLibraryFFM.FD_vips_image_write_to_buffer, arena)

        def mockLookup = createMockLookup(["vips_image_write_to_buffer": stub])
        def vips = new VipsLibraryFFM(mockLookup, mockLookup, mockLookup)
        def image = MemorySegment.ofAddress(111)
        def bufPtr = MemorySegment.ofAddress(222)
        def lenPtr = MemorySegment.ofAddress(333)

        when:
        vips.vipsImageWriteToBuffer(image, bufPtr, lenPtr, ".jpg")

        then:
        recordedArgs.size() == 1
        recordedArgs[0].image == image
        recordedArgs[0].bufPtr == bufPtr
        recordedArgs[0].lenPtr == lenPtr
        recordedArgs[0].suffix == ".jpg"
    }

    def "vipsDzsave passes correct arguments"() {
        given:
        def recordedArgs = []
        def impl = new Object() {
            int apply(MemorySegment input, MemorySegment path, MemorySegment options) {
                recordedArgs << [input: input, path: path.reinterpret(1024).getUtf8String(0)]
                return 0
            }
        }
        MethodType mt = MethodType.methodType(int.class, [MemorySegment.class, MemorySegment.class, MemorySegment.class] as Class[])
        MethodHandle mh = MethodHandles.lookup().findVirtual(impl.class, "apply", mt).bindTo(impl)
        MemorySegment stub = linker.upcallStub(mh, VipsLibraryFFM.FD_vips_dzsave, arena)

        def mockLookup = createMockLookup(["vips_dzsave": stub])
        def vips = new VipsLibraryFFM(mockLookup, mockLookup, mockLookup)
        def image = MemorySegment.ofAddress(111)

        when:
        vips.vipsDzsave(image, "/tmp/out")

        then:
        recordedArgs.size() == 1
        recordedArgs[0].input == image
        recordedArgs[0].path == "/tmp/out"
    }

    def "gSignalConnectData passes correct arguments"() {
        given:
        def recordedArgs = []
        def impl = new Object() {
            long apply(MemorySegment instance, MemorySegment signal, MemorySegment handler, MemorySegment data, MemorySegment closure, int flags) {
                recordedArgs << [instance: instance, signal: signal.reinterpret(1024).getUtf8String(0), handler: handler, data: data]
                return 1L
            }
        }
        MethodType mt = MethodType.methodType(long.class, [MemorySegment.class, MemorySegment.class, MemorySegment.class, MemorySegment.class, MemorySegment.class, int.class] as Class[])
        MethodHandle mh = MethodHandles.lookup().findVirtual(impl.class, "apply", mt).bindTo(impl)
        MemorySegment stub = linker.upcallStub(mh, VipsLibraryFFM.FD_g_signal_connect_data, arena)

        def mockLookup = createMockLookup(["g_signal_connect_data": stub])
        def vips = new VipsLibraryFFM(mockLookup, mockLookup, mockLookup)
        def instance = MemorySegment.ofAddress(111)
        def handler = MemorySegment.ofAddress(222)
        def data = MemorySegment.ofAddress(333)

        when:
        vips.gSignalConnectData(instance, "read", handler, data)

        then:
        recordedArgs.size() == 1
        recordedArgs[0].instance == instance
        recordedArgs[0].signal == "read"
        recordedArgs[0].handler == handler
        recordedArgs[0].data == data
    }

    private SymbolLookup createMockLookup(Map<String, MemorySegment> customMappings) {
        return new SymbolLookup() {
            @Override
            Optional<MemorySegment> find(String name) {
                if (customMappings.containsKey(name)) {
                    return Optional.of(customMappings.get(name))
                }
                // Return a dummy for all others to satisfy the constructor
                // In Java 21, MemorySegment.ofAddress(long) is available
                return Optional.of(MemorySegment.ofAddress(9999))
            }
        }
    }
}
