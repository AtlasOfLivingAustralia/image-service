package au.org.ala.images.ffm

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.foreign.*
import java.lang.invoke.MethodHandle

/**
 * FFM-based wrapper that creates a VipsTargetCustom from a Java OutputStream.
 */
@Slf4j
@CompileStatic
class OutputStreamVipsTargetFFM implements AutoCloseable {

    private final VipsLibraryFFM vips
    private final OutputStream outputStream
    private final MemorySegment target
    private final MemorySegment writeCallbackStub
    private final Arena callbackArena

    private boolean closed = false

    private static final int BUFFER_SIZE = 64 * 1024

    private static final FunctionDescriptor FD_WRITE_CALLBACK = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS
    )

    OutputStreamVipsTargetFFM(VipsLibraryFFM vips, OutputStream outputStream) {
        this.vips = vips
        this.outputStream = outputStream
        this.callbackArena = Arena.ofShared()

        try {
            this.target = vips.vipsTargetCustomNew()
            if (target == null || target.address() == 0) {
                throw new IOException("Failed to create VipsTargetCustom")
            }

            Linker linker = Linker.nativeLinker()
            MethodHandle writeHandle = createWriteMethodHandle()
            this.writeCallbackStub = linker.upcallStub(
                    writeHandle,
                    FD_WRITE_CALLBACK,
                    callbackArena
            )

            long writeHandlerId = vips.gSignalConnectData(target, "write", writeCallbackStub, MemorySegment.NULL)
            if (writeHandlerId == 0) {
                log.warn("Failed to connect write callback to VipsTargetCustom")
            }

        } catch (Throwable e) {
            callbackArena.close()
            throw new IOException("Failed to create VipsTarget", e)
        }
    }

    MemorySegment getTarget() {
        if (closed) throw new IllegalStateException("Target closed")
        return target
    }

    private MethodHandle createWriteMethodHandle() {
        return java.lang.invoke.MethodHandles.lookup().findVirtual(this.class, "handleWrite",
                java.lang.invoke.MethodType.methodType(long.class, [MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class] as Class[]))
                .bindTo(this)
    }

    @SuppressWarnings("unused")
    private long handleWrite(MemorySegment target, MemorySegment buffer, long length, MemorySegment userData) {
        if (closed) return -1

        try {
            long remaining = length
            long offset = 0
            while (remaining > 0) {
                int toWrite = (int) Math.min(remaining, (long) BUFFER_SIZE)
                byte[] javaBuffer = new byte[toWrite]
                MemorySegment.copy(buffer, ValueLayout.JAVA_BYTE, offset, javaBuffer, 0, toWrite)
                outputStream.write(javaBuffer)
                remaining -= toWrite
                offset += toWrite
            }
            return length
        } catch (IOException e) {
            log.error("Error writing to OutputStream", e)
            return -1
        }
    }

    @Override
    void close() {
        if (!closed) {
            closed = true
            try {
                vips.gObjectUnref(target)
            } catch (Throwable e) {
                log.warn("Error unreffing VipsTarget", e)
            }
            callbackArena.close()
            try {
                outputStream.flush()
            } catch (IOException ignored) {}
        }
    }
}
