package au.org.ala.images.ffm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * FFM-based wrapper that creates a VipsTargetCustom from a Java OutputStream.
 */
public class OutputStreamVipsTargetFFM implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OutputStreamVipsTargetFFM.class);

    private final VipsLibraryFFM vips;
    private final OutputStream outputStream;
    private final MemorySegment target;
    private final MemorySegment writeCallbackStub;
    private final Arena callbackArena;
    private final byte[] javaBuffer;
    private long bytesWritten = 0;

    private boolean closed = false;

    private static final int BUFFER_SIZE = 64 * 1024;

    private static final FunctionDescriptor FD_WRITE_CALLBACK = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS
    );

    public OutputStreamVipsTargetFFM(VipsLibraryFFM vips, OutputStream outputStream) throws IOException {
        this.vips = vips;
        this.outputStream = outputStream;
        this.callbackArena = Arena.ofShared();
        this.javaBuffer = new byte[BUFFER_SIZE];

        try {
            this.target = vips.vipsTargetCustomNew();
            if (target == null || target.address() == 0) {
                throw new IOException("Failed to create VipsTargetCustom");
            }

            Linker linker = Linker.nativeLinker();
            MethodHandle writeHandle = MethodHandles.lookup().findVirtual(OutputStreamVipsTargetFFM.class, "handleWrite",
                    MethodType.methodType(long.class, MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class)).bindTo(this);
            this.writeCallbackStub = linker.upcallStub(writeHandle, FD_WRITE_CALLBACK, callbackArena);

            long writeHandlerId = vips.gSignalConnectData(target, "write", writeCallbackStub, MemorySegment.NULL);
            if (writeHandlerId == 0) {
                throw new IOException("Failed to connect write callback to VipsTargetCustom");
            }

        } catch (Throwable e) {
            close();
            throw new IOException("Failed to create VipsTarget", e);
        }
    }

    public MemorySegment getTarget() {
        if (closed) {
            throw new IllegalStateException("Target closed");
        }
        return target;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    @SuppressWarnings("unused")
    private long handleWrite(MemorySegment target, MemorySegment buffer, long length, MemorySegment userData) {
        if (closed) {
            return -1;
        }

        try {
            long remaining = length;
            long offset = 0;
            while (remaining > 0) {
                int toWrite = (int) Math.min(remaining, (long) BUFFER_SIZE);
                MemorySegment boundedBuffer = buffer.reinterpret(length);
                MemorySegment.copy(boundedBuffer, ValueLayout.JAVA_BYTE, offset, javaBuffer, 0, toWrite);
                outputStream.write(javaBuffer, 0, toWrite);
                bytesWritten += toWrite;
                remaining -= toWrite;
                offset += toWrite;
            }
            return length;
        } catch (IOException e) {
            log.error("Error writing to OutputStream", e);
            return -1;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (target != null && target.address() != 0) {
                    vips.gObjectUnref(target);
                }
            } catch (Throwable e) {
                log.warn("Error unreffing VipsTarget", e);
            }
            callbackArena.close();
            try {
                outputStream.flush();
            } catch (IOException ignored) {
            }
        }
    }
}
