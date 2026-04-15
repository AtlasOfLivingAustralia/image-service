package au.org.ala.images.ffm;

import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * FFM-based wrapper that creates a VipsSourceCustom from a Java InputStream.
 * This uses the Foreign Function & Memory API to stream data directly
 * from Java to libvips without buffering the entire image.
 */
public class InputStreamVipsSourceFFM implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InputStreamVipsSourceFFM.class);

    /** Configurable read limit for mark/reset. Default 10MB. */
    public static final int READ_LIMIT = Integer.getInteger("vips.source.readlimit", 10 * 1024 * 1024);

    private final VipsLibraryFFM vips;
    private final ByteSource byteSource;
    private InputStream inputStream;
    private final MemorySegment source;
    private final MemorySegment readCallbackStub;
    private final MemorySegment seekCallbackStub;
    private final Arena callbackArena;
    private byte[] javaBuffer;

    private long position = 0;
    private boolean closed = false;

    // Buffer size for reading from InputStream
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB

    // Function descriptors for callbacks
    private static final FunctionDescriptor FD_READ_CALLBACK = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG, // return: bytes read
            ValueLayout.ADDRESS,    // source
            ValueLayout.ADDRESS,    // buffer
            ValueLayout.JAVA_LONG,  // length
            ValueLayout.ADDRESS     // user_data
    );

    private static final FunctionDescriptor FD_SEEK_CALLBACK = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG, // return: new position
            ValueLayout.ADDRESS,    // source
            ValueLayout.JAVA_LONG,  // offset
            ValueLayout.JAVA_INT,   // whence
            ValueLayout.ADDRESS     // user_data
    );

    public InputStreamVipsSourceFFM(VipsLibraryFFM vips, InputStream inputStream) throws IOException {
        this(vips, inputStream, null);
    }

    public InputStreamVipsSourceFFM(VipsLibraryFFM vips, ByteSource byteSource) throws IOException {
        this(vips, byteSource.openStream(), byteSource);
    }

    private InputStreamVipsSourceFFM(VipsLibraryFFM vips, InputStream inputStream, ByteSource byteSource) throws IOException {
        this.vips = vips;
        this.inputStream = inputStream;
        this.byteSource = byteSource;
        this.callbackArena = Arena.ofShared();
        this.javaBuffer = new byte[BUFFER_SIZE];

        markStreamIfSupported();

        try {
            this.source = vips.vipsSourceCustomNew();
            if (source == null || source.address() == 0) {
                throw new IOException("Failed to create VipsSourceCustom");
            }

            Linker linker = Linker.nativeLinker();

            // Read callback
            MethodHandle readHandle = MethodHandles.lookup().findVirtual(InputStreamVipsSourceFFM.class, "handleRead",
                    MethodType.methodType(long.class, MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class)).bindTo(this);
            this.readCallbackStub = linker.upcallStub(readHandle, FD_READ_CALLBACK, callbackArena);

            // Seek callback
            MethodHandle seekHandle = MethodHandles.lookup().findVirtual(InputStreamVipsSourceFFM.class, "handleSeek",
                    MethodType.methodType(long.class, MemorySegment.class, long.class, int.class, MemorySegment.class)).bindTo(this);
            this.seekCallbackStub = linker.upcallStub(seekHandle, FD_SEEK_CALLBACK, callbackArena);

            // Connect the callbacks to the source
            long readHandlerId = vips.gSignalConnectData(source, "read", readCallbackStub, MemorySegment.NULL);
            long seekHandlerId = vips.gSignalConnectData(source, "seek", seekCallbackStub, MemorySegment.NULL);

            if (readHandlerId == 0) {
                throw new IOException("Failed to connect read callback to VipsSourceCustom");
            }
            if (seekHandlerId == 0) {
                log.debug("Failed to connect seek callback to VipsSourceCustom (may not be supported)");
            }

            log.debug("Created InputStreamVipsSourceFFM with read handler: {}, seek handler: {}", readHandlerId, seekHandlerId);

        } catch (Throwable e) {
            close();
            throw new IOException("Failed to create VipsSource", e);
        }
    }

    public MemorySegment getSource() {
        if (closed) {
            throw new IllegalStateException("Source has been closed");
        }
        return source;
    }

    @SuppressWarnings("unused")
    private long handleRead(MemorySegment source, MemorySegment buffer, long length, MemorySegment userData) {
        if (closed) {
            log.warn("Read called on closed source");
            return -1;
        }

        try {
            int toRead = (int) Math.min(length, (long) javaBuffer.length);

            int bytesRead = inputStream.read(javaBuffer, 0, toRead);

            if (bytesRead < 0) {
                return 0; // EOF
            }

            if (bytesRead > 0) {
                MemorySegment boundedBuffer = buffer.reinterpret(length);
                MemorySegment.copy(javaBuffer, 0, boundedBuffer, ValueLayout.JAVA_BYTE, 0, bytesRead);
//                MemorySegment.copy(javaBuffer, 0, buffer, ValueLayout.JAVA_BYTE, 0, bytesRead);
                position += bytesRead;
            }

            return bytesRead;

        } catch (IOException e) {
            log.error("Error reading from InputStream", e);
            return -1;
        }
    }

    @SuppressWarnings("unused")
    private long handleSeek(MemorySegment source, long offset, int whence, MemorySegment userData) {
        if (closed) {
            log.warn("Seek called on closed source");
            return -1;
        }

        try {
            if (whence == 0) { // SEEK_SET
                // Prefer using reset() if the stream supports marks, as it avoids reopening the stream
                if (inputStream.markSupported()) {
                    try {
                        if (offset == 0) {
                            inputStream.reset();
                            position = 0;
                            log.trace("Seeked to beginning using reset");
                            return 0;
                        } else if (offset < position) {
                            // If we've marked the stream (at 0), we can reset and skip to the desired offset
                            inputStream.reset();
                            ByteStreams.skipFully(inputStream, offset);
                            position = offset;
                            log.trace("Seeked to {} using reset and skip", offset);
                            return position;
                        }
                    } catch (IOException e) {
                        log.debug("Reset failed (likely exceeded read limit or not marked): {}", e.getMessage());
                        // Fall back to ByteSource if available
                    }
                }

                // If seeking forward, we can just skip
                if (offset >= position) {
                    long toSkip = offset - position;
                    if (toSkip > 0) {
                        ByteStreams.skipFully(inputStream, toSkip);
                        position = offset;
                        log.trace("Seeked forward to {} using skip", offset);
                    }
                    return position;
                }

                // Fall back to reopening the ByteSource if available
                if (byteSource != null) {
                    inputStream.close();
                    inputStream = byteSource.openStream();
                    markStreamIfSupported();
                    if (offset > 0) {
                        ByteStreams.skipFully(inputStream, offset);
                    }
                    position = offset;
                    log.trace("Seeked to {} by reopening ByteSource", offset);
                    return position;
                }
            }
            return -1;
        } catch (IOException e) {
            log.debug("Error seeking in InputStream: {}", e.getMessage());
            return -1;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (source != null && source.address() != 0) {
                    vips.gObjectUnref(source);
                }
            } catch (Throwable e) {
                log.warn("Error unreffing VipsSource", e);
            }
            callbackArena.close();

            if (byteSource != null && inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.debug("Error closing InputStream: {}", e.getMessage());
                }
            }
        }
    }

    private void markStreamIfSupported() {
        if (inputStream.markSupported()) {
            log.trace("Marking input stream with limit: {}", READ_LIMIT);
            inputStream.mark(READ_LIMIT);
        }
    }

    public boolean isSeekable() {
        return byteSource != null || inputStream.markSupported();
    }

    public long getPosition() {
        return position;
    }
}
