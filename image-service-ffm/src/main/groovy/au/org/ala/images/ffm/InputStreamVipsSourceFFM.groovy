package au.org.ala.images.ffm

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.foreign.*
import java.lang.invoke.MethodHandle

/**
 * FFM-based wrapper that creates a VipsSourceCustom from a Java InputStream.
 * This uses the Foreign Function & Memory API (Java 22+) to stream data directly
 * from Java to libvips without buffering the entire image.
 *
 * The VipsSourceCustom is a libvips object that implements streaming I/O through callbacks.
 * We provide read and seek callbacks that delegate to the Java InputStream.
 */
@Slf4j
@CompileStatic
class InputStreamVipsSourceFFM implements AutoCloseable {

    private final VipsLibraryFFM vips
    private final InputStream inputStream
    private final MemorySegment source
    private final MemorySegment readCallbackStub
    private final MemorySegment seekCallbackStub
    private final Arena callbackArena

    private long position = 0
    private boolean closed = false

    // Buffer size for reading from InputStream
    private static final int BUFFER_SIZE = 64 * 1024  // 64KB

    // Function descriptors for callbacks
    private static final FunctionDescriptor FD_READ_CALLBACK = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,  // return: bytes read
            ValueLayout.ADDRESS,     // source
            ValueLayout.ADDRESS,     // buffer
            ValueLayout.JAVA_LONG,   // length
            ValueLayout.ADDRESS      // user_data
    )

    private static final FunctionDescriptor FD_SEEK_CALLBACK = FunctionDescriptor.of(
            ValueLayout.JAVA_LONG,  // return: new position
            ValueLayout.ADDRESS,     // source
            ValueLayout.JAVA_LONG,   // offset
            ValueLayout.JAVA_INT,    // whence
            ValueLayout.ADDRESS      // user_data
    )

    /**
     * Create a VipsSource from an InputStream.
     * @param vips VipsLibraryFFM instance
     * @param inputStream the input stream to wrap
     */
    InputStreamVipsSourceFFM(VipsLibraryFFM vips, InputStream inputStream) {
        this.vips = vips
        this.inputStream = inputStream
        this.callbackArena = Arena.ofShared()

        // Mark the stream if possible for seeking support
        if (inputStream.markSupported()) {
            inputStream.mark(Integer.MAX_VALUE)
        }

        try {
            // Create the custom source
            this.source = vips.vipsSourceCustomNew()
            if (source == null || source.address() == 0) {
                throw new IOException("Failed to create VipsSourceCustom")
            }

            // Create upcall stubs for callbacks
            Linker linker = Linker.nativeLinker()

            // Read callback
            MethodHandle readHandle = createReadMethodHandle()
            this.readCallbackStub = linker.upcallStub(
                    readHandle,
                    FD_READ_CALLBACK,
                    callbackArena
            )

            // Seek callback
            MethodHandle seekHandle = createSeekMethodHandle()
            this.seekCallbackStub = linker.upcallStub(
                    seekHandle,
                    FD_SEEK_CALLBACK,
                    callbackArena
            )

            // Connect the callbacks to the source
            long readHandlerId = vips.gSignalConnectData(source, "read", readCallbackStub, MemorySegment.NULL)
            long seekHandlerId = vips.gSignalConnectData(source, "seek", seekCallbackStub, MemorySegment.NULL)

            if (readHandlerId == 0) {
                log.warn("Failed to connect read callback to VipsSourceCustom")
            }
            if (seekHandlerId == 0) {
                log.debug("Failed to connect seek callback to VipsSourceCustom (may not be supported)")
            }

            log.debug("Created InputStreamVipsSourceFFM with read handler: {}, seek handler: {}",
                    readHandlerId, seekHandlerId)

        } catch (Throwable e) {
            callbackArena.close()
            throw new IOException("Failed to create VipsSource", e)
        }
    }

    private MethodHandle createReadMethodHandle() throws NoSuchMethodException, IllegalAccessException {
        return java.lang.invoke.MethodHandles.lookup()
                .bind(this, "handleRead",
                        java.lang.invoke.MethodType.methodType(long.class,
                                MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class))
    }

    private MethodHandle createSeekMethodHandle() throws NoSuchMethodException, IllegalAccessException {
        return java.lang.invoke.MethodHandles.lookup()
                .bind(this, "handleSeek",
                        java.lang.invoke.MethodType.methodType(long.class,
                                MemorySegment.class, long.class, int.class, MemorySegment.class))
    }

    /**
     * Get the VipsSource memory segment.
     * @return memory segment to VipsSourceCustom
     */
    MemorySegment getSource() {
        if (closed) {
            throw new IllegalStateException("Source has been closed")
        }
        return source
    }

    /**
     * Handle read callback from libvips.
     * Read up to 'length' bytes from the InputStream into the buffer.
     *
     * @param source native source pointer
     * @param buffer native buffer to write to
     * @param length number of bytes requested
     * @param userData user data (unused)
     * @return number of bytes read, 0 for EOF, -1 for error
     */
    @SuppressWarnings('unused')
    private long handleRead(MemorySegment source, MemorySegment buffer, long length, MemorySegment userData) {
        if (closed) {
            log.warn("Read called on closed source")
            return -1
        }

        try {
            // Read from InputStream in chunks
            int toRead = (int) Math.min(length, BUFFER_SIZE)
            byte[] javaBuffer = new byte[toRead]

            int bytesRead = inputStream.read(javaBuffer, 0, toRead)

            if (bytesRead < 0) {
                // EOF
                log.trace("Read EOF at position {}", position)
                return 0
            }

            if (bytesRead > 0) {
                // Copy from Java byte array to native buffer
                MemorySegment.copy(javaBuffer, 0, buffer, ValueLayout.JAVA_BYTE, 0, bytesRead)
                position += bytesRead
                log.trace("Read {} bytes at position {}", bytesRead, position - bytesRead)
            }

            return bytesRead

        } catch (IOException e) {
            log.error("Error reading from InputStream", e)
            return -1
        } catch (Exception e) {
            log.error("Unexpected error in read callback", e)
            return -1
        }
    }

    /**
     * Handle seek callback from libvips.
     * Attempt to seek in the InputStream if supported.
     *
     * @param source native source pointer
     * @param offset offset to seek to
     * @param whence SEEK_SET (0), SEEK_CUR (1), or SEEK_END (2)
     * @param userData user data (unused)
     * @return new position, -1 if seeking not supported or error
     */
    @SuppressWarnings('unused')
    private long handleSeek(MemorySegment source, long offset, int whence, MemorySegment userData) {
        if (closed) {
            log.warn("Seek called on closed source")
            return -1
        }

        try {
            // Most InputStreams don't support seeking
            // We can only support SEEK_SET to 0 (reset) if mark/reset is supported
            if (whence == 0 && offset == 0 && inputStream.markSupported()) {
                // SEEK_SET to beginning
                inputStream.reset()
                position = 0
                log.trace("Seeked to beginning")
                return 0
            }

            // Seeking not supported for this stream
            log.trace("Seek not supported: whence={}, offset={}", whence, offset)
            return -1

        } catch (IOException e) {
            log.debug("Error seeking in InputStream", e)
            return -1
        } catch (Exception e) {
            log.error("Unexpected error in seek callback", e)
            return -1
        }
    }

    @Override
    void close() {
        if (!closed) {
            closed = true

            // Unref the source object
            if (source != null && source.address() != 0) {
                try {
                    vips.gObjectUnref(source)
                    log.trace("Closed InputStreamVipsSourceFFM")
                } catch (Throwable e) {
                    log.warn("Error unreffing VipsSource", e)
                }
            }

            // Close callback arena
            try {
                callbackArena.close()
            } catch (Exception e) {
                log.warn("Error closing callback arena", e)
            }

            // Close the underlying stream
            try {
                inputStream.close()
            } catch (IOException e) {
                log.debug("Error closing InputStream", e)
            }
        }
    }

    /**
     * Check if this source supports seeking.
     * @return true if the underlying stream supports mark/reset
     */
    boolean isSeekable() {
        return inputStream.markSupported()
    }

    /**
     * Get current read position.
     * @return bytes read from stream
     */
    long getPosition() {
        return position
    }
}
