package au.org.ala.images.jna

import com.sun.jna.Memory
import com.sun.jna.Pointer
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Wrapper that creates a VipsSourceCustom from a Java InputStream.
 * This allows streaming data directly from Java to libvips without buffering the entire image.
 *
 * The VipsSourceCustom is a libvips object that implements streaming I/O through callbacks.
 * We provide read and seek callbacks that delegate to the Java InputStream.
 */
@Slf4j
@CompileStatic
class InputStreamVipsSource implements AutoCloseable {

    private final VipsLibrary vips
    private final InputStream inputStream
    private final Pointer source
    private final VipsLibrary.ReadCallback readCallback
    private final VipsLibrary.SeekCallback seekCallback

    private long position = 0
    private boolean closed = false

    // Buffer size for reading from InputStream
    private static final int BUFFER_SIZE = 64 * 1024  // 64KB

    /**
     * Create a VipsSource from an InputStream.
     * @param vips VipsLibrary instance
     * @param inputStream the input stream to wrap
     */
    InputStreamVipsSource(VipsLibrary vips, InputStream inputStream) {
        this.vips = vips
        this.inputStream = inputStream

        // Mark the stream if possible for seeking support
        if (inputStream.markSupported()) {
            inputStream.mark(Integer.MAX_VALUE)
        }

        // Create the custom source
        this.source = vips.vips_source_custom_new()
        if (source == null || source == Pointer.NULL) {
            throw new IOException("Failed to create VipsSourceCustom")
        }

        // Create callbacks that delegate to this instance
        this.readCallback = new VipsLibrary.ReadCallback() {
            @Override
            long invoke(Pointer source, Pointer buffer, long length, Pointer user_data) {
                return handleRead(buffer, length)
            }
        }

        this.seekCallback = new VipsLibrary.SeekCallback() {
            @Override
            long invoke(Pointer source, long offset, int whence, Pointer user_data) {
                return handleSeek(offset, whence)
            }
        }

        // Connect the callbacks to the source
        // g_signal_connect_data(instance, signal_name, callback, data, destroy_notify, flags)
        long readHandlerId = vips.g_signal_connect_data(source, "read", readCallback,
                                                         Pointer.NULL, Pointer.NULL, 0)
        long seekHandlerId = vips.g_signal_connect_data(source, "seek", seekCallback,
                                                         Pointer.NULL, Pointer.NULL, 0)

        if (readHandlerId == 0) {
            log.warn("Failed to connect read callback to VipsSourceCustom")
        }
        if (seekHandlerId == 0) {
            log.debug("Failed to connect seek callback to VipsSourceCustom (may not be supported)")
        }

        log.debug("Created InputStreamVipsSource with read handler: {}, seek handler: {}",
                  readHandlerId, seekHandlerId)
    }

    /**
     * Get the VipsSource pointer.
     * @return pointer to VipsSourceCustom
     */
    Pointer getSource() {
        if (closed) {
            throw new IllegalStateException("Source has been closed")
        }
        return source
    }

    /**
     * Handle read callback from libvips.
     * Read up to 'length' bytes from the InputStream into the buffer.
     *
     * @param buffer native buffer to write to
     * @param length number of bytes requested
     * @return number of bytes read, 0 for EOF, -1 for error
     */
    private long handleRead(Pointer buffer, long length) {
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
                buffer.write(0, javaBuffer, 0, bytesRead)
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
     * @param offset offset to seek to
     * @param whence SEEK_SET (0), SEEK_CUR (1), or SEEK_END (2)
     * @return new position, -1 if seeking not supported or error
     */
    private long handleSeek(long offset, int whence) {
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
            if (source != null && source != Pointer.NULL) {
                try {
                    vips.g_object_unref(source)
                    log.trace("Closed InputStreamVipsSource")
                } catch (Exception e) {
                    log.warn("Error unreffing VipsSource", e)
                }
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
