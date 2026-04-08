package au.org.ala.images.jna

import com.sun.jna.Pointer
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Wrapper that creates a VipsTargetCustom from a Java OutputStream.
 * This allows streaming data directly from libvips to Java without buffering the entire image.
 */
@Slf4j
@CompileStatic
class OutputStreamVipsTarget implements AutoCloseable {

    private final VipsLibrary vips
    private final OutputStream outputStream
    private final Pointer target
    private final VipsLibrary.WriteCallback writeCallback
    private final byte[] javaBuffer
    private long bytesWritten = 0
    private boolean closed = false

    // Buffer size for copying from native memory to Java byte array
    private static final int BUFFER_SIZE = 64 * 1024  // 64KB

    /**
     * Create a VipsTarget from an OutputStream.
     * @param vips VipsLibrary instance
     * @param outputStream the output stream to wrap
     */
    OutputStreamVipsTarget(VipsLibrary vips, OutputStream outputStream) {
        this.vips = vips
        this.outputStream = outputStream
        this.javaBuffer = new byte[BUFFER_SIZE]

        // Create the custom target
        this.target = vips.vips_target_custom_new()
        if (target == null || target == Pointer.NULL) {
            throw new IOException("Failed to create VipsTargetCustom")
        }

        // Create callback that delegates to this instance
        this.writeCallback = new VipsLibrary.WriteCallback() {
            @Override
            long invoke(Pointer target, Pointer buffer, long length, Pointer user_data) {
                return handleWrite(buffer, length)
            }
        }

        // Connect the "write" signal
        long writeHandlerId = vips.g_signal_connect_data(target, "write", writeCallback,
                                                          Pointer.NULL, Pointer.NULL, 0)

        if (writeHandlerId == 0) {
            log.warn("Failed to connect write callback to VipsTargetCustom")
        }

        log.debug("Created OutputStreamVipsTarget with write handler: {}", writeHandlerId)
    }

    /**
     * Get the number of bytes written to the OutputStream.
     * @return bytes written
     */
    long getBytesWritten() {
        return bytesWritten
    }

    /**
     * Get the VipsTarget pointer.
     * @return pointer to VipsTargetCustom
     */
    Pointer getTarget() {
        if (closed) {
            throw new IllegalStateException("Target has been closed")
        }
        return target
    }

    /**
     * Handle write callback from libvips.
     * Write 'length' bytes from the buffer to the OutputStream.
     *
     * @param buffer native buffer containing data
     * @param length number of bytes to write
     * @return number of bytes written, -1 for error
     */
    private long handleWrite(Pointer buffer, long length) {
        if (closed) {
            log.warn("Write called on closed target")
            return -1
        }

        try {
            long remaining = length
            long offset = 0
            while (remaining > 0) {
                int toWrite = (int) Math.min(remaining, (long) BUFFER_SIZE)
                
                // Copy from native buffer to Java byte array
                buffer.read(offset, javaBuffer, 0, toWrite)
                
                outputStream.write(javaBuffer, 0, toWrite)
                
                bytesWritten += toWrite
                remaining -= toWrite
                offset += toWrite
            }
            
            log.trace("Wrote {} bytes to OutputStream", length)
            return length

        } catch (IOException e) {
            log.error("Error writing to OutputStream", e)
            return -1
        } catch (Exception e) {
            log.error("Unexpected error in write callback", e)
            return -1
        }
    }

    @Override
    void close() {
        if (!closed) {
            closed = true

            // Unref the target object
            if (target != null && target != Pointer.NULL) {
                try {
                    vips.g_object_unref(target)
                    log.trace("Closed OutputStreamVipsTarget")
                } catch (Exception e) {
                    log.warn("Error unreffing VipsTarget", e)
                }
            }

            // Flush the underlying stream
            try {
                outputStream.flush()
                // We DON'T close the output stream here, as it's owned by the ByteSink
            } catch (IOException e) {
                log.debug("Error flushing OutputStream", e)
            }
        }
    }
}
