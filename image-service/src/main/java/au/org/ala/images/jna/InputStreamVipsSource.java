package au.org.ala.images.jna;

import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper that creates a VipsSourceCustom from a Java InputStream.
 * This allows streaming data directly from Java to libvips without buffering the entire image.
 *
 * The VipsSourceCustom is a libvips object that implements streaming I/O through callbacks.
 * We provide read and seek callbacks that delegate to the Java InputStream.
 *
 * This class relies on libvips not overlapping calls to the read callback, which is true for libvips 8.12.
 */
public class InputStreamVipsSource implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InputStreamVipsSource.class);

    /** Configurable read limit for mark/reset. Default 10MB. */
    public static int READ_LIMIT = Integer.getInteger("vips.source.readlimit", 10 * 1024 * 1024);

    private final VipsLibrary vips;
    private final ByteSource byteSource;
    private InputStream inputStream;
    private final Pointer source;
    private final VipsLibrary.ReadCallback readCallback;
    private final VipsLibrary.SeekCallback seekCallback;
    private final byte[] javaBuffer;

    private long position = 0;
    private boolean closed = false;

    // Buffer size for reading from InputStream
    private static final int BUFFER_SIZE = 64 * 1024;  // 64KB

    /**
     * Create a VipsSource from an InputStream.
     * @param vips VipsLibrary instance
     * @param inputStream the input stream to wrap
     */
    public InputStreamVipsSource(VipsLibrary vips, InputStream inputStream) throws IOException {
        this.vips = vips;
        this.inputStream = inputStream;
        this.byteSource = null;
        this.javaBuffer = new byte[BUFFER_SIZE];

        // Mark the stream if possible for seeking support, but with a bounded limit
        if (inputStream.markSupported()) {
            log.trace("Marking input stream with limit: {}", READ_LIMIT);
            inputStream.mark(READ_LIMIT);
        }

        // Create the custom source
        this.source = vips.vips_source_custom_new();
        if (source == null || source == Pointer.NULL) {
            throw new IOException("Failed to create VipsSourceCustom");
        }

        this.readCallback = createReadCallback();
        this.seekCallback = createSeekCallback();

        connectCallbacks();
    }

    /**
     * Create a VipsSource from a ByteSource.
     * This allows seeking by reopening the stream.
     * @param vips VipsLibrary instance
     * @param byteSource the byte source to wrap
     */
    public InputStreamVipsSource(VipsLibrary vips, ByteSource byteSource) throws IOException {
        this.vips = vips;
        this.byteSource = byteSource;
        this.inputStream = byteSource.openStream();
        this.javaBuffer = new byte[BUFFER_SIZE];

        // No need to mark if we have ByteSource, we can reopen instead

        // Create the custom source
        this.source = vips.vips_source_custom_new();
        if (source == null || source == Pointer.NULL) {
            throw new IOException("Failed to create VipsSourceCustom");
        }

        this.readCallback = createReadCallback();
        this.seekCallback = createSeekCallback();

        connectCallbacks();
    }

    private VipsLibrary.ReadCallback createReadCallback() {
        return (source1, buffer, length, user_data) -> handleRead(buffer, length);
    }

    private VipsLibrary.SeekCallback createSeekCallback() {
        return (source1, offset, whence, user_data) -> handleSeek(offset, whence);
    }

    private void connectCallbacks() throws IOException {
        // Connect the callbacks to the source
        // g_signal_connect_data(instance, signal_name, callback, data, destroy_notify, flags)
        long readHandlerId = vips.g_signal_connect_data(source, "read", readCallback,
                                                         Pointer.NULL, null, 0);
        long seekHandlerId = vips.g_signal_connect_data(source, "seek", seekCallback,
                                                         Pointer.NULL, null, 0);

        if (readHandlerId == 0) {
            close();
            throw new IOException("Failed to connect read callback to VipsSourceCustom");
        }
        if (seekHandlerId == 0) {
            log.debug("Failed to connect seek callback to VipsSourceCustom (may not be supported)");
        }

        log.debug("Created InputStreamVipsSource with read handler: {}, seek handler: {}",
                  readHandlerId, seekHandlerId);
    }

    /**
     * Get the VipsSource pointer.
     * @return pointer to VipsSourceCustom
     */
    public Pointer getSource() {
        if (closed) {
            throw new IllegalStateException("Source has been closed");
        }
        return source;
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
            log.warn("Read called on closed source");
            return -1;
        }

        try {
            // Read from InputStream in chunks, capped by our internal buffer size
            int toRead = (int) Math.min(length, (long) javaBuffer.length);

            int bytesRead = inputStream.read(javaBuffer, 0, toRead);

            if (bytesRead < 0) {
                // EOF
                log.trace("Read EOF at position {}", position);
                return 0;
            }

            if (bytesRead > 0) {
                // Copy from Java byte array to native buffer
                buffer.write(0, javaBuffer, 0, bytesRead);
                position += bytesRead;
                log.trace("Read {} bytes at position {}", bytesRead, position - bytesRead);
            }

            return bytesRead;

        } catch (IOException e) {
            log.error("Error reading from InputStream", e);
            return -1;
        } catch (Exception e) {
            log.error("Unexpected error in read callback", e);
            return -1;
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
            log.warn("Seek called on closed source");
            return -1;
        }

        try {
            // Support SEEK_SET to any position if ByteSource is available, otherwise only to 0 via mark/reset
            if (whence == 0) {
                if (offset == 0) {
                    if (byteSource != null) {
                        inputStream.close();
                        inputStream = byteSource.openStream();
                        position = 0;
                        log.trace("Seeked to beginning by reopening ByteSource");
                        return 0;
                    } else if (inputStream.markSupported()) {
                        try {
                            inputStream.reset();
                            position = 0;
                            log.trace("Seeked to beginning using reset");
                            return 0;
                        } catch (IOException e) {
                            log.debug("Reset failed (likely exceeded read limit or not marked): {}", e.getMessage());
                            return -1;
                        }
                    }
                } else if (byteSource != null) {
                    // SEEK_SET to non-zero offset
                    inputStream.close();
                    inputStream = byteSource.openStream();
                    ByteStreams.skipFully(inputStream, offset);
                    position = offset;
                    log.trace("Seeked to {} by reopening ByteSource", offset);
                    return position;
                }
            }

            // Seeking not supported for this stream
            log.trace("Seek not supported: whence={}, offset={}", whence, offset);
            return -1;

        } catch (IOException e) {
            log.debug("Error seeking in InputStream: {}", e.getMessage());
            return -1;
        } catch (Exception e) {
            log.error("Unexpected error in seek callback", e);
            return -1;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;

            // Unref the source object
            if (source != null && source != Pointer.NULL) {
                try {
                    vips.g_object_unref(source);
                    log.trace("Closed InputStreamVipsSource");
                } catch (Exception e) {
                    log.warn("Error unreffing VipsSource", e);
                }
            }

            // If we opened the stream from ByteSource, we should close it.
            // If it was provided to the constructor, the caller is responsible.
            if (byteSource != null && inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.debug("Error closing InputStream: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Check if this source supports seeking.
     * @return true if the underlying stream supports mark/reset OR if we have a ByteSource
     */
    public boolean isSeekable() {
        return byteSource != null || inputStream.markSupported();
    }

    /**
     * Get current read position.
     * @return bytes read from stream
     */
    public long getPosition() {
        return position;
    }
}
