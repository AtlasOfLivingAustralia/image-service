package au.org.ala.images.util;

import java.io.OutputStream;

/**
 * An output stream whose data is only published when it is closed normally. Streams from
 * {@link ByteSinkFactory} sinks may implement this; producers that fail part way through writing
 * must call {@link #abortQuietly(OutputStream, Throwable)} before the stream is closed, otherwise
 * close() cannot tell a failure from a normal end of data and may publish a truncated object.
 */
public interface AbortableOutputStream {

    /**
     * Discards the data written so far. A subsequent close() must not publish anything and is a no-op.
     */
    void abort(Throwable cause);

    /**
     * Aborts {@code stream} if it supports it; never throws. Failures are attached to {@code cause}.
     */
    static void abortQuietly(OutputStream stream, Throwable cause) {
        if (stream instanceof AbortableOutputStream) {
            try {
                ((AbortableOutputStream) stream).abort(cause);
            } catch (Exception e) {
                if (cause != null && cause != e) {
                    cause.addSuppressed(e);
                }
            }
        }
    }
}
