package au.org.ala.images.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Base for sink streams that publish on close: after {@link #abort} writes fail, flush is ignored
 * and subclasses must make close() a no-op (check {@link #isAborted()}).
 */
public abstract class AbortableFilterOutputStream extends FilterOutputStream implements AbortableOutputStream {

    private volatile boolean aborted;

    protected AbortableFilterOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public final void abort(Throwable cause) {
        if (aborted) {
            return;
        }
        aborted = true;
        onAbort(cause);
    }

    /** Called once on the first abort; must discard (not publish) the written data. */
    protected abstract void onAbort(Throwable cause);

    public boolean isAborted() {
        return aborted;
    }

    @Override
    public void write(int b) throws IOException {
        ensureNotAborted();
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureNotAborted();
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        if (!aborted) {
            out.flush();
        }
    }

    private void ensureNotAborted() throws IOException {
        if (aborted) {
            throw new IOException("Stream has been aborted");
        }
    }
}
