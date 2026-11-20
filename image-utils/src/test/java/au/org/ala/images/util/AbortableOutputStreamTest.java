package au.org.ala.images.util;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbortableOutputStreamTest {

    @Test
    public void abortQuietlyAttachesCheckedExceptionThrownByAbort() {
        IOException abortFailure = new IOException("close failed");
        AbortableFilterOutputStream stream = new AbortableFilterOutputStream(new ByteArrayOutputStream()) {
            @Override
            protected void onAbort(Throwable cause) {
                sneakyThrow(abortFailure);
            }
        };
        RuntimeException cause = new RuntimeException("encoder failed");

        AbortableOutputStream.abortQuietly(stream, cause);

        assertTrue(stream.isAborted());
        assertArrayEquals(new Throwable[] { abortFailure }, cause.getSuppressed());
    }

    @Test
    public void abortQuietlyIgnoresNonAbortableStreams() {
        RuntimeException cause = new RuntimeException("encoder failed");

        AbortableOutputStream.abortQuietly(new ByteArrayOutputStream(), cause);

        assertEquals(0, cause.getSuppressed().length);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }
}
