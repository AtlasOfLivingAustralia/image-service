package au.org.ala.images.storage

import groovy.transform.CompileStatic
import software.amazon.awssdk.core.ResponseInputStream

import java.time.Duration

/**
 * Owns an application-level idle deadline for an SDK async blocking response.
 * It is intentionally limited to ResponseInputStream so abort can cancel the
 * SDK subscription that is servicing a blocked application read.
 */
@CompileStatic
class S3ApplicationWatchdogInputStream<ResponseT> extends FilterInputStream {

    private final ResponseInputStream<ResponseT> responseInputStream
    private final S3ApplicationStreamWatchdog watchdog
    private final String description

    S3ApplicationWatchdogInputStream(ResponseInputStream<ResponseT> responseInputStream, Duration idleTimeout, String description) {
        super(Objects.requireNonNull(responseInputStream, 'responseInputStream'))
        this.responseInputStream = responseInputStream
        this.description = Objects.requireNonNull(description, 'description')
        watchdog = new S3ApplicationStreamWatchdog(idleTimeout, { responseInputStream.abort() } as Runnable)
    }

    boolean isTimedOut() { watchdog.state == S3ApplicationStreamWatchdogState.TIMED_OUT }

    S3ApplicationStreamIdleTimeoutException timeoutException(Throwable cause = null) {
        new S3ApplicationStreamIdleTimeoutException("S3 application stream was idle while reading ${description}", cause)
    }

    @Override
    int read() throws IOException {
        readWithProgress({ super.read() } as ReadOperation)
    }

    @Override
    int read(byte[] bytes) throws IOException {
        readWithProgress({ super.read(bytes) } as ReadOperation)
    }

    @Override
    int read(byte[] bytes, int offset, int length) throws IOException {
        readWithProgress({ super.read(bytes, offset, length) } as ReadOperation)
    }

    @Override
    void close() throws IOException {
        try {
            super.close()
        } catch (Throwable exception) {
            throw timeoutOr(exception)
        } finally {
            watchdog.closed()
        }
    }

    private int readWithProgress(ReadOperation operation) throws IOException {
        try {
            int count = operation.read()
            if (count < 0) {
                watchdog.completed()
            } else {
                watchdog.progressed(count == 0 ? 0 : count)
            }
            return count
        } catch (Throwable exception) {
            throw timeoutOr(exception)
        }
    }

    private IOException timeoutOr(Throwable exception) {
        if (isTimedOut()) {
            return timeoutException(exception)
        }
        watchdog.failed()
        if (exception instanceof IOException) {
            return exception as IOException
        }
        return new IOException("S3 application stream failed while reading ${description}", exception)
    }

    @CompileStatic
    private static interface ReadOperation {
        int read() throws IOException
    }
}
