package au.org.ala.images.storage

import groovy.transform.CompileStatic
import software.amazon.awssdk.utils.CancellableOutputStream

import java.time.Duration

/** Output stream wrapper for the SDK boundary that documents cancellation support. */
@CompileStatic
class S3ApplicationWatchdogOutputStream extends FilterOutputStream {

    private final S3ApplicationStreamWatchdog watchdog
    private final String description

    S3ApplicationWatchdogOutputStream(CancellableOutputStream delegate, Duration idleTimeout, String description) {
        super(delegate)
        this.description = description
        watchdog = new S3ApplicationStreamWatchdog(idleTimeout, { delegate.cancel() } as Runnable)
    }

    void completed() { watchdog.completed() }
    void failed() { watchdog.failed() }

    boolean isTimedOut() { watchdog.state == S3ApplicationStreamWatchdogState.TIMED_OUT }

    S3ApplicationStreamIdleTimeoutException timeoutException(Throwable cause = null) {
        return new S3ApplicationStreamIdleTimeoutException("S3 application stream was idle while writing ${description}", cause)
    }

    @Override
    void write(int value) throws IOException {
        try {
            super.write(value)
            watchdog.progressed(1)
        } catch (IOException exception) {
            throw timeoutOr(exception)
        }
    }

    @Override
    void write(byte[] bytes, int offset, int length) throws IOException {
        try {
            super.write(bytes, offset, length)
            watchdog.progressed(length)
        } catch (IOException exception) {
            throw timeoutOr(exception)
        }
    }

    @Override
    void close() throws IOException {
        try {
            super.close()
        } catch (IOException exception) {
            throw timeoutOr(exception)
        }
    }

    private IOException timeoutOr(IOException exception) {
        if (watchdog.state == S3ApplicationStreamWatchdogState.TIMED_OUT) {
            return timeoutException(exception)
        }
        watchdog.failed()
        return exception
    }
}
