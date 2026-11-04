package au.org.ala.images.storage

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
import software.amazon.awssdk.core.exception.ApiCallTimeoutException

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Tracks producer progress for one S3 upload so a failure can be logged with enough context to tell
 * a stuck connection (no data moving) from a timeout that fired while data was still flowing
 * (a slow producer, or a timeout shorter than the upload).
 */
@Slf4j
@CompileStatic
final class S3UploadProgress {

    /** A failure within this long after the last producer write counts as "producer still active". */
    static final long ACTIVE_WINDOW_MS = 2000

    private final String kind
    private final String bucket
    private final String key
    private final long openedNanos = System.nanoTime()
    private final AtomicLong bytes = new AtomicLong()
    private final AtomicLong lastProgressNanos = new AtomicLong(openedNanos)
    private volatile boolean producerFinished
    private final AtomicBoolean reported = new AtomicBoolean()

    S3UploadProgress(String kind, String bucket, String key) {
        this.kind = kind
        this.bucket = bucket
        this.key = key
    }

    void progressed(long count) {
        if (count > 0) {
            bytes.addAndGet(count)
            lastProgressNanos.set(System.nanoTime())
        }
    }

    void producerFinished() {
        producerFinished = true
        lastProgressNanos.set(System.nanoTime())
    }

    long getBytes() { bytes.get() }

    long getMillisSinceProgress() { (System.nanoTime() - lastProgressNanos.get()).intdiv(1_000_000L) as long }

    long getElapsedMillis() { (System.nanoTime() - openedNanos).intdiv(1_000_000L) as long }

    static boolean isTimeout(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.cause == cause ? null : cause.cause) {
            if (cause instanceof ApiCallTimeoutException || cause instanceof ApiCallAttemptTimeoutException
                    || cause instanceof TimeoutException || cause instanceof S3UploadCompletionTimeoutException
                    || cause instanceof S3ApplicationStreamIdleTimeoutException) {
                return true
            }
        }
        return false
    }

    /** Logs one WARN per upload (later calls are ignored). Never throws. */
    void reportFailure(Throwable failure, String health = null) {
        if (failure == null || !reported.compareAndSet(false, true)) {
            return
        }
        try {
            Throwable root = failure
            while (root.cause != null && root.cause != root) {
                root = root.cause
            }
            long sinceProgress = millisSinceProgress
            boolean timeout = isTimeout(failure)
            String verdict
            if (timeout && !producerFinished && sinceProgress < ACTIVE_WINDOW_MS) {
                verdict = 'TIMED OUT WHILE PRODUCER WAS STILL WRITING - likely a slow producer or a timeout shorter than the upload, not a stuck connection'
            } else if (timeout && producerFinished && sinceProgress < ACTIVE_WINDOW_MS) {
                verdict = 'timed out just after the producer finished - upload may have been slow rather than stuck'
            } else if (timeout) {
                verdict = 'timed out with no recent producer progress - consistent with a stuck connection/client'
            } else {
                verdict = 'failed'
            }
            log.warn('S3 {} upload {}: bucket={} key={} cause={} bytesWritten={} elapsedMs={} msSinceLastProgress={} producerFinished={}{}',
                    kind, verdict, bucket, key, root.toString(), bytes.get(), elapsedMillis, sinceProgress, producerFinished,
                    health ? " ${health}" : '')
        } catch (Exception e) {
            log.debug('Failed to report S3 upload failure', e)
        }
    }

    /** Input stream wrapper that records read progress (for the ingest path). */
    InputStream track(InputStream input) {
        final S3UploadProgress progress = this
        return new FilterInputStream(input) {
            @Override
            int read() throws IOException {
                int value = super.read()
                if (value >= 0) progress.progressed(1) else progress.producerFinished()
                return value
            }

            @Override
            int read(byte[] b, int off, int len) throws IOException {
                int count = super.read(b, off, len)
                if (count > 0) progress.progressed(count) else if (count < 0) progress.producerFinished()
                return count
            }
        }
    }
}
