package au.org.ala.images.storage

import groovy.transform.CompileStatic
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager

import java.time.Instant
import java.util.concurrent.CompletableFuture

/**
 * Best-effort, null-safe diagnostic snapshot of the shared streaming-upload pipeline
 * (S3TransferManager + its underlying S3AsyncClient).
 *
 * The AWS SDK does not expose queued-vs-inflight upload counts publicly
 * (the Java S3TransferManager has no S3ClientStats-style accessor), so this captures
 * only identity and lifecycle state that lets logs determine whether the SAME
 * transfer manager / async client is being reused across successive failures.
 */
@CompileStatic
final class S3UploadHealth {

    private S3UploadHealth() {
        // utility holder; not instantiable
    }

    /**
     * Builds a cheap diagnostic map describing the shared upload pipeline state.
     * Never throws; every accessor is guarded.
     *
     * @param transferManager the shared S3TransferManager (may be null)
     * @param asyncClient     the shared S3AsyncClient backing the manager (may be null)
     * @param uploadFuture    the per-upload completion future (may be null)
     * @return an immutable-ish LinkedHashMap of diagnostic attributes
     */
    static Map<String, Object> snapshot(S3TransferManager transferManager, S3AsyncClient asyncClient, CompletableFuture<?> uploadFuture) {
        Map<String, Object> state = new LinkedHashMap<>()
        state.transferManagerId = identityHex(transferManager)
        state.s3AsyncClientId = identityHex(asyncClient)
        state.uploadFutureDone = uploadFuture == null ? null : Boolean.valueOf(uploadFuture.isDone())
        state.uploadFutureCancelled = uploadFuture == null ? null : Boolean.valueOf(uploadFuture.isCancelled())
        state.uploadFutureError = failureOf(uploadFuture)
        state.threadName = currentThreadName()
        state.timestamp = Instant.now().toString()
        return state
    }

    /**
     * Renders {@link #snapshot} as a compact, single-line, log-friendly string.
     */
    static String describe(S3TransferManager transferManager, S3AsyncClient asyncClient, CompletableFuture<?> uploadFuture) {
        Map<String, Object> state = snapshot(transferManager, asyncClient, uploadFuture)
        return state.collect { Map.Entry<String, Object> entry -> "${entry.key}=${entry.value}" }.join(' ')
    }

    /**
     * Root cause of an already-failed upload future, or null. When the upload fails before
     * subscribing the body, the subscribe timeout otherwise hides the real error.
     */
    private static String failureOf(CompletableFuture<?> uploadFuture) {
        if (uploadFuture == null || !uploadFuture.isCompletedExceptionally() || uploadFuture.isCancelled()) {
            return null
        }
        try {
            uploadFuture.getNow(null)
            return null
        } catch (Throwable thrown) {
            Throwable root = thrown
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause()
            }
            return root.toString()
        }
    }

    private static String identityHex(Object instance) {
        if (instance == null) {
            return 'null'
        }
        return Integer.toHexString(System.identityHashCode(instance))
    }

    private static String currentThreadName() {
        Thread current = Thread.currentThread()
        return current == null ? 'unknown' : current.getName()
    }

    static boolean isSubscribeTimeout(Throwable throwable) {
        return throwable instanceof IllegalStateException &&
                throwable.message != null &&
                throwable.message ==~ /(?s).*was not made within .* seconds.*/
    }
}
