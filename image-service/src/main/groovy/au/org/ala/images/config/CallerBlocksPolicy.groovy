package au.org.ala.images.config

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * A rejection handler that applies backpressure by blocking the submitting thread until a queue
 * slot frees up, rather than rejecting immediately (AbortPolicy) or running the task on the
 * caller thread (CallerRunsPolicy). If no slot frees within {@code submitTimeoutSeconds} the task
 * is rejected with a {@link RejectedExecutionException}.
 */
final class CallerBlocksPolicy implements RejectedExecutionHandler {
    private final String name
    private final long submitTimeoutSeconds

    CallerBlocksPolicy(String name, long submitTimeoutSeconds) {
        if (submitTimeoutSeconds < 0) {
            throw new IllegalArgumentException("submitTimeoutSeconds must be >= 0, was ${submitTimeoutSeconds}")
        }
        this.name = name
        this.submitTimeoutSeconds = submitTimeoutSeconds
    }

    @Override
    void rejectedExecution(Runnable task, ThreadPoolExecutor pool) {
        if (pool.isShutdown()) {
            throw new RejectedExecutionException("${name} is shut down, cannot accept task")
        }
        boolean accepted
        try {
            accepted = pool.queue.offer(task, submitTimeoutSeconds, TimeUnit.SECONDS)
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt()
            throw new RejectedExecutionException("Interrupted while waiting for a ${name} queue slot", ie)
        }
        if (!accepted) {
            throw new RejectedExecutionException("${name} queue still full after waiting ${submitTimeoutSeconds}s (queueSize=${pool.queue.size()} activeCount=${pool.activeCount} poolSize=${pool.poolSize} maximumPoolSize=${pool.maximumPoolSize})")
        }
        if (pool.isShutdown() && pool.queue.remove(task)) {
            throw new RejectedExecutionException("${name} shut down while waiting for a queue slot")
        }
    }
}
