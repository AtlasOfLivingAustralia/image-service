package au.org.ala.images.storage

import groovy.transform.CompileStatic

import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.function.LongSupplier

@CompileStatic
enum S3ApplicationStreamWatchdogState {
    ACTIVE, TIMED_OUT, COMPLETED, CLOSED, FAILED
}

@CompileStatic
interface S3ApplicationStreamWatchdogTask {
    void cancel()
}

@CompileStatic
interface S3ApplicationStreamWatchdogScheduler {
    S3ApplicationStreamWatchdogTask schedule(Runnable task, long delayNanos)
}

/**
 * One-shot, resettable idle watchdog.  The supplied cancellation action must
 * unblock the operation that is currently waiting for application progress.
 */
@CompileStatic
class S3ApplicationStreamWatchdog {

    private final long timeoutNanos
    private final LongSupplier nanoClock
    private final S3ApplicationStreamWatchdogScheduler scheduler
    private final Runnable timeoutAction

    private S3ApplicationStreamWatchdogTask scheduledTask
    private long deadlineNanos
    private long generation
    private S3ApplicationStreamWatchdogState state = S3ApplicationStreamWatchdogState.ACTIVE

    S3ApplicationStreamWatchdog(Duration timeout, Runnable timeoutAction) {
        this(timeout, System.&nanoTime as LongSupplier, new ExecutorScheduler(), timeoutAction)
    }

    S3ApplicationStreamWatchdog(Duration timeout, LongSupplier nanoClock,
                                S3ApplicationStreamWatchdogScheduler scheduler, Runnable timeoutAction) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException('S3 application stream idle timeout must be positive')
        }
        this.timeoutNanos = timeout.toNanos()
        this.nanoClock = Objects.requireNonNull(nanoClock, 'nanoClock')
        this.scheduler = Objects.requireNonNull(scheduler, 'scheduler')
        this.timeoutAction = Objects.requireNonNull(timeoutAction, 'timeoutAction')
        arm()
    }

    synchronized void progressed(long byteCount) {
        if (byteCount <= 0 || state != S3ApplicationStreamWatchdogState.ACTIVE) {
            return
        }
        arm()
    }

    synchronized void completed() {
        terminate(S3ApplicationStreamWatchdogState.COMPLETED)
    }

    synchronized void closed() {
        terminate(S3ApplicationStreamWatchdogState.CLOSED)
    }

    synchronized void failed() {
        terminate(S3ApplicationStreamWatchdogState.FAILED)
    }

    synchronized S3ApplicationStreamWatchdogState getState() {
        return state
    }

    private void arm() {
        cancelScheduledTask()
        deadlineNanos = nanoClock.getAsLong() + timeoutNanos
        long armedGeneration = ++generation
        scheduledTask = scheduler.schedule({ expire(armedGeneration) } as Runnable, timeoutNanos)
    }

    private void expire(long callbackGeneration) {
        Runnable action = null
        synchronized (this) {
            if (state != S3ApplicationStreamWatchdogState.ACTIVE || callbackGeneration != generation) {
                return
            }
            long remainingNanos = deadlineNanos - nanoClock.getAsLong()
            if (remainingNanos > 0) {
                scheduledTask = scheduler.schedule({ expire(callbackGeneration) } as Runnable, remainingNanos)
                return
            }
            state = S3ApplicationStreamWatchdogState.TIMED_OUT
            cancelScheduledTask()
            action = timeoutAction
        }
        try {
            action.run()
        } catch (Throwable ignored) {
            // The blocked caller receives the timeout after its cancellation action unblocks it.
        }
    }

    private void terminate(S3ApplicationStreamWatchdogState terminalState) {
        if (state != S3ApplicationStreamWatchdogState.ACTIVE) {
            return
        }
        state = terminalState
        ++generation
        cancelScheduledTask()
    }

    private void cancelScheduledTask() {
        scheduledTask?.cancel()
        scheduledTask = null
    }

    @CompileStatic
    private static class ExecutorScheduler implements S3ApplicationStreamWatchdogScheduler {
        private static final ScheduledExecutorService executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor({ Runnable runnable ->
            Thread thread = new Thread(runnable, 's3-application-stream-watchdog')
            thread.daemon = true
            return thread
        } as java.util.concurrent.ThreadFactory)

        @Override
        S3ApplicationStreamWatchdogTask schedule(Runnable task, long delayNanos) {
            ScheduledFuture<?> future = executor.schedule(task, delayNanos, TimeUnit.NANOSECONDS)
            return { future.cancel(false) } as S3ApplicationStreamWatchdogTask
        }
    }
}
