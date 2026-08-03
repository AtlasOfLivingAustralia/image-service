package au.org.ala.images.storage

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.IntConsumer
import java.util.function.IntSupplier
import java.util.function.Function

/** Pure CRT safeguards kept separate from S3 client and CRT global initialization. */
class S3CrtSafeguards {

    static int requirePositive(String property, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("${property} must be a positive integer")
        }
        return value
    }

    static int requireNonNegative(String property, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("${property} must be zero or a positive integer")
        }
        return value
    }

    static boolean applyBootstrapIfEligible(int eventLoopThreads, boolean clientAlreadyCreated, Runnable setStaticDefault) {
        if (eventLoopThreads == 0 || clientAlreadyCreated) {
            return false
        }
        setStaticDefault.run()
        return true
    }

    /** Resolves the Grails configuration boundary lazily, with a system-property fallback. */
    static int resolveConfiguredEventLoopThreads(Function<String, Integer> configuredValue, IntSupplier systemFallback) {
        Integer configuredThreads = configuredValue.apply('aws.s3.crt.event-loop-threads')
        int eventLoopThreads = configuredThreads != null ? configuredThreads : systemFallback.asInt
        return requireNonNegative('aws.s3.crt.event-loop-threads', eventLoopThreads)
    }

    /** Applies an already-resolved configuration value immediately before CRT client construction. */
    static boolean applyConfiguredBootstrap(IntSupplier configuredThreads, boolean clientAlreadyCreated,
                                            IntConsumer setStaticDefault) {
        if (clientAlreadyCreated) {
            return false
        }
        int eventLoopThreads = configuredThreads.asInt
        if (eventLoopThreads == 0) {
            return false
        }
        setStaticDefault.accept(eventLoopThreads)
        return true
    }

    static ThreadPoolExecutor newCompletionExecutor(int threads, int queueCapacity) {
        int validatedThreads = requirePositive('aws.s3.crt.future-completion-threads', threads)
        int validatedQueueCapacity = requirePositive('aws.s3.crt.future-completion-queue-capacity', queueCapacity)
        return new ThreadPoolExecutor(
                validatedThreads, validatedThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(validatedQueueCapacity),
                { Runnable runnable -> new Thread(runnable, 's3-crt-future-completion-test') } as ThreadFactory,
                { Runnable task, ThreadPoolExecutor executor ->
                    throw new RejectedExecutionException('S3 CRT future-completion executor is saturated')
                } as java.util.concurrent.RejectedExecutionHandler)
    }

    static void scheduleEvictedResourceClose(AutoCloseable resource, ScheduledExecutorService scheduler, long delay, TimeUnit unit) {
        scheduler.schedule({
            try {
                resource?.close()
            } catch (Exception ignored) {
                // Cache eviction must not prevent other queued resource closes.
            }
        }, delay, unit)
    }

    static void configureMaxConcurrency(int maxConcurrency, Consumer<Integer> configure) {
        configure.accept(requirePositive('aws.s3.crt.max-concurrency', maxConcurrency))
    }
}
