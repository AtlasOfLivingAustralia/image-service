package au.org.ala.images.config

import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class CallerBlocksPolicySpec extends Specification {

    def "submit blocks until a queue slot frees, then the task runs on a pool thread"() {
        given: 'a 1-thread pool with a 1-slot queue, both occupied by a blocking task'
        def pool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1))
        pool.prestartAllCoreThreads()
        def policy = new CallerBlocksPolicy("Test pool", 5)
        def releaseFirst = new CountDownLatch(1)
        def firstStarted = new CountDownLatch(1)
        def taskThread = new AtomicReference<String>()
        def admittedTaskRan = new CountDownLatch(1)
        // Occupy the single worker thread
        pool.execute({ firstStarted.countDown(); releaseFirst.await(5, TimeUnit.SECONDS) } as Runnable)
        assert firstStarted.await(5, TimeUnit.SECONDS)
        // Fill the single queue slot with a second queued task
        pool.execute({ } as Runnable)
        assert pool.queue.remainingCapacity() == 0

        when: 'a third task is submitted via the policy on a separate caller thread'
        def caller = new Thread({
            policy.rejectedExecution({ taskThread.set(Thread.currentThread().name); admittedTaskRan.countDown() } as Runnable, pool)
        }, 'caller-thread')
        caller.start()

        then: 'the caller blocks on the full queue (not rejected, not running the task itself)'
        waitUntilBlocked(caller)
        caller.isAlive()
        taskThread.get() == null

        when: 'the first task finishes, freeing the worker so the queue drains'
        releaseFirst.countDown()
        caller.join(5000)
        admittedTaskRan.await(5, TimeUnit.SECONDS)

        then: 'the blocked caller was admitted and its task ran on a pool thread, not the caller'
        !caller.isAlive()
        taskThread.get() != null
        taskThread.get() != 'caller-thread'

        cleanup:
        pool.shutdownNow()
    }

    def "submit rejects after submitTimeoutSeconds when the queue stays full"() {
        given: 'a 1-thread pool with a full 1-slot queue that never drains'
        def pool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1))
        pool.prestartAllCoreThreads()
        def policy = new CallerBlocksPolicy("Test pool", 1)
        def releaseFirst = new CountDownLatch(1)
        def firstStarted = new CountDownLatch(1)
        pool.execute({ firstStarted.countDown(); releaseFirst.await(10, TimeUnit.SECONDS) } as Runnable)
        assert firstStarted.await(5, TimeUnit.SECONDS)
        pool.execute({ } as Runnable) // fill the queue slot
        assert pool.queue.remainingCapacity() == 0

        when: 'the policy tries to admit a task but no slot frees within the timeout'
        long start = System.nanoTime()
        policy.rejectedExecution({ } as Runnable, pool)

        then: 'it fails loud with RejectedExecutionException after roughly the submit timeout'
        thrown(RejectedExecutionException)
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
        elapsedMs >= 900 // blocked for the ~1s submit timeout, not instantly

        cleanup:
        releaseFirst.countDown()
        pool.shutdownNow()
    }

    def "construction rejects a negative submit timeout"() {
        when:
        new CallerBlocksPolicy("Test pool", -1)

        then:
        thrown(IllegalArgumentException)
    }

    def "derivativeLoaderExecutor rejects invalid pool configuration at startup"() {
        when:
        new ImageServiceConfiguration().derivativeLoaderExecutor(threads, queueCapacity, 5)

        then:
        thrown(IllegalArgumentException)

        where:
        threads | queueCapacity
        0       | 100
        -1      | 100
        4       | -1
    }

    private static boolean waitUntilBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (System.nanoTime() < deadline) {
            if (thread.state == Thread.State.TIMED_WAITING) {
                return true
            }
            Thread.onSpinWait()
        }
        return false
    }
}
