package au.org.ala.images

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
        def policy = new Application.CallerBlocksPolicy(5)
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
        // Give the caller a moment to block on the full queue
        Thread.sleep(200)

        then: 'the caller is still blocked (not rejected, not running the task itself)'
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
        def policy = new Application.CallerBlocksPolicy(1)
        def releaseFirst = new CountDownLatch(1)
        pool.execute({ releaseFirst.await(10, TimeUnit.SECONDS) } as Runnable)
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

    def "policy never runs the task on the calling thread"() {
        given: 'a saturated pool; the task must never execute on the caller even while blocking'
        def pool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1))
        pool.prestartAllCoreThreads()
        def policy = new Application.CallerBlocksPolicy(1)
        def releaseFirst = new CountDownLatch(1)
        pool.execute({ releaseFirst.await(10, TimeUnit.SECONDS) } as Runnable)
        pool.execute({ } as Runnable)
        def callerName = Thread.currentThread().name
        def ranOnCaller = new AtomicReference<Boolean>(false)

        when: 'the policy blocks and then rejects'
        try {
            policy.rejectedExecution({ ranOnCaller.set(Thread.currentThread().name == callerName) } as Runnable, pool)
        } catch (RejectedExecutionException expected) {
            // expected: the point is the task body never executed on this caller thread
        }

        then: 'the task body was never invoked on the calling thread'
        !ranOnCaller.get()

        cleanup:
        releaseFirst.countDown()
        pool.shutdownNow()
    }

    def "construction rejects a negative submit timeout"() {
        when:
        new Application.CallerBlocksPolicy(-1)

        then:
        thrown(IllegalArgumentException)
    }
}
