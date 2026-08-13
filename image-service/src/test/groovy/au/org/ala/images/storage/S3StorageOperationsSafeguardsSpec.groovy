package au.org.ala.images.storage

import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class S3StorageOperationsSafeguardsSpec extends Specification {

    def 'CRT configuration guards accept documented defaults and reject invalid values'() {
        expect:
        S3CrtSafeguards.requirePositive('aws.s3.crt.max-concurrency', 32) == 32
        S3CrtSafeguards.requirePositive('aws.s3.crt.future-completion-threads', 2) == 2
        S3CrtSafeguards.requirePositive('aws.s3.crt.future-completion-queue-capacity', 256) == 256
        S3CrtSafeguards.requireNonNegative('aws.s3.crt.event-loop-threads', 0) == 0

        when:
        S3CrtSafeguards.requirePositive(property, value)

        then:
        def error = thrown(IllegalArgumentException)
        error.message == "${property} must be a positive integer"

        where:
        property                                          | value
        'aws.s3.crt.max-concurrency'                      | -1
        'aws.s3.crt.future-completion-threads'            | 0
        'aws.s3.crt.future-completion-queue-capacity'     | -1
    }

    def 'event-loop bootstrap does not mutate CRT state for no-op or late initialization'() {
        given:
        def invocations = new AtomicInteger()
        def setStaticDefault = { invocations.incrementAndGet() } as Runnable

        expect:
        !S3CrtSafeguards.applyBootstrapIfEligible(0, false, setStaticDefault)
        !S3CrtSafeguards.applyBootstrapIfEligible(2, true, setStaticDefault)
        invocations.get() == 0

        when:
        def initialized = S3CrtSafeguards.applyBootstrapIfEligible(2, false, setStaticDefault)

        then:
        initialized
        invocations.get() == 1
    }

    def 'configured event-loop bootstrap gives Grails YAML precedence and passes its value to the setter'() {
        given:
        def configuredThreads = new AtomicInteger()

        when:
        def resolvedThreads = S3CrtSafeguards.resolveConfiguredEventLoopThreads(
                { String property -> property == 'aws.s3.crt.event-loop-threads' ? 7 : null },
                { -> 3 })
        def initialized = S3CrtSafeguards.applyConfiguredBootstrap({ -> resolvedThreads }, false,
                { int value -> configuredThreads.set(value) } as java.util.function.IntConsumer)

        then:
        initialized
        configuredThreads.get() == 7
    }

    def 'event-loop resolution rejects negative Grails configuration before CRT construction'() {
        when:
        S3CrtSafeguards.resolveConfiguredEventLoopThreads(
                { String ignored -> -1 },
                { -> 0 })

        then:
        def error = thrown(IllegalArgumentException)
        error.message == 'aws.s3.crt.event-loop-threads must be zero or a positive integer'
    }

    def 'CRT completion executor rejects saturation and is only closed by its owner'() {
        given:
        ThreadPoolExecutor executor = S3CrtSafeguards.newCompletionExecutor(1, 1)
        def running = new CountDownLatch(1)
        def release = new CountDownLatch(1)
        executor.execute {
            running.countDown()
            release.await()
        }
        assert running.await(5, TimeUnit.SECONDS)
        executor.execute({ } as Runnable)

        when:
        executor.execute({ } as Runnable)

        then:
        thrown(RejectedExecutionException)
        !executor.shutdown

        cleanup:
        release?.countDown()
        executor?.shutdownNow()
    }

    def 'cache eviction closes only its resource and leaves completion executor available'() {
        given:
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
        ThreadPoolExecutor completionExecutor = S3CrtSafeguards.newCompletionExecutor(1, 1)
        def closed = new AtomicBoolean()

        when:
        S3CrtSafeguards.scheduleEvictedResourceClose({ closed.set(true) } as AutoCloseable, scheduler, 0, TimeUnit.MILLISECONDS)
        scheduler.shutdown()

        then:
        scheduler.awaitTermination(5, TimeUnit.SECONDS)
        closed.get()
        !completionExecutor.shutdown

        cleanup:
        scheduler?.shutdownNow()
        completionExecutor?.shutdownNow()
    }

    def 'CRT max concurrency is applied through the builder configuration seam'() {
        given:
        def configuredConcurrency = new AtomicInteger()

        when:
        S3CrtSafeguards.configureMaxConcurrency(17, { int value -> configuredConcurrency.set(value) })

        then:
        configuredConcurrency.get() == 17
    }
}
