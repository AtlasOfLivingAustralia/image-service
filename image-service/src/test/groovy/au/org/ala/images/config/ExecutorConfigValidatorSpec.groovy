package au.org.ala.images.config

import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import spock.lang.Specification

/**
 * Tests for {@link ExecutorConfigValidator}: the defense-in-depth check that catches
 * a silently misconfigured {@code derivativeLoaderExecutor} after context refresh.
 */
class ExecutorConfigValidatorSpec extends Specification {

    def "validator passes when executor is correctly configured"() {
        given: 'a properly configured ThreadPoolTaskExecutor'
        def executor = new ThreadPoolTaskExecutor()
        executor.corePoolSize = 4
        executor.maxPoolSize = 4
        executor.queueCapacity = 100
        executor.afterPropertiesSet()

        and: 'a validator wired to it'
        def validator = new ExecutorConfigValidator()
        validator.derivativeLoaderExecutor = executor

        when: 'afterSingletonsInstantiated is called'
        validator.afterSingletonsInstantiated()

        then: 'no exception is thrown'
        notThrown(IllegalStateException)

        cleanup:
        executor.shutdown()
    }

    def "validator throws when corePoolSize is zero"() {
        given: 'an executor with corePoolSize = 0 (the old @Value failure mode)'
        // Note: afterPropertiesSet() would throw because the JDK ThreadPoolExecutor
        // rejects corePoolSize=0, but the validator reads the raw configured properties
        // from the ThreadPoolTaskExecutor wrapper, not the live pool — so we can test
        // the validation logic without initialising the underlying pool.
        def executor = new ThreadPoolTaskExecutor()
        executor.corePoolSize = 0
        executor.maxPoolSize = 0
        executor.queueCapacity = 100

        and:
        def validator = new ExecutorConfigValidator()
        validator.derivativeLoaderExecutor = executor

        when:
        validator.afterSingletonsInstantiated()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('corePoolSize')
    }

    def "validator throws when corePoolSize differs from maxPoolSize"() {
        given: 'an executor with mismatched core and max pool sizes'
        def executor = new ThreadPoolTaskExecutor()
        executor.corePoolSize = 2
        executor.maxPoolSize = 4
        executor.queueCapacity = 100
        executor.afterPropertiesSet()

        and:
        def validator = new ExecutorConfigValidator()
        validator.derivativeLoaderExecutor = executor

        when:
        validator.afterSingletonsInstantiated()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('must equal')

        cleanup:
        executor.shutdown()
    }

    def "validator throws when queueCapacity is negative"() {
        given: 'an executor with queueCapacity = -1 (unbounded, the old @Value failure mode)'
        def executor = new ThreadPoolTaskExecutor()
        executor.corePoolSize = 4
        executor.maxPoolSize = 4
        executor.queueCapacity = -1
        executor.afterPropertiesSet()

        and:
        def validator = new ExecutorConfigValidator()
        validator.derivativeLoaderExecutor = executor

        when:
        validator.afterSingletonsInstantiated()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('queueCapacity')

        cleanup:
        executor.shutdown()
    }

    def "validator skips non-ThreadPoolTaskExecutor implementations"() {
        given: 'a non-ThreadPoolTaskExecutor TaskExecutor'
        TaskExecutor executor = { Runnable task -> task.run() } as TaskExecutor

        and:
        def validator = new ExecutorConfigValidator()
        validator.derivativeLoaderExecutor = executor

        when:
        validator.afterSingletonsInstantiated()

        then: 'no exception is thrown'
        notThrown(IllegalStateException)
    }
}
