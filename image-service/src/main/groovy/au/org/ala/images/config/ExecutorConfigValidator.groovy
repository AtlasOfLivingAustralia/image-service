package au.org.ala.images.config

import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.DependsOn
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Component

import org.springframework.beans.factory.SmartInitializingSingleton

/**
 * Validates that the {@code derivativeLoaderExecutor} bean was configured with sane pool
 * parameters after all singletons have been instantiated.
 *
 * This is defense-in-depth: the primary gate is the startup log emitted by
 * {@code Application.createThreadPoolTaskExecutor}, but this validator catches the case where
 * the executor was silently constructed with degenerate values (e.g. a {@code @Value} field
 * that resolved to its Groovy default of {@code 0} because injection ordering failed).
 *
 * Lives as a separate bean rather than inside {@link ImageServiceConfiguration} to avoid a
 * configuration-bean/executor creation cycle.
 */
@Slf4j
@Component
@DependsOn("derivativeLoaderExecutor")
class ExecutorConfigValidator implements SmartInitializingSingleton {

    @Autowired
    @Qualifier("derivativeLoaderExecutor")
    TaskExecutor derivativeLoaderExecutor

    @Override
    void afterSingletonsInstantiated() {
        if (!(derivativeLoaderExecutor instanceof ThreadPoolTaskExecutor)) {
            log.debug("derivativeLoaderExecutor is not a ThreadPoolTaskExecutor ({}); skipping pool validation",
                    derivativeLoaderExecutor?.class?.name)
            return
        }

        ThreadPoolTaskExecutor pool = (ThreadPoolTaskExecutor) derivativeLoaderExecutor
        int core = pool.corePoolSize
        int max = pool.maxPoolSize
        int queue = pool.queueCapacity

        if (core <= 0) {
            throw new IllegalStateException(
                    "derivativeLoaderExecutor corePoolSize must be > 0 but was ${core}; " +
                    "check that derivative.loader.threads is being resolved correctly")
        }
        if (max <= 0) {
            throw new IllegalStateException(
                    "derivativeLoaderExecutor maxPoolSize must be > 0 but was ${max}; " +
                    "check that derivative.loader.threads is being resolved correctly")
        }
        if (core != max) {
            throw new IllegalStateException(
                    "derivativeLoaderExecutor corePoolSize (${core}) must equal maxPoolSize (${max}); " +
                    "the pool is intentionally fixed-size")
        }
        if (queue < 0) {
            throw new IllegalStateException(
                    "derivativeLoaderExecutor queueCapacity must be >= 0 but was ${queue}; " +
                    "check that derivative.loader.queueCapacity is being resolved correctly")
        }

        log.info("derivativeLoaderExecutor validated: core={} max={} queue={}", core, max, queue)
    }
}
