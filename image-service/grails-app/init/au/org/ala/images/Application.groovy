package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ProcessCommandExecutor
import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import groovy.util.logging.Slf4j
import org.springframework.context.annotation.Bean
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

import javax.annotation.PostConstruct
import javax.imageio.ImageIO
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Slf4j
class Application extends GrailsAutoConfiguration {

    public static final int TILE_SIZE = 256

    /**
     * Static logger for {@link #createThreadPoolTaskExecutor} — the helper is
     * {@code private static} so the instance-level {@code log} from {@code @Slf4j}
     * is not in scope.
     */
    private static final org.slf4j.Logger EXEC_LOG =
            org.slf4j.LoggerFactory.getLogger("au.org.ala.images.ThreadPoolExecutors")

    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

    @Bean
    TaskExecutor analyticsExecutor() {
        return createThreadPoolTaskExecutor("analytics-", 1)
    }

    @Bean
    TaskExecutor storageLocationExecutor() {
        return createThreadPoolTaskExecutor("storage-", 1)
    }

    @Bean
    StorageOperationsRegistry storageOperationsRegistry() {
        return new StorageOperationsRegistry(this.grailsApplication)
    }

    // This is a workaround, instead of using @EnableConfigurationProperties above
    // we create the bean manually then load it using the ImageOptimisationConfigLoader
    // (which is a @Component)
    @Bean
    ImageOptimisationConfig imageOptimisationConfig() {
        return new ImageOptimisationConfig()
    }

    @Bean
    CommandExecutor commandExecutor() {
        return new ProcessCommandExecutor()
    }

    @PostConstruct
    void init() {
        log.info("Initialising ImageIO settings...")
        ImageIO.scanForPlugins()
        ImageIO.setUseCache(false)
    }

    static TaskExecutor createThreadPoolTaskExecutor(String namePrefix, int coreSize, int maxSize = coreSize, int queueCapacity = -1, RejectedExecutionHandler rejectionHandler = null) {
        EXEC_LOG.info("executor[{}]: requested core={} max={} queue={} rejection={} -> applied core={} max={} queue={}",
                namePrefix, coreSize, maxSize, queueCapacity, rejectionHandler?.class?.simpleName,
                Math.max(1, coreSize), Math.max(1, maxSize), queueCapacity >= 0 ? queueCapacity : 'unbounded')
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor()
        executor.corePoolSize = Math.max(1, coreSize)
        executor.maxPoolSize = Math.max(1, maxSize)
        if (queueCapacity >= 0) {
            executor.queueCapacity = queueCapacity
        }
        executor.threadNamePrefix = namePrefix
        executor.waitForTasksToCompleteOnShutdown = true
        executor.awaitTerminationSeconds = 10
        // Burst load: lazy core-thread creation lets thread 1 win all fast tasks, so eagerly
        // start every core thread at startup to spread work across the whole pool immediately.
        executor.prestartAllCoreThreads = true
        if (rejectionHandler != null) {
            executor.rejectedExecutionHandler = rejectionHandler
        }
        return executor
    }

    /**
     * A rejection handler that applies backpressure by blocking the submitting thread until a queue
     * slot frees up, rather than rejecting immediately (AbortPolicy) or running the task on the
     * caller thread (CallerRunsPolicy). If no slot frees within {@code submitTimeoutSeconds} the task
     * is rejected with a {@link RejectedExecutionException} so the caller surfaces a bounded 503
     * instead of blocking forever.
     */
    static final class CallerBlocksPolicy implements RejectedExecutionHandler {
        private final long submitTimeoutSeconds

        CallerBlocksPolicy(long submitTimeoutSeconds) {
            if (submitTimeoutSeconds < 0) {
                throw new IllegalArgumentException("submitTimeoutSeconds must be >= 0, was ${submitTimeoutSeconds}")
            }
            this.submitTimeoutSeconds = submitTimeoutSeconds
        }

        @Override
        void rejectedExecution(Runnable task, ThreadPoolExecutor pool) {
            if (pool.isShutdown()) {
                throw new RejectedExecutionException("Derivative loader is shut down, cannot accept task")
            }
            boolean accepted
            try {
                accepted = pool.queue.offer(task, submitTimeoutSeconds, TimeUnit.SECONDS)
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt()
                throw new RejectedExecutionException("Interrupted while waiting for a derivative loader queue slot", ie)
            }
            if (!accepted) {
                throw new RejectedExecutionException("Derivative loader queue still full after waiting ${submitTimeoutSeconds}s (queueSize=${pool.queue.size()} activeCount=${pool.activeCount} poolSize=${pool.poolSize} maximumPoolSize=${pool.maximumPoolSize})")
            }
        }
    }
}
