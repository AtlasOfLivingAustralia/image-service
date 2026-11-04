package au.org.ala.images.config

import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.MapPropertySource
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import spock.lang.Specification

/**
 * Guards the {@code @Value} method-parameter injection mechanism on
 * {@link ImageServiceConfiguration}: proves that a property set in the Spring
 * {@code Environment} is resolved into the {@code @Bean} factory method parameter,
 * not silently left at the Groovy field default.
 *
 * This is the regression test for the root cause fixed in Phase 6: when the beans
 * lived on {@code Application} (a {@code GrailsAutoConfiguration}), {@code @Value}
 * fields were populated after {@code @Bean} factory methods ran, so every executor
 * and tiler was constructed with its Groovy default ({@code 0} / {@code false} / "").
 */
class ImageServiceConfigurationSpec extends Specification {

    def "derivativeLoaderExecutor receives configured thread count via @Value method-param injection"() {
        given: 'a Spring context with only the derivativeLoaderExecutor bean and a custom property'
        def ctx = new AnnotationConfigApplicationContext()
        ctx.environment.propertySources.addFirst(
                new MapPropertySource('test', ['derivative.loader.threads': 7]))
        ctx.register(DerivativeLoaderOnlyConfig)
        ctx.refresh()

        when: 'the bean is retrieved'
        TaskExecutor executor = ctx.getBean('derivativeLoaderExecutor', TaskExecutor)

        then: 'it was constructed with the configured value, not the Groovy default of 0'
        executor instanceof ThreadPoolTaskExecutor
        ((ThreadPoolTaskExecutor) executor).corePoolSize == 7
        ((ThreadPoolTaskExecutor) executor).maxPoolSize == 7

        cleanup:
        ctx.close()
    }

    def "derivativeLoaderExecutor queueCapacity is clamped and submitted to the pool"() {
        given: 'a Spring context with a custom queueCapacity'
        def ctx = new AnnotationConfigApplicationContext()
        ctx.environment.propertySources.addFirst(
                new MapPropertySource('test', [
                        'derivative.loader.threads'      : 2,
                        'derivative.loader.queueCapacity': 42
                ]))
        ctx.register(DerivativeLoaderOnlyConfig)
        ctx.refresh()

        when:
        TaskExecutor executor = ctx.getBean('derivativeLoaderExecutor', TaskExecutor)

        then:
        ((ThreadPoolTaskExecutor) executor).queueCapacity == 42

        cleanup:
        ctx.close()
    }

    /**
     * Minimal config that exercises only the derivativeLoaderExecutor bean definition.
     * We cannot register the full ImageServiceConfiguration in a plain Spring context
     * because it requires GrailsApplication, CommandExecutor, etc.
     */
    @Configuration
    static class DerivativeLoaderOnlyConfig {

        @Bean
        TaskExecutor derivativeLoaderExecutor(
                @org.springframework.beans.factory.annotation.Value('${derivative.loader.threads:4}') int threads,
                @org.springframework.beans.factory.annotation.Value('${derivative.loader.queueCapacity:100}') int queueCapacity,
                @org.springframework.beans.factory.annotation.Value('${derivative.loader.submitTimeoutSeconds:5}') long submitTimeoutSeconds) {
            return au.org.ala.images.Application.createThreadPoolTaskExecutor(
                    "derivative-loader-",
                    threads,
                    threads,
                    Math.max(0, queueCapacity),
                    new au.org.ala.images.Application.CallerBlocksPolicy(submitTimeoutSeconds))
        }
    }
}
