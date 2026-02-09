package au.org.ala.images.metrics

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.beans.factory.annotation.Autowired

/**
 * Trait to provide Micrometer metrics support for services and controllers
 */
@CompileStatic
@Slf4j
trait MetricsSupport {

    @Autowired(required = false)
    MeterRegistry meterRegistry

    private Map<String, Timer> timers = [:]
    private Map<String, Counter> counters = [:]

    /**
     * Get or create a timer metric
     */
    Timer getTimer(String name, String description = null, Map<String, String> tags = [:]) {
        if (!meterRegistry) return null

        String key = "${name}:${tags.toString()}"
        if (!timers.containsKey(key)) {
            def builder = Timer.builder(name)
            if (description) {
                builder.description(description)
            }
            tags.each { k, v ->
                builder.tag(k, v)
            }
            timers[key] = builder.register(meterRegistry)
        }
        return timers[key]
    }

    /**
     * Get or create a counter metric
     */
    Counter getCounter(String name, String description = null, Map<String, String> tags = [:]) {
        if (!meterRegistry) return null

        String key = "${name}:${tags.toString()}"
        if (!counters.containsKey(key)) {
            def builder = Counter.builder(name)
            if (description) {
                builder.description(description)
            }
            tags.each { k, v ->
                builder.tag(k, v)
            }
            counters[key] = builder.register(meterRegistry)
        }
        return counters[key]
    }

    /**
     * Record a timed operation
     */
    def <T> T recordTime(String name, String description = null, Map<String, String> tags = [:], Closure<T> operation) {
        Timer timer = getTimer(name, description, tags)
        if (timer) {
            return timer.recordCallable(operation)
        } else {
            return operation.call()
        }
    }

    /**
     * Increment a counter
     */
    void incrementCounter(String name, String description = null, Map<String, String> tags = [:], double amount = 1.0) {
        Counter counter = getCounter(name, description, tags)
        counter?.increment(amount)
    }

    /**
     * Record a successful operation
     */
    void recordSuccess(String operation, Map<String, String> additionalTags = [:]) {
        incrementCounter("${getMetricPrefix()}.success", "Successful operations", additionalTags + [operation: operation])
    }

    /**
     * Record a failed operation
     */
    void recordError(String operation, Map<String, String> additionalTags = [:]) {
        incrementCounter("${getMetricPrefix()}.errors", "Failed operations", additionalTags + [operation: operation])
    }

    /**
     * Get the metric prefix for this service/controller
     * Override in implementing classes to customize
     */
    String getMetricPrefix() {
        String className = this.class.simpleName
        if (className.endsWith('Service')) {
            return "service.${className.replaceAll('Service$', '').toLowerCase()}"
        } else if (className.endsWith('Controller')) {
            return "controller.${className.replaceAll('Controller$', '').toLowerCase()}"
        }
        return className.toLowerCase()
    }
}

