package au.org.ala.images.storage

import spock.lang.Specification

import java.time.Duration
import java.util.function.LongSupplier

class S3ApplicationStreamWatchdogSpec extends Specification {

    def 'positive progress renews the only scheduled timeout and timeout action wins once'() {
        given:
        def clock = new FakeClock()
        def scheduler = new FakeScheduler(clock)
        int timeouts = 0
        def watchdog = new S3ApplicationStreamWatchdog(Duration.ofSeconds(5), clock, scheduler, { timeouts++ } as Runnable)

        when:
        clock.advanceSeconds(4)
        watchdog.progressed(3)
        clock.advanceSeconds(1)
        scheduler.runDueTasks()

        then:
        watchdog.state == S3ApplicationStreamWatchdogState.ACTIVE
        timeouts == 0

        when:
        clock.advanceSeconds(4)
        scheduler.runDueTasks()
        watchdog.completed()
        scheduler.runDueTasks()

        then:
        watchdog.state == S3ApplicationStreamWatchdogState.TIMED_OUT
        timeouts == 1
    }

    def 'zero progress does not renew and close wins over a queued timeout'() {
        given:
        def clock = new FakeClock()
        def scheduler = new FakeScheduler(clock)
        int timeouts = 0
        def watchdog = new S3ApplicationStreamWatchdog(Duration.ofSeconds(5), clock, scheduler, { timeouts++ } as Runnable)

        when:
        watchdog.progressed(0)
        watchdog.closed()
        clock.advanceSeconds(5)
        scheduler.runDueTasks()

        then:
        watchdog.state == S3ApplicationStreamWatchdogState.CLOSED
        timeouts == 0
    }

    def 'stale expiry callback cannot replace the progress deadline or leak a task after close'() {
        given:
        def clock = new FakeClock()
        def scheduler = new FakeScheduler(clock)
        def watchdog = new S3ApplicationStreamWatchdog(Duration.ofSeconds(5), clock, scheduler, {} as Runnable)
        def staleExpiry = scheduler.lastScheduledTask

        when: 'progress replaces the deadline while the cancelled expiry callback is already executing'
        watchdog.progressed(1)
        staleExpiry.runnable.run()

        then: 'the stale generation does not schedule a replacement'
        scheduler.activeTaskCount == 1

        when:
        watchdog.closed()

        then: 'the terminal state cancels the sole tracked task'
        watchdog.state == S3ApplicationStreamWatchdogState.CLOSED
        scheduler.activeTaskCount == 0
    }

    private static class FakeClock implements LongSupplier {
        long nanos

        @Override
        long getAsLong() { nanos }

        void advanceSeconds(long seconds) { nanos += Duration.ofSeconds(seconds).toNanos() }
    }

    private static class FakeScheduler implements S3ApplicationStreamWatchdogScheduler {
        private final FakeClock clock
        private final List<FakeTask> tasks = []

        FakeScheduler(FakeClock clock) { this.clock = clock }

        @Override
        S3ApplicationStreamWatchdogTask schedule(Runnable runnable, long delayNanos) {
            def task = new FakeTask(runnable, clock.nanos + delayNanos)
            tasks << task
            return task
        }

        void runDueTasks() {
            tasks.findAll { !it.cancelled && it.deadlineNanos <= clock.nanos }.each { task ->
                task.cancelled = true
                task.runnable.run()
            }
        }

        FakeTask getLastScheduledTask() { tasks.last() }

        int getActiveTaskCount() { tasks.count { !it.cancelled } }

        private static class FakeTask implements S3ApplicationStreamWatchdogTask {
            final Runnable runnable
            final long deadlineNanos
            boolean cancelled

            FakeTask(Runnable runnable, long deadlineNanos) {
                this.runnable = runnable
                this.deadlineNanos = deadlineNanos
            }

            @Override
            void cancel() { cancelled = true }
        }
    }
}
