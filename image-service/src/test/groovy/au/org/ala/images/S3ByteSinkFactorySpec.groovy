package au.org.ala.images

import au.org.ala.images.storage.S3ApplicationWatchdogOutputStream
import au.org.ala.images.storage.S3ApplicationStreamIdleTimeoutException
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class S3ByteSinkFactorySpec extends Specification {

    def 'SDK cancellable output stream unblocks a blocked write after the idle timeout'() {
        given:
        def delegate = new CancellableBlockedOutputStream()
        def stream = new S3ApplicationWatchdogOutputStream(delegate, Duration.ofMillis(50), 'upload')
        def result = new AtomicReference<Throwable>()
        def writer = Thread.start {
            try {
                stream.write(1)
            } catch (Throwable error) {
                result.set(error)
            }
        }

        expect: 'the named write boundary is blocked before cancellation'
        delegate.writeStarted.await(5, TimeUnit.SECONDS)

        when:
        writer.join(5_000)

        then:
        !writer.alive
        delegate.cancelled.await(5, TimeUnit.SECONDS)
        stream.timedOut
        result.get() instanceof S3ApplicationStreamIdleTimeoutException

        cleanup:
        stream.close()
    }

    private static class CancellableBlockedOutputStream extends software.amazon.awssdk.utils.CancellableOutputStream {
        final CountDownLatch writeStarted = new CountDownLatch(1)
        final CountDownLatch cancelled = new CountDownLatch(1)

        @Override
        void write(int value) throws IOException {
            writeStarted.countDown()
            cancelled.await()
            throw new IOException('cancelled')
        }

        @Override
        void cancel() {
            cancelled.countDown()
        }
    }
}
