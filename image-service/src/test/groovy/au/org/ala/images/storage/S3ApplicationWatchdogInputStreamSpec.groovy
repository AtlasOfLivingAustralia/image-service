package au.org.ala.images.storage

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.SdkResponse
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.async.SdkPublisher
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class S3ApplicationWatchdogInputStreamSpec extends Specification {

    def 'blocking async transformer read is aborted once and surfaces the application idle timeout'() {
        given:
        def transformer = AsyncResponseTransformer.<SdkResponse>toBlockingInputStream()
        def response = Stub(SdkResponse)
        def subscription = new BlockingSubscription()
        def publisher = new SingleSubscriptionPublisher(subscription)
        def responseFuture = transformer.prepare()
        transformer.onResponse(response)
        transformer.onStream(publisher)
        ResponseInputStream<SdkResponse> responseStream = responseFuture.get(5, TimeUnit.SECONDS)
        def stream = new S3ApplicationWatchdogInputStream<SdkResponse>(responseStream, Duration.ofMillis(50), 'test object')
        def readFailure = new AtomicReference<Throwable>()
        def reader = Thread.start {
            try {
                stream.read()
            } catch (Throwable error) {
                readFailure.set(error)
            }
        }

        expect: 'the named read boundary is waiting on the SDK blocking transformer'
        subscription.requested.await(5, TimeUnit.SECONDS)

        when:
        reader.join(5_000)

        then: 'the watchdog abort unblocks within five seconds and maps the winner to the application timeout'
        !reader.alive
        subscription.cancelled.await(5, TimeUnit.SECONDS)
        subscription.cancelCount == 1
        stream.timedOut
        readFailure.get() instanceof S3ApplicationStreamIdleTimeoutException

        when: 'terminal cleanup is repeated after the timeout'
        stream.close()

        then: 'no second abort is scheduled'
        subscription.cancelCount == 1
    }

    private static class SingleSubscriptionPublisher implements SdkPublisher<ByteBuffer> {
        private final Subscription subscription

        SingleSubscriptionPublisher(Subscription subscription) { this.subscription = subscription }

        @Override
        void subscribe(Subscriber<? super ByteBuffer> subscriber) { subscriber.onSubscribe(subscription) }
    }

    private static class BlockingSubscription implements Subscription {
        final CountDownLatch requested = new CountDownLatch(1)
        final CountDownLatch cancelled = new CountDownLatch(1)
        volatile int cancelCount

        @Override
        void request(long count) { requested.countDown() }

        @Override
        void cancel() {
            cancelCount++
            cancelled.countDown()
        }
    }
}
