package au.org.ala.images

import au.org.ala.images.storage.S3ApplicationWatchdogOutputStream
import au.org.ala.images.storage.S3ApplicationStreamIdleTimeoutException
import au.org.ala.images.storage.S3UploadCompletionTimeoutException
import au.org.ala.images.util.AbortableOutputStream
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.Upload
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class S3ByteSinkFactorySpec extends Specification {

    private static final String BUCKET = 'test-bucket'
    private static final String REGION = 'au-test-1'

    def 'stuck subscription is rethrown unchanged and records diagnostics + counter'() {
        given: 'a transfer manager that never subscribes the blocking body and never completes'
        def registry = new SimpleMeterRegistry()
        def s3Client = Mock(S3AsyncClient)
        def neverCompleting = new CompletableFuture<>()
        def transferManager = neverSubscribingTransferManager(neverCompleting)
        def factory = newFactory(registry, s3Client, transferManager, 1)

        when: 'the stream is opened against the stuck transfer manager'
        factory.getByteSinkForNames('thumbnail').openStream()

        then: 'the SDK stuck-subscription IllegalStateException propagates unchanged'
        def error = thrown(IllegalStateException)
        error.message.contains('was not made within')

        and: 'a subscribe-timeout counter tagged by bucket and region was recorded'
        def counter = registry.find('s3.upload.subscribe.timeout')
                .tags('bucket', BUCKET, 'region', REGION)
                .counter()
        counter != null
        counter.count() == 1.0

        and: 'a streaming start counter was recorded but no completion'
        registry.find('s3.upload.streaming.start')
                .tags('bucket', BUCKET, 'region', REGION).counter()?.count() == 1.0
        registry.find('s3.upload.streaming.completion')
                .tags('bucket', BUCKET, 'region', REGION).counter() == null
    }

    def 'stuck subscription propagates when no meter registry is present'() {
        given: 'no MeterRegistry (metrics disabled path)'
        def s3Client = Mock(S3AsyncClient)
        def transferManager = neverSubscribingTransferManager(new CompletableFuture<>())
        def factory = newFactory(null, s3Client, transferManager, 1)

        when:
        factory.getByteSinkForNames('thumbnail').openStream()

        then: 'the exception still propagates unchanged with no metrics side effects'
        def error = thrown(IllegalStateException)
        error.message.contains('was not made within')
    }

    def 'isStuckSubscription only matches the SDK subscribe-timeout message'() {
        expect:
        S3ByteSinkFactory.isStuckSubscription(
                new IllegalStateException('The service request was not made within 1 seconds of outputStream being invoked'))
        !S3ByteSinkFactory.isStuckSubscription(new IllegalStateException('boom-unrelated'))
        !S3ByteSinkFactory.isStuckSubscription(new IllegalStateException((String) null))
        !S3ByteSinkFactory.isStuckSubscription(null)
    }

    def 'stuck subscription cancels the orphaned upload so it cannot hold client capacity'() {
        given:
        def neverCompleting = new CompletableFuture<>()
        def factory = newFactory(null, Mock(S3AsyncClient), neverSubscribingTransferManager(neverCompleting), 1)

        when:
        factory.getByteSinkForNames('thumbnail').openStream()

        then:
        thrown(IllegalStateException)
        neverCompleting.isCancelled()
    }

    def 'close is bounded when the upload never completes and the upload is cancelled'() {
        given: 'a transfer manager that consumes the body but never completes the upload'
        def registry = new SimpleMeterRegistry()
        def neverCompleting = new CompletableFuture<>()
        def factory = newFactory(registry, Mock(S3AsyncClient), drainingTransferManager(neverCompleting), 1)
        factory.completionTimeout = Duration.ofMillis(200)
        def stream = factory.getByteSinkForNames('thumbnail').openStream()
        stream.write(new byte[16])

        when:
        long start = System.nanoTime()
        stream.close()

        then:
        def error = thrown(IOException)
        error.cause instanceof S3UploadCompletionTimeoutException
        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 5
        neverCompleting.isCancelled()
        registry.find('s3.upload.completion.timeout').tags('bucket', BUCKET, 'region', REGION).counter()?.count() == 1.0
    }

    def 'close surfaces the upload failure cause'() {
        given:
        def failed = new CompletableFuture<>()
        def factory = newFactory(null, Mock(S3AsyncClient), drainingTransferManager(failed), 1)
        def stream = factory.getByteSinkForNames('thumbnail').openStream()
        failed.completeExceptionally(new IllegalArgumentException('boom'))

        when:
        stream.close()

        then:
        def error = thrown(IOException)
        error.message == 'S3 upload failed'
        error.cause.cause instanceof IllegalArgumentException
    }

    def 'abort cancels the upload, and close afterwards neither commits nor blocks'() {
        given:
        def registry = new SimpleMeterRegistry()
        def pending = new CompletableFuture<>()
        def factory = newFactory(registry, Mock(S3AsyncClient), drainingTransferManager(pending), 1)
        def stream = factory.getByteSinkForNames('thumbnail').openStream()
        stream.write(new byte[16])

        when:
        AbortableOutputStream.abortQuietly(stream, new IOException('encoder failed'))
        stream.close()

        then:
        stream instanceof AbortableOutputStream
        pending.isCancelled()
        registry.find('s3.upload.streaming.aborted').counter()?.count() == 1.0
        registry.find('s3.upload.streaming.completion').counter() == null
        registry.find('s3.upload.success').counter() == null

        when:
        stream.write(1)

        then:
        thrown(IOException)
    }

    // ---- helpers ----

    /** A transfer manager that subscribes and drains the request body, completing via {@code future}. */
    private S3TransferManager drainingTransferManager(CompletableFuture future) {
        def upload = Mock(Upload)
        upload.completionFuture() >> future
        def transferManager = Mock(S3TransferManager)
        transferManager.upload(_ as UploadRequest) >> { UploadRequest request ->
            request.requestBody().subscribe(new Subscriber<ByteBuffer>() {
                @Override void onSubscribe(Subscription subscription) { subscription.request(Long.MAX_VALUE) }
                @Override void onNext(ByteBuffer item) {}
                @Override void onError(Throwable throwable) {}
                @Override void onComplete() {}
            })
            upload
        }
        return transferManager
    }
    private S3ByteSinkFactory newFactory(io.micrometer.core.instrument.MeterRegistry registry,
                                         S3AsyncClient s3Client,
                                         S3TransferManager transferManager,
                                         int connectionTimeoutSeconds) {
        def pathStrategy = new DefaultStoragePathStrategy([], true, false)
        return new S3ByteSinkFactory(s3Client, transferManager, connectionTimeoutSeconds,
                pathStrategy, BUCKET, 'uuid-1234', [:], registry, REGION)
    }

    /**
     * A transfer manager that accepts the upload but NEVER subscribes the request body and
     * returns a future that never completes. This reproduces the production stuck state where
     * BlockingOutputStreamAsyncRequestBody.outputStream() times out waiting for a subscription.
     */
    private S3TransferManager neverSubscribingTransferManager(CompletableFuture future) {
        def upload = Mock(Upload)
        upload.completionFuture() >> future
        def transferManager = Mock(S3TransferManager)
        // deliberately do NOT subscribe req.requestBody(); returning the upload leaves the
        // BlockingOutputStreamAsyncRequestBody waiting for a subscriber that never arrives.
        transferManager.upload(_ as UploadRequest) >> upload
        return transferManager
    }

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
