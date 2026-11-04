package au.org.ala.images.storage.s3local

import au.org.ala.images.storage.S3StorageOperations
import au.org.ala.images.util.AbortableOutputStream
import grails.util.Holders
import groovy.util.logging.Slf4j
import org.grails.config.PropertySourcesConfig
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification

import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

/**
 * Fault-injection tests for S3 streaming uploads (guards against the aws-sdk-java-v2#4893 hangs) against a local S3
 * behind Toxiproxy. Each feature asserts the HEALTHY behaviour, so a failing feature indicates a
 * regression. Thread dumps for failures/hangs are written to build/s3-local/.
 *
 * Run with: ./gradlew :image-service:s3FaultTest  (see s3-local/README.md for options).
 * S3StorageOperations reads its config once per JVM, so client settings (CRT vs Netty etc.)
 * are chosen per Gradle invocation via -P/-D au.org.ala.images.s3.* properties.
 */
@Slf4j
@Requires({ Boolean.getBoolean('s3local.enabled') })
class S3UploadFaultSpec extends Specification {

    static final int SUBSCRIBE_TIMEOUT = Integer.getInteger('au.org.ala.images.s3.timeouts.call', 15)
    static final int EVICTION_DELAY = Integer.getInteger('au.org.ala.images.s3.timeouts.eviction', 2 * SUBSCRIBE_TIMEOUT)
    static final int HANG_SECONDS = Integer.getInteger('s3local.hang.seconds', 90)
    static final boolean CRT = Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.crt.enabled', 'true'))
    static final int CRT_MAX_CONCURRENCY = Integer.getInteger('au.org.ala.images.s3.crt.max-concurrency', 32)

    @Shared S3LocalEnvironment env
    @Shared S3Client direct
    @Shared ExecutorService workers
    @Shared Path dumpDir = Paths.get(System.getProperty('s3local.dumpDir', 'build/s3-local'))
    @Shared byte[] payload = new byte[256 * 1024].tap { new Random(42).nextBytes(it) }

    def setupSpec() {
        if (Holders.findApplicationContext() == null) {
            // Empty config: every S3StorageOperations setting falls back to its au.org.ala.images.s3.* system property.
            Holders.setConfig(new PropertySourcesConfig([:]))
        }
        Files.createDirectories(dumpDir)
        env = S3LocalEnvironment.start()
        direct = env.directClient()
        AtomicInteger n = new AtomicInteger()
        workers = Executors.newCachedThreadPool({ Runnable r -> new Thread(r, "s3local-worker-${n.incrementAndGet()}").tap { daemon = true } } as ThreadFactory)
        log.info('s3local settings: crt={} crtMaxConcurrency={} subscribeTimeout={}s eviction={}s hang={}s derivativePool={}',
                CRT, CRT_MAX_CONCURRENCY, SUBSCRIBE_TIMEOUT, EVICTION_DELAY, HANG_SECONDS,
                System.getProperty('au.org.ala.images.s3.upload.derivative.pool.enabled', 'false'))
    }

    def cleanupSpec() {
        workers?.shutdownNow()
        direct?.close()
        env?.close()
    }

    def cleanup() {
        env.reset()
    }

    def "baseline: concurrent derivative and ingest uploads succeed"() {
        given:
        def ops = newOps('baseline')

        when:
        def results = uploadMany(ops, 64, 'baseline')
        def ingest = runWithTimeout('ingest') { storeOriginal(ops, uuid('ingest')) }

        then:
        results.every { it.ok }
        ingest.ok
    }

    def "abandoned upload streams do not wedge the client after release"() {
        given: 'a producer that obtains streams but never writes or closes them (leaked on an error path)'
        def ops = newOps('abandoned')
        int abandonCount = Integer.getInteger('s3local.abandon.count', CRT ? CRT_MAX_CONCURRENCY + 4 : 64)
        List<OutputStream> abandoned = Collections.synchronizedList([])
        List<Throwable> openFailures = Collections.synchronizedList([])

        when:
        List<Future> opens = (1..abandonCount).collect { int i ->
            workers.submit({
                try {
                    abandoned << ops.thumbnailByteSinkFactory(uuid("abandon-$i")).getByteSinkForNames('thumbnail').openStream()
                } catch (Throwable t) {
                    openFailures << t
                }
            } as Runnable)
        }
        opens.each { it.get(SUBSCRIBE_TIMEOUT * 2 + 10, TimeUnit.SECONDS) }
        log.info('abandoned: opened={} failedToOpen={} firstFailure={}', abandoned.size(), openFailures.size(), openFailures ? openFailures[0].toString() : '')
        def whileHeld = runWithTimeout('fresh upload while streams abandoned') { uploadOne(ops, uuid('fresh-held')) }

        and: 'the leaked streams are eventually closed'
        List<Result> releases = abandoned.withIndex().collect { OutputStream os, int i ->
            runWithTimeout("close-abandoned-$i") { os.close() }
        }
        log.info('abandoned: release results {}', summarise(releases))
        def afterRelease = runWithTimeout('fresh upload after release') { uploadOne(ops, uuid('fresh-released')) }
        log.info('abandoned: whileHeld={} afterRelease={}', whileHeld, afterRelease)

        then: 'leaked streams genuinely hold client capacity (logged above), but the client must recover once they are released'
        afterRelease.ok
    }

    def "producer failure mid-write does not commit a truncated object"() {
        given:
        def ops = newOps('truncated')
        String id = uuid('truncated')

        when: 'the producer writes part of the image then fails, aborting the stream as the image-utils callers do'
        try {
            OutputStream os = ops.thumbnailByteSinkFactory(id).getByteSinkForNames('thumbnail').openStream()
            os.withCloseable {
                try {
                    os.write(payload, 0, 1024)
                    throw new IOException('simulated encoder failure')
                } catch (Throwable t) {
                    AbortableOutputStream.abortQuietly(os, t)
                    throw t
                }
            }
        } catch (IOException expected) {
            log.info('producer failure surfaced: {}', expected.toString())
        }
        Thread.sleep(1000) // let any (incorrect) commit land before checking
        Long storedLength = objectLength('truncated', ops.storagePathStrategy().createPathFromUUID(id, 'thumbnail'))
        log.info('object after producer failure: {}', storedLength == null ? 'absent' : "${storedLength} bytes")
        def after = runWithTimeout('upload after aborts') { uploadOne(ops, uuid('after-abort')) }

        then:
        storedLength == null
        after.ok
    }

    def "uploads recover after a blackholed (half-open) connection"() {
        expect:
        faultThenRecover('blackhole') { env.blackhole('upstream') } { env.removeToxic('blackhole_upstream') }
    }

    def "uploads recover after connections are dropped (proxy down/up)"() {
        expect:
        faultThenRecover('proxydown') { env.proxyEnabled(false) } { env.proxyEnabled(true) }
    }

    def "slow but healthy producer is not timed out"() {
        given: 'a producer that keeps writing steadily for longer than the SDK call timeouts (no network fault)'
        def ops = newOps('slowwriter')
        int seconds = Integer.getInteger('s3local.slow.seconds', SUBSCRIBE_TIMEOUT + 5)

        when:
        def derivative = runWithTimeout('slow derivative') { uploadOverSeconds(ops, uuid('slow-derivative'), seconds) }
        def ingest = runWithTimeout('slow ingest') {
            storeOriginalSlowly(ops, uuid('slow-ingest'), seconds)
        }
        log.info('slowwriter ({}s): derivative={} ingest={}', seconds, derivative, ingest)

        then:
        derivative.ok
        ingest.ok
    }

    def "slow multipart-sized upload is not timed out"() {
        given: 'a producer writing an object larger than the 8 MB part size slowly enough that each part takes longer than the call timeout'
        def ops = newOps('slowlarge')
        int megabytes = Integer.getInteger('s3local.large.mb', 20)
        int seconds = Integer.getInteger('s3local.large.seconds', SUBSCRIBE_TIMEOUT * 4)
        byte[] large = new byte[megabytes * 1024 * 1024].tap { new Random(7).nextBytes(it) }

        String id = uuid('slow-large')

        when:
        def derivative = runWithTimeout('slow large derivative') {
            ops.thumbnailByteSinkFactory(id).getByteSinkForNames('thumbnail').openStream().withCloseable { OutputStream os ->
                writeOverSeconds(os, large, seconds)
            }
        }
        def ingest = runWithTimeout('slow large ingest') {
            def pipeIn = new PipedInputStream(1024 * 1024)
            def pipeOut = new PipedOutputStream(pipeIn)
            def feeder = workers.submit({ pipeOut.withCloseable { writeOverSeconds(it, large, seconds) } } as Runnable)
            ops.storeAnywhere(uuid('slow-large-ingest'), pipeIn, 'original', 'image/jpeg', null, null)
            feeder.get(5, TimeUnit.SECONDS)
        }
        log.info('slowlarge ({} MB over {}s): derivative={} ingest={}', megabytes, seconds, derivative, ingest)

        then:
        derivative.ok
        ingest.ok
        objectLength('slowlarge', ops.storagePathStrategy().createPathFromUUID(id, 'thumbnail')) == (long) large.length
    }

    def "uploads recover after connection resets"() {
        expect:
        faultThenRecover('reset') { env.resetPeer(0) } { env.removeToxic('reset_peer') }
    }

    def "uploads still work after client cache eviction with DefaultCredentialsProvider"() {
        given: 'container credentials resolved via system properties (first link in the default chain)'
        System.setProperty('aws.accessKeyId', S3LocalEnvironment.ACCESS_KEY)
        System.setProperty('aws.secretAccessKey', S3LocalEnvironment.SECRET_KEY)
        def first = newOps('creds-a', true)
        def second = newOps('creds-b', true)

        when:
        def before = uploadMany(first, 8, 'creds-before') + uploadMany(second, 8, 'creds-before')
        S3StorageOperations.clearS3ClientCache()
        log.info('cleared S3 client caches; waiting {}s for delayed close of evicted clients', EVICTION_DELAY + 2)
        Thread.sleep((EVICTION_DELAY + 2) * 1000L)
        def after = uploadMany(first, 8, 'creds-after') + uploadMany(second, 8, 'creds-after')

        then:
        before.every { it.ok }
        after.every { it.ok }
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Starts slow in-flight uploads, applies a fault while they are writing, lifts it, then checks
     * that (a) the in-flight uploads terminate within HANG_SECONDS and (b) fresh uploads succeed.
     */
    private boolean faultThenRecover(String name, Closure applyFault, Closure liftFault) {
        def ops = newOps(name)
        assert uploadMany(ops, 8, "$name-warm").every { it.ok }: 'warm-up uploads failed before any fault'

        List<Future<Result>> inflight = (1..8).collect { int i ->
            workers.submit({ timed("$name-inflight-$i") { uploadSlowly(ops, uuid("$name-inflight-$i")) } } as Callable<Result>)
        }
        Thread.sleep(1000)
        applyFault()
        Thread.sleep(Long.getLong('s3local.fault.millis', 10_000L))
        liftFault()

        List<Result> inflightResults = inflight.withIndex().collect { Future<Result> f, int i -> await(f, "$name-inflight-$i") }
        List<Result> fresh = uploadMany(ops, 16, "$name-after")
        def ingest = runWithTimeout("$name-ingest-after") { storeOriginal(ops, uuid("$name-ingest-after")) }

        log.info('{}: inflight={} fresh={} ingest={}', name, summarise(inflightResults), summarise(fresh), ingest)
        boolean inflightTerminated = inflightResults.every { !it.hung }
        if (!inflightTerminated) dumpThreads("$name-inflight-hung")
        return inflightTerminated && fresh.every { it.ok } && ingest.ok
    }

    private S3StorageOperations newOps(String bucketSuffix, boolean containerCredentials = false) {
        String bucket = "fault-${bucketSuffix}"
        env.createBucket(bucket)
        new S3StorageOperations(
                bucket: bucket, region: S3LocalEnvironment.REGION, prefix: '',
                accessKey: containerCredentials ? null : S3LocalEnvironment.ACCESS_KEY,
                secretKey: containerCredentials ? null : S3LocalEnvironment.SECRET_KEY,
                containerCredentials: containerCredentials, publicRead: false, privateAcl: false, redirect: false,
                pathStyleAccess: true, hostname: env.proxiedEndpoint, cloudfrontDomain: '')
    }

    private void uploadOne(S3StorageOperations ops, String id) {
        ops.thumbnailByteSinkFactory(id).getByteSinkForNames('thumbnail').openStream().withCloseable { it.write(payload) }
    }

    /** Writes in small chunks with pauses so the upload is mid-body when a fault is injected. */
    private void uploadSlowly(S3StorageOperations ops, String id) {
        ops.thumbnailByteSinkFactory(id).getByteSinkForNames('thumbnail').openStream().withCloseable { OutputStream os ->
            for (int offset = 0; offset < payload.length; offset += 16 * 1024) {
                os.write(payload, offset, Math.min(16 * 1024, payload.length - offset))
                os.flush()
                Thread.sleep(250)
            }
        }
    }

    /** Writes the payload in even chunks spread over {@code seconds}, flushing each chunk. */
    private void uploadOverSeconds(S3StorageOperations ops, String id, int seconds) {
        ops.thumbnailByteSinkFactory(id).getByteSinkForNames('thumbnail').openStream().withCloseable { OutputStream os ->
            writeOverSeconds(os, seconds)
        }
    }

    /** Ingest from an InputStream that trickles the payload over {@code seconds}. */
    private void storeOriginalSlowly(S3StorageOperations ops, String id, int seconds) {
        def pipeIn = new PipedInputStream(payload.length)
        def pipeOut = new PipedOutputStream(pipeIn)
        def feeder = workers.submit({ pipeOut.withCloseable { writeOverSeconds(it, seconds) } } as Runnable)
        ops.storeAnywhere(id, pipeIn, 'original', 'image/jpeg', null, null)
        feeder.get(5, TimeUnit.SECONDS)
    }

    private void writeOverSeconds(OutputStream os, int seconds) {
        writeOverSeconds(os, payload, seconds)
    }

    private static void writeOverSeconds(OutputStream os, byte[] data, int seconds) {
        int chunks = seconds * 4
        int chunk = (int) Math.ceil(data.length / (double) chunks)
        for (int offset = 0; offset < data.length; offset += chunk) {
            os.write(data, offset, Math.min(chunk, data.length - offset))
            os.flush()
            Thread.sleep(250)
        }
    }

    private void storeOriginal(S3StorageOperations ops, String id) {
        ops.storeAnywhere(id, new ByteArrayInputStream(payload), 'original', 'image/jpeg', null, (long) payload.length)
    }

    private List<Result> uploadMany(S3StorageOperations ops, int count, String label) {
        List<Future<Result>> futures = (1..count).collect { int i ->
            workers.submit({ timed("$label-$i") { uploadOne(ops, uuid("$label-$i")) } } as Callable<Result>)
        }
        futures.withIndex().collect { Future<Result> f, int i -> await(f, "$label-$i") }
    }

    private Result runWithTimeout(String label, Closure body) {
        await(workers.submit({ timed(label, body) } as Callable<Result>), label)
    }

    private Result await(Future<Result> future, String label) {
        try {
            return future.get(HANG_SECONDS, TimeUnit.SECONDS)
        } catch (TimeoutException ignored) {
            log.warn('{} still running after {}s - treating as hung', label, HANG_SECONDS)
            dumpThreads("hung-$label")
            return new Result(label: label, hung: true, millis: HANG_SECONDS * 1000L)
        }
    }

    private static Result timed(String label, Closure body) {
        long start = System.nanoTime()
        try {
            body()
            return new Result(label: label, ok: true, millis: elapsed(start))
        } catch (Throwable t) {
            Throwable root = t
            while (root.cause != null && root.cause != root) root = root.cause
            log.info('{} failed after {}ms: {} (root: {})', label, elapsed(start), t.toString(), root.toString())
            return new Result(label: label, error: "${t.class.simpleName}: ${root}", millis: elapsed(start))
        }
    }

    private Long objectLength(String bucketSuffix, String key) {
        try {
            return direct.headObject { it.bucket("fault-$bucketSuffix").key(key) }.contentLength()
        } catch (NoSuchKeyException ignored) {
            return null
        } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
            if (e.statusCode() == 404) return null
            throw e
        }
    }

    private static String summarise(List<Result> results) {
        int ok = results.count { it.ok } as int
        int hung = results.count { it.hung } as int
        Map<String, Integer> errors = results.findAll { !it.ok && !it.hung }.countBy { it.error.take(160) }
        "ok=$ok hung=$hung failed=${results.size() - ok - hung} maxMs=${results*.millis.max()} errors=$errors"
    }

    private void dumpThreads(String label) {
        def file = dumpDir.resolve("threads-${label.replaceAll('[^A-Za-z0-9_.-]', '_')}-${System.currentTimeMillis()}.txt")
        def text = ManagementFactory.threadMXBean.dumpAllThreads(true, true).collect { info ->
            def sb = new StringBuilder("\"${info.threadName}\" ${info.threadState}")
            if (info.lockName) sb.append(" on ${info.lockName}")
            if (info.lockOwnerName) sb.append(" owned by \"${info.lockOwnerName}\"")
            sb.append('\n')
            info.stackTrace.each { sb.append("\tat ${it}\n") }
            sb.toString()
        }.join('\n')
        Files.writeString(file, text)
        log.warn('Thread dump written to {}', file.toAbsolutePath())
    }

    private static String uuid(String label) { "${label}-${UUID.randomUUID()}" }

    private static long elapsed(long startNanos) { TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos) }

    @groovy.transform.ToString(includeNames = true)
    static class Result {
        String label
        boolean ok
        boolean hung
        String error
        long millis
    }
}
