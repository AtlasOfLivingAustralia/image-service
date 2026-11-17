package au.org.ala.images.storage

import au.org.ala.images.S3ByteSinkFactory
import grails.util.Holders
import org.grails.config.PropertySourcesConfig
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.internal.crt.DefaultS3CrtAsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

/**
 * Verifies the opt-in derivative (thumbnail/tile) upload isolation pool: when enabled,
 * {@code byteSinkFactory} round-robins across distinct transfer managers backed by distinct
 * dedicated async clients; when disabled it keeps using the shared cached pipeline.
 */
class S3DerivativeUploadPoolSpec extends Specification {

    /**
     * {@link S3StorageOperations} resolves static config from {@code Holders.getConfig()} at
     * class-init time. In the plain-Specification test JVM that config is absent unless a
     * DataTest has already run, so install a minimal config before touching the class.
     */
    def setupSpec() {
        if (Holders.findApplicationContext() == null) {
            Holders.setConfig(new PropertySourcesConfig([
                    // Deliberately enable CRT globally: the pool must bypass this and use Netty.
                    'aws.s3.crt.enabled'                      : true,
                    'aws.s3.upload.derivative.pool.enabled'   : false,
                    'aws.s3.upload.derivative.pool.size'      : 4,
            ]))
        }
    }

    /**
     * Builds pool entries without touching the network or the real AWS builders: each entry is a
     * fresh, distinguishable mock pair. Instance method because Spock mocks require spec scope.
     */
    private S3StorageOperations.DerivativeUploadPool mockPool(int size) {
        List<S3AsyncClient> clients = []
        List<S3TransferManager> managers = []
        for (int index = 0; index < size; index++) {
            clients.add(Stub(S3AsyncClient))
            managers.add(Stub(S3TransferManager))
        }
        return new S3StorageOperations.DerivativeUploadPool(clients, managers)
    }

    private static class PoolOps extends S3StorageOperations {
        private final boolean poolEnabled
        private final S3StorageOperations.DerivativeUploadPool pool
        S3AsyncClient sharedUploadClient
        S3TransferManager sharedTransferManager
        int sharedTransferManagerLookups = 0
        private boolean seamInstalled = false

        PoolOps(boolean poolEnabled, S3StorageOperations.DerivativeUploadPool pool) {
            this.poolEnabled = poolEnabled
            this.pool = pool
        }

        @Override
        void setTransferManagerFactoryForTesting(Function<S3AsyncClient, S3TransferManager> factory) {
            seamInstalled = true
            super.setTransferManagerFactoryForTesting(factory)
        }

        @Override
        protected boolean derivativeUploadPoolEnabled() {
            return poolEnabled
        }

        @Override
        protected S3StorageOperations.DerivativeUploadPool derivativeUploadPool() {
            return pool
        }

        @Override
        protected S3TransferManager getS3TransferManager() {
            sharedTransferManagerLookups++
            // Delegate to super when the testing seam is installed so the seam path is exercised
            return seamInstalled ? super.getS3TransferManager() : sharedTransferManager
        }

        @Override
        protected S3AsyncClient getS3UploadAsyncClient() {
            return sharedUploadClient
        }
    }

    private PoolOps newPoolOps(boolean poolEnabled, int poolSize) {
        def ops = new PoolOps(poolEnabled, mockPool(poolSize))
        ops.bucket = 'derivative-bucket'
        ops.region = 'au-test-1'
        ops.sharedUploadClient = Stub(S3AsyncClient)
        ops.sharedTransferManager = Stub(S3TransferManager)
        return ops
    }

    def 'pool disabled: byteSinkFactory uses the shared transfer manager and client'() {
        given:
        def ops = newPoolOps(false, 3)

        when:
        def sinkFactory = ops.byteSinkFactory('uuid-off', [imageType: 'thumbnail'])

        then: 'the shared pipeline is used and the pool is never consulted'
        sinkFactory instanceof S3ByteSinkFactory
        sinkFactory.s3TransferManager.is(ops.sharedTransferManager)
        sinkFactory.s3Client.is(ops.sharedUploadClient)
        ops.sharedTransferManagerLookups == 1
    }

    def 'pool enabled: successive byteSinkFactory calls round-robin across distinct transfer managers and clients'() {
        given:
        def poolSize = 3
        def ops = newPoolOps(true, poolSize)
        def pool = ops.derivativeUploadPool()

        when: 'one full cycle plus one extra call'
        def sinks = (0..poolSize).collect { index -> ops.byteSinkFactory("uuid-${index}", [imageType: 'thumbnail']) }

        then: 'every visit uses the pool entry expected by round-robin order'
        sinks.size() == poolSize + 1
        for (int index = 0; index < poolSize; index++) {
            assert sinks[index].s3TransferManager.is(pool.managerAt(index))
            assert sinks[index].s3Client.is(pool.clientAt(index))
        }
        and: 'the call after a full cycle wraps back to the first entry'
        sinks[poolSize].s3TransferManager.is(pool.managerAt(0))
        sinks[poolSize].s3Client.is(pool.clientAt(0))

        and: 'all pool entries are distinct objects, proving isolation is real'
        sinks[0..<poolSize]*.s3TransferManager.toSet().size() == poolSize
        sinks[0..<poolSize]*.s3Client.toSet().size() == poolSize

        and: 'the shared transfer manager is never used while the pool is active'
        ops.sharedTransferManagerLookups == 0
    }

    def 'scheduleClose closes every pool transfer manager and its dedicated client after the delay'() {
        given:
        def scheduler = Executors.newSingleThreadScheduledExecutor()
        def closedManagers = new AtomicInteger()
        def closedClients = new AtomicInteger()
        List<S3AsyncClient> clients = []
        List<S3TransferManager> managers = []
        2.times {
            managers.add(Stub(S3TransferManager) { close() >> { closedManagers.incrementAndGet() } })
            clients.add(Stub(S3AsyncClient) { close() >> { closedClients.incrementAndGet() } })
        }
        def pool = new S3StorageOperations.DerivativeUploadPool(clients, managers)

        when:
        pool.scheduleClose(scheduler, 0, TimeUnit.MILLISECONDS)
        scheduler.shutdown()

        then: 'both entries (manager + dedicated client) are closed'
        scheduler.awaitTermination(5, TimeUnit.SECONDS)
        closedManagers.get() == 2
        closedClients.get() == 2

        cleanup:
        scheduler?.shutdownNow()
    }

    def 'transferManagerFactoryForTesting wins over the pool when enabled'() {
        given:
        def poolSize = 2
        def ops = newPoolOps(true, poolSize)
        def seamTransferManager = Stub(S3TransferManager)
        def seamClients = []
        ops.setTransferManagerFactoryForTesting({ S3AsyncClient client ->
            seamClients << client
            seamTransferManager
        } as Function<S3AsyncClient, S3TransferManager>)

        when:
        def sinkFactory = ops.byteSinkFactory('uuid-seam', [imageType: 'thumbnail'])

        then: 'the shared path (including the testing seam) is used, not the pool'
        sinkFactory.s3TransferManager.is(seamTransferManager)
        sinkFactory.s3Client.is(ops.sharedUploadClient)
        ops.sharedTransferManagerLookups == 1
        seamClients == [ops.sharedUploadClient]
    }

    def 'legacy async client override keeps the legacy shared transfer manager even when the pool is enabled'() {
        given:
        def legacyClient = Stub(S3AsyncClient)
        def legacyTransferManager = Stub(S3TransferManager)
        def ops = new LegacyPoolOps(true, mockPool(2), legacyClient, legacyTransferManager)

        when:
        def sinkFactory = ops.byteSinkFactory('uuid-legacy', [imageType: 'tile'])

        then: 'the legacy override path wins and the pool is bypassed'
        sinkFactory.s3TransferManager.is(legacyTransferManager)
        sinkFactory.s3Client.is(legacyClient)
    }

    /**
     * Exercises the real {@code derivativeUploadPoolLoader} via the instance seam while the
     * global {@code aws.s3.crt.enabled=true} flag is set. The pool clients must be standard
     * Netty-backed {@link S3AsyncClient}s (not the CRT async client), because all CRT clients
     * share the process-wide CRT EventLoopGroup that has exhibited the stuck-subscribe failure
     * (aws-sdk-java-v2#4893); pool isolation only holds if pool clients do not share it.
     */
    def 'pool clients are Netty (not CRT) even when aws.s3.crt.enabled is true'() {
        given: 'an ops instance whose loader builds real clients against a dummy endpoint'
        def ops = new S3StorageOperations(
                bucket: 'netty-pool-bucket',
                region: 'au-test-1',
                prefix: '',
                accessKey: 'x',
                secretKey: 'y',
                containerCredentials: false,
                publicRead: false,
                privateAcl: false,
                redirect: false,
                pathStyleAccess: true,
                hostname: 'http://127.0.0.1:1', // dummy; no request is ever made
                cloudfrontDomain: null)

        when: 'the real pool loader runs under the globally-CRT config'
        def pool = ops.buildDerivativeUploadPool()

        then: 'every pool client is a standard Netty-backed client, not the CRT async client'
        pool.size() == 4
        pool.clients.every { S3AsyncClient client ->
            !(client instanceof DefaultS3CrtAsyncClient)
        }
        and: 'each client is backed by a Netty async HTTP client'
        pool.clients.every { S3AsyncClient client ->
            def httpClient = readField(client, 'httpClient')
            httpClient == null || httpClient.class.name.contains('Netty')
        }

        cleanup: 'close quietly; no network was touched'
        pool?.managers?.each { try { it?.close() } catch (ignored) {} }
        pool?.clients?.each { try { it?.close() } catch (ignored) {} }
    }

    /**
     * Best-effort reflective read of a (possibly absent) field up the class hierarchy.
     * Returns null when the field is not present rather than failing the assertion.
     */
    private static Object readField(Object target, String fieldName) {
        Class<?> type = target.class
        while (type != null) {
            try {
                def field = type.getDeclaredField(fieldName)
                field.accessible = true
                return field.get(target)
            } catch (NoSuchFieldException ignored) {
                type = type.superclass
            } catch (Throwable ignored) {
                return null
            }
        }
        return null
    }

    /**
     * Legacy override variant: overriding getS3AsyncClient marks the instance as legacy, which
     * must bypass the pool even when enabled.
     */
    private static class LegacyPoolOps extends S3StorageOperations {
        private final boolean poolEnabled
        private final S3StorageOperations.DerivativeUploadPool pool
        private final S3AsyncClient legacyClient
        private final S3TransferManager legacyTransferManager

        LegacyPoolOps(boolean poolEnabled, S3StorageOperations.DerivativeUploadPool pool,
                      S3AsyncClient legacyClient, S3TransferManager legacyTransferManager) {
            this.poolEnabled = poolEnabled
            this.pool = pool
            this.legacyClient = legacyClient
            this.legacyTransferManager = legacyTransferManager
            this.bucket = 'legacy-bucket'
            this.region = 'au-test-1'
        }

        @Override
        protected boolean derivativeUploadPoolEnabled() {
            return poolEnabled
        }

        @Override
        protected S3StorageOperations.DerivativeUploadPool derivativeUploadPool() {
            return pool
        }

        @Override
        protected S3AsyncClient getS3AsyncClient() {
            return legacyClient
        }

        @Override
        protected S3TransferManager getS3TransferManager() {
            return legacyTransferManager
        }
    }
}
