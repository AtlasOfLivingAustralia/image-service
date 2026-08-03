package au.org.ala.images

import au.org.ala.images.storage.S3StorageOperations
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadBucketResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import software.amazon.awssdk.transfer.s3.model.Upload
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import java.util.function.Function
import grails.testing.gorm.DataTest
import groovy.util.logging.Slf4j
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import spock.lang.Specification

@Slf4j
class S3StorageOperationsSpec extends Specification implements DataTest {

    def setupSpec() {
        mockDomains(Image, StorageLocation, S3StorageLocation)
    }

    private static class TestOps extends S3StorageOperations {
        S3Client mockClient
        S3AsyncClient mockSmallAsyncClient
        S3AsyncClient mockDownloadAsyncClient
        S3AsyncClient mockUploadAsyncClient
        S3TransferManager mockTransferManager
        boolean forceAsync = false

        @Override
        protected S3Client getS3Client() { return mockClient }

        @Override
        protected S3AsyncClient getSmallS3AsyncClient() { return mockSmallAsyncClient }

        @Override
        protected S3AsyncClient getS3DownloadAsyncClient() { return mockDownloadAsyncClient }

        @Override
        protected S3AsyncClient getS3UploadAsyncClient() { return mockUploadAsyncClient }

        @Override
        protected S3TransferManager getS3TransferManager() { return mockTransferManager ?: super.getS3TransferManager() }

        @Override
        protected boolean isUseAsyncS3Client() { return forceAsync }
    }

    private static class LegacyAsyncClientOps extends S3StorageOperations {
        S3AsyncClient legacyAsyncClient

        @Override
        protected S3AsyncClient getS3AsyncClient() { return legacyAsyncClient }

        S3AsyncClient smallAsyncClient() { return getSmallS3AsyncClient() }
        S3AsyncClient downloadAsyncClient() { return getS3DownloadAsyncClient() }
        S3AsyncClient uploadAsyncClient() { return getS3UploadAsyncClient() }
    }

    private void consumeBody(UploadRequest req) {
        req.requestBody().subscribe(new org.reactivestreams.Subscriber<java.nio.ByteBuffer>() {
            @Override void onSubscribe(org.reactivestreams.Subscription s) { s.request(Long.MAX_VALUE) }
            @Override void onNext(java.nio.ByteBuffer t) {}
            @Override void onError(Throwable t) {}
            @Override void onComplete() {}
        })
    }

    private CompletableFuture<byte[]> captureBody(UploadRequest req) {
        def completed = new CompletableFuture<byte[]>()
        def output = new ByteArrayOutputStream()
        req.requestBody().subscribe(new Subscriber<java.nio.ByteBuffer>() {
            @Override void onSubscribe(Subscription subscription) { subscription.request(Long.MAX_VALUE) }
            @Override void onNext(java.nio.ByteBuffer buffer) {
                def bytes = new byte[buffer.remaining()]
                buffer.get(bytes)
                output.write(bytes)
            }
            @Override void onError(Throwable error) { completed.completeExceptionally(error) }
            @Override void onComplete() { completed.complete(output.toByteArray()) }
        })
        return completed
    }

    def "store uses PublicRead ACL and public cache when publicRead=true"() {
        given:
        def client = Mock(S3Client)
        def tm = Mock(S3TransferManager)
        def upload = Mock(Upload)
        def completedUpload = CompletedUpload.builder()
                .response(PutObjectResponse.builder().eTag('etag').build())
                .build()

        def ops = new TestOps(bucket: 'b', publicRead: true, privateAcl: false, mockClient: client, mockTransferManager: tm)

        when:
        ops.store('uuid-1', new ByteArrayInputStream('x'.bytes), 'image/jpeg', null, 'x'.bytes.length)

        then:
        1 * tm.upload(_ as UploadRequest) >> { UploadRequest req ->
            def request = req.putObjectRequest()
            assert request.contentType() == 'image/jpeg'
            assert request.acl() == ObjectCannedACL.PUBLIC_READ
            assert request.cacheControl() == 'public,s-maxage=31536000,max-age=31536000'
            consumeBody(req)
            return upload
        }
        1 * upload.completionFuture() >> CompletableFuture.completedFuture(completedUpload)
    }

    def "store uses Private ACL and private cache when privateAcl=true and publicRead=false"() {
        given:
        def client = Mock(S3Client)
        def tm = Mock(S3TransferManager)
        def upload = Mock(Upload)
        def completedUpload = CompletedUpload.builder()
                .response(PutObjectResponse.builder().eTag('etag').build())
                .build()

        def ops = new TestOps(bucket: 'b', publicRead: false, privateAcl: true, mockClient: client, mockTransferManager: tm)

        when:
        ops.store('uuid-2', new ByteArrayInputStream('y'.bytes), 'image/png', 'inline', 'y'.bytes.length)

        then:
        1 * tm.upload(_ as UploadRequest) >> { UploadRequest req ->
            def request = req.putObjectRequest()
            assert request.contentDisposition() == 'inline'
            assert request.acl() == ObjectCannedACL.PRIVATE
            assert request.cacheControl() == 'private,max-age=31536000'
            consumeBody(req)
            return upload
        }
        1 * upload.completionFuture() >> CompletableFuture.completedFuture(completedUpload)
    }

    def "store leaves ACL and cache-control unset when both flags are false"() {
        given:
        def client = Mock(S3Client)
        def tm = Mock(S3TransferManager)
        def upload = Mock(Upload)
        def completedUpload = CompletedUpload.builder()
                .response(PutObjectResponse.builder().eTag('etag').build())
                .build()

        def ops = new TestOps(bucket: 'b', publicRead: false, privateAcl: false, mockClient: client, mockTransferManager: tm)

        when:
        ops.store('uuid-3', new ByteArrayInputStream('z'.bytes), 'image/gif', null, null)

        then:
        1 * tm.upload(_ as UploadRequest) >> { UploadRequest req ->
            def request = req.putObjectRequest()
            assert request.acl() == null
            assert request.cacheControl() == null
            consumeBody(req)
            return upload
        }
        1 * upload.completionFuture() >> CompletableFuture.completedFuture(completedUpload)
    }

    def "updateACL sets PublicRead and public cache-control when publicRead=true"() {
        given:
        def client = Mock(S3Client)
        def transferManager = Mock(S3TransferManager)
        def iterable = Mock(ListObjectsV2Iterable)
        def object = S3Object.builder().key('k').size(1L).build()
        def ops = new TestOps(bucket: 'b', prefix: '', publicRead: true, privateAcl: false, mockClient: client, mockTransferManager: transferManager)

        when:
        ops.updateACL()

        then:
        1 * client.listObjectsV2Paginator(_ as Consumer<ListObjectsV2Request.Builder>) >> iterable
        1 * iterable.iterator() >> { return [ListObjectsV2Response.builder().contents([object]).isTruncated(false).build()].iterator() }
        1 * client.putObjectAcl(_ as Consumer<PutObjectAclRequest.Builder>) >> { Consumer<PutObjectAclRequest.Builder> c ->
            def b = PutObjectAclRequest.builder(); c.accept(b);
            assert b.build().acl() == ObjectCannedACL.PUBLIC_READ
        }
        1 * client.headObject(_ as Consumer<HeadObjectRequest.Builder>) >> { Consumer<HeadObjectRequest.Builder> c -> }
        1 * client.copyObject(_ as Consumer<CopyObjectRequest.Builder>)
    }

    def "updateACL sets Private and private cache-control when privateAcl=true and publicRead=false"() {
        given:
        def client = Mock(S3Client)
        def transferManager = Mock(S3TransferManager)
        def iterable = Mock(ListObjectsV2Iterable)
        def object = S3Object.builder().key('k').size(1L).build()
        def ops = new TestOps(bucket: 'b', prefix: '', publicRead: false, privateAcl: true, mockClient: client, mockTransferManager: transferManager)

        when:
        ops.updateACL()

        then:
        1 * client.listObjectsV2Paginator(_ as Consumer<ListObjectsV2Request.Builder>) >> iterable
        1 * iterable.iterator() >> { return [ListObjectsV2Response.builder().contents([object]).isTruncated(false).build()].iterator() }

        1 * client.putObjectAcl(_ as Consumer<PutObjectAclRequest.Builder>) >> { Consumer<PutObjectAclRequest.Builder> c ->
            def b = PutObjectAclRequest.builder(); c.accept(b);
            assert b.build().acl() == ObjectCannedACL.PRIVATE
        }
        1 * client.headObject(_ as Consumer<HeadObjectRequest.Builder>) >> { Consumer<HeadObjectRequest.Builder> c -> }
        1 * client.copyObject(_ as Consumer<CopyObjectRequest.Builder>)
    }

    def "updateACL does not change ACL and removes cache-control when neither flag is set"() {
        given:
        def client = Mock(S3Client)
        def transferManager = Mock(S3TransferManager)

        def iterable = Mock(ListObjectsV2Iterable)
        def object = S3Object.builder().key('k').size(1L).build()

        def ops = new TestOps(bucket: 'b', prefix: '', publicRead: false, privateAcl: false, mockClient: client, mockTransferManager: transferManager)

        when:
        ops.updateACL()

        then:
        1 * client.listObjectsV2Paginator(_ as Consumer<ListObjectsV2Request.Builder>) >> iterable
        1 * iterable.iterator() >> { return [ListObjectsV2Response.builder().contents([object]).isTruncated(false).build()].iterator() }

        0 * client.putObjectAcl(_)
        1 * client.headObject(_ as Consumer<HeadObjectRequest.Builder>) >> { Consumer<HeadObjectRequest.Builder> c -> }
        1 * client.copyObject(_ as Consumer<CopyObjectRequest.Builder>)
    }

    def "verifySettings uses small async client for head/put/delete when forceAsync is enabled"() {
        given:
        def smallAsync = Mock(S3AsyncClient)
        def downloadAsync = Mock(S3AsyncClient)
        def client = Mock(S3Client)
        def ops = new TestOps(bucket: 'b', prefix: '', mockClient: client, mockSmallAsyncClient: smallAsync, mockDownloadAsyncClient: downloadAsync, forceAsync: true)

        when:
        def result = ops.verifySettings()

        then:
        result
        1 * smallAsync.headBucket(_ as Consumer) >> CompletableFuture.completedFuture(HeadBucketResponse.builder().build())
        1 * smallAsync.putObject(_ as Consumer, _) >> CompletableFuture.completedFuture(PutObjectResponse.builder().eTag('etag').build())
        1 * smallAsync.deleteObject(_ as Consumer) >> CompletableFuture.completedFuture(DeleteObjectResponse.builder().build())
        0 * client._
        0 * downloadAsync._
    }

    def "stored uses small async client headObject when forceAsync is enabled"() {
        given:
        def smallAsync = Mock(S3AsyncClient)
        def downloadAsync = Mock(S3AsyncClient)
        def client = Mock(S3Client)
        def ops = new TestOps(bucket: 'b', prefix: '', mockClient: client, mockSmallAsyncClient: smallAsync, mockDownloadAsyncClient: downloadAsync, forceAsync: true)

        when:
        def exists = ops.stored('uuid-1')

        then:
        exists
        1 * smallAsync.headObject(_ as Consumer) >> CompletableFuture.completedFuture(HeadObjectResponse.builder().build())
        0 * client._
        0 * downloadAsync._
    }

    def "retrieve uses the download async client when forceAsync is enabled"() {
        given:
        def smallAsync = Mock(S3AsyncClient)
        def downloadAsync = Mock(S3AsyncClient)
        def uploadAsync = Mock(S3AsyncClient)
        def client = Mock(S3Client)
        def bytes = 'abc'.bytes
        def responseBytes = ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), bytes)
        def ops = new TestOps(bucket: 'b', prefix: '', mockClient: client, mockSmallAsyncClient: smallAsync, mockDownloadAsyncClient: downloadAsync, mockUploadAsyncClient: uploadAsync, forceAsync: true)

        when:
        def actual = ops.retrieve('uuid-2')

        then:
        actual == bytes
        1 * downloadAsync.getObject(_ as Consumer, _) >> CompletableFuture.completedFuture(responseBytes)
        0 * smallAsync.getObject(_, _)
        0 * uploadAsync._
        0 * client._
    }

    def "legacy async client override remains the single-client seam for subclasses"() {
        given:
        def legacyAsync = Mock(S3AsyncClient)
        def transferManager = Mock(S3TransferManager)
        def transferManagerClients = []
        def ops = new LegacyAsyncClientOps(bucket: 'b', legacyAsyncClient: legacyAsync)
        ops.setTransferManagerFactoryForTesting({ S3AsyncClient client ->
            transferManagerClients << client
            transferManager
        } as Function<S3AsyncClient, S3TransferManager>)

        when:
        def smallClient = ops.smallAsyncClient()
        def downloadClient = ops.downloadAsyncClient()
        def uploadClient = ops.uploadAsyncClient()
        def actualTransferManager = ops.getS3TransferManager()

        then:
        smallClient.is(legacyAsync)
        downloadClient.is(legacyAsync)
        uploadClient.is(legacyAsync)
        actualTransferManager.is(transferManager)
        transferManagerClients == [legacyAsync]
    }

    def "close releases the legacy transfer manager without closing the subclass-owned async client"() {
        given:
        def legacyAsync = Mock(S3AsyncClient)
        def transferManager = Mock(S3TransferManager)
        def ops = new LegacyAsyncClientOps(bucket: 'b', legacyAsyncClient: legacyAsync)
        ops.setTransferManagerFactoryForTesting({ S3AsyncClient client ->
            assert client.is(legacyAsync)
            transferManager
        } as Function<S3AsyncClient, S3TransferManager>)

        when:
        ops.getS3TransferManager()
        ops.close()

        then:
        1 * transferManager.close()
        0 * legacyAsync.close()
    }

    def "migrateTo streams a blocking S3 download into an upload-role transfer manager"() {
        given:
        def sourceSmallAsync = Mock(S3AsyncClient)
        def downloadAsync = Mock(S3AsyncClient)
        def uploadAsync = Mock(S3AsyncClient)
        def sourceClient = Mock(S3Client)
        def destinationClient = Mock(S3Client)
        def transferManager = Mock(S3TransferManager)
        def upload = Mock(Upload)
        def completedUpload = CompletedUpload.builder().response(PutObjectResponse.builder().eTag('etag').build()).build()
        def source = new TestOps(bucket: 'source-bucket', prefix: '', mockClient: sourceClient, mockSmallAsyncClient: sourceSmallAsync, mockDownloadAsyncClient: downloadAsync, forceAsync: true)
        def destination = new TestOps(bucket: 'destination-bucket', prefix: '', mockClient: destinationClient, mockUploadAsyncClient: uploadAsync)
        def uuid = 'uuid-source'
        def sourceKey = source.createOriginalPathFromUUID(uuid)
        def object = S3Object.builder().key(sourceKey).size(6L).build()
        def pages = Mock(ListObjectsV2Publisher)
        def subscription = Stub(Subscription)
        CompletableFuture<byte[]> uploadedBody
        def transferManagerClients = []

        destination.setTransferManagerFactoryForTesting({ S3AsyncClient client ->
            transferManagerClients << client
            transferManager
        } as Function<S3AsyncClient, S3TransferManager>)

        when:
        source.migrateTo(uuid, 'image/jpeg', destination)

        then:
        1 * sourceSmallAsync.listObjectsV2Paginator(_ as Consumer) >> pages
        1 * pages.subscribe(_ as Subscriber) >> { Subscriber<ListObjectsV2Response> subscriber ->
            subscriber.onSubscribe(subscription)
            subscriber.onNext(ListObjectsV2Response.builder().contents([object]).build())
            subscriber.onComplete()
        }
        1 * sourceSmallAsync.headObject(_ as Consumer) >> CompletableFuture.completedFuture(HeadObjectResponse.builder().contentType('image/jpeg').contentLength(6L).build())
        1 * downloadAsync.getObject(_ as Consumer, _) >> CompletableFuture.completedFuture(new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream('source'.bytes)))
        1 * transferManager.upload(_ as UploadRequest) >> { UploadRequest request ->
            uploadedBody = captureBody(request)
            upload
        }
        1 * upload.completionFuture() >> CompletableFuture.completedFuture(completedUpload)
        transferManagerClients == [uploadAsync]
        uploadedBody.get() == 'source'.bytes
        0 * sourceClient._
        0 * destinationClient._
        0 * uploadAsync._
    }
}
