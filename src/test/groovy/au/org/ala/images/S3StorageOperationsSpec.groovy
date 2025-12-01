package au.org.ala.images

import au.org.ala.images.storage.S3StorageOperations
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import software.amazon.awssdk.transfer.s3.model.Upload
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import grails.testing.gorm.DataTest
import groovy.util.logging.Slf4j
import spock.lang.Specification

@Slf4j
class S3StorageOperationsSpec extends Specification implements DataTest {

    def setupSpec() {
        mockDomains(Image, StorageLocation, S3StorageLocation)
    }

    private static class TestOps extends S3StorageOperations {
        S3Client mockClient
        S3TransferManager mockTransferManager

        @Override
        protected S3Client getS3Client() { return mockClient }

        @Override
        protected S3TransferManager getS3TransferManager() { return mockTransferManager }
    }

    private void consumeBody(UploadRequest req) {
        req.requestBody().subscribe(new org.reactivestreams.Subscriber<java.nio.ByteBuffer>() {
            @Override void onSubscribe(org.reactivestreams.Subscription s) { s.request(Long.MAX_VALUE) }
            @Override void onNext(java.nio.ByteBuffer t) {}
            @Override void onError(Throwable t) {}
            @Override void onComplete() {}
        })
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
        ops.store('uuid-1', new ByteArrayInputStream('x'.bytes), 'image/jpeg', null, 123)

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
        ops.store('uuid-2', new ByteArrayInputStream('y'.bytes), 'image/png', 'inline', 10)

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
}
