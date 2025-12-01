package au.org.ala.images

import au.org.ala.images.storage.S3StorageOperations
import au.org.ala.web.AuthService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3ObjectSummary
import grails.testing.gorm.DataTest
import grails.web.mapping.LinkGenerator
import groovy.util.logging.Slf4j
import org.grails.spring.beans.factory.InstanceFactoryBean
import spock.lang.Specification

@Slf4j
class S3StorageOperationsSpec extends Specification implements DataTest {

    def setupSpec() {
        mockDomains(Image, StorageLocation, S3StorageLocation)
    }

    private static class TestOps extends S3StorageOperations {
        AmazonS3 mockClient
        @Override
        protected AmazonS3 getS3Client() { return mockClient }
    }

    def "store uses PublicRead ACL and public cache when publicRead=true"() {
        given:
        def client = Mock(AmazonS3)
        def ops = new TestOps(bucket: 'b', publicRead: true, privateAcl: false, mockClient: client)

        when:
        ops.store('uuid-1', new ByteArrayInputStream('x'.bytes), 'image/jpeg', null, 123)

        then:
        1 * client.putObject({ it.metadata.contentType == 'image/jpeg' &&
            it.metadata.contentLength == 123 &&
            it.metadata.rawMetadata['x-amz-acl'] == CannedAccessControlList.PublicRead.toString() &&
            it.metadata.cacheControl == 'public,s-maxage=31536000,max-age=31536000' })
    }

    def "store uses Private ACL and private cache when privateAcl=true and publicRead=false"() {
        given:
        def client = Mock(AmazonS3)
        def ops = new TestOps(bucket: 'b', publicRead: false, privateAcl: true, mockClient: client)

        when:
        ops.store('uuid-2', new ByteArrayInputStream('y'.bytes), 'image/png', 'inline', 10)

        then:
        1 * client.putObject({ it.metadata.contentDisposition == 'inline' &&
            it.metadata.rawMetadata['x-amz-acl'] == CannedAccessControlList.Private.toString() &&
            it.metadata.cacheControl == 'private,max-age=31536000' })
    }

    def "store leaves ACL and cache-control unset when both flags are false"() {
        given:
        def client = Mock(AmazonS3)
        def ops = new TestOps(bucket: 'b', publicRead: false, privateAcl: false, mockClient: client)

        when:
        ops.store('uuid-3', new ByteArrayInputStream('z'.bytes), 'image/gif', null, null)

        then:
        1 * client.putObject({ !it.metadata.rawMetadata.containsKey('x-amz-acl') && it.metadata.cacheControl == null })
    }

    def "updateACL sets PublicRead and public cache-control when publicRead=true"() {
        given:
        def client = Mock(AmazonS3)
        def obj = new S3ObjectSummary(bucketName: 'b', key: 'k')
        def md = new ObjectMetadata()
        def ops = new TestOps(bucket: 'b', prefix: '', publicRead: true, privateAcl: false, mockClient: client)

        when:
        ops.updateACL()

        then:
        1 * client.listObjects('b', '') >> { args ->
            new ObjectListing().tap {
                objectSummaries.addAll([obj])
                truncated = false
            }
        }

        1 * client.setObjectAcl('b', 'k', CannedAccessControlList.PublicRead)
        1 * client.getObjectMetadata('b', 'k') >> md
        1 * client.copyObject({ CopyObjectRequest cor ->
            cor.destinationBucketName == 'b' && cor.destinationKey == 'k' &&
                cor.newObjectMetadata.cacheControl == 'public,s-maxage=31536000,max-age=31536000'
        })
    }

    def "updateACL sets Private and private cache-control when privateAcl=true and publicRead=false"() {
        given:
        def client = Mock(AmazonS3)
        def obj = new S3ObjectSummary(bucketName: 'b', key: 'k')
        def md = new ObjectMetadata()
        def ops = new TestOps(bucket: 'b', prefix: '', publicRead: false, privateAcl: true, mockClient: client)

        when:
        ops.updateACL()

        then:
        1 * client.listObjects('b', '') >> { args ->
            new ObjectListing().tap {
                objectSummaries.addAll([obj])
                truncated = false
            }
        }
        1 * client.setObjectAcl('b', 'k', CannedAccessControlList.Private)
        1 * client.getObjectMetadata('b', 'k') >> md
        1 * client.copyObject({ CopyObjectRequest cor ->
            cor.newObjectMetadata.cacheControl == 'private,max-age=31536000'
        })
    }

    def "updateACL does not change ACL and removes cache-control when neither flag is set"() {
        given:
        def client = Mock(AmazonS3)
        def obj = new S3ObjectSummary(bucketName: 'b', key: 'k')
        def md = new ObjectMetadata()
        def ops = new TestOps(bucket: 'b', prefix: '', publicRead: false, privateAcl: false, mockClient: client)

        when:
        ops.updateACL()

        then:
        1 * client.listObjects('b', '') >> { args ->
            new ObjectListing().tap {
                objectSummaries.addAll([obj])
                truncated = false
            }
        }
        0 * client.setObjectAcl(_, _, _)
        1 * client.getObjectMetadata('b', 'k') >> md
        1 * client.copyObject({ CopyObjectRequest cor -> cor.newObjectMetadata.cacheControl == null })
    }
}
