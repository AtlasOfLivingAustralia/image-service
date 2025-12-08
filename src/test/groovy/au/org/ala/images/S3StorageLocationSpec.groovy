package au.org.ala.images

import cloud.localstack.Constants
import cloud.localstack.Localstack
import cloud.localstack.ServiceName
import cloud.localstack.docker.LocalstackDockerExtension
import cloud.localstack.docker.annotation.LocalstackDockerProperties
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import grails.testing.gorm.DomainUnitTest
import org.junit.jupiter.api.extension.ExtendWith

import static cloud.localstack.deprecated.TestUtils.DEFAULT_REGION

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = [ ServiceName.S3 ], imageTag = '4.1.1')
class S3StorageLocationSpec extends StorageLocationSpec implements DomainUnitTest<S3StorageLocation> {

    List<S3StorageLocation> getStorageLocations() {[
            new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bouquet', prefix: '', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/prefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix2', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix3/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix4/subprefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/prefix5/subprefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix6/subprefix/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/prefix7/subprefix/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
    ]}
    S3StorageLocation alternateStorageLocation

    def setupSpec() {

        S3Client clientS3 = S3Client.builder()
                .endpointOverride(URI.create(Localstack.INSTANCE.endpointS3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(Constants.TEST_ACCESS_KEY, Constants.TEST_SECRET_KEY)))
                .region(Region.of(Constants.DEFAULT_REGION))
                .forcePathStyle(true)
                .build()

        clientS3.createBucket(CreateBucketRequest.builder().bucket('bouquet').build())
        clientS3.createBucket(CreateBucketRequest.builder().bucket('bucket').build())
        clientS3.createBucket(CreateBucketRequest.builder().bucket('other-bucket').build())
    }

    def setup() {
        def localstack = Localstack.INSTANCE
        alternateStorageLocation = new S3StorageLocation(region: DEFAULT_REGION, bucket: 'other-bucket', prefix: '/other/prefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, cloudfrontDomain: '', pathStyleAccess: true, hostname: localstack.endpointS3).save()
    }

}
