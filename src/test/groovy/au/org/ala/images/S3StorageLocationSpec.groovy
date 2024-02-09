package au.org.ala.images

import cloud.localstack.Constants
import cloud.localstack.Localstack
import cloud.localstack.docker.LocalstackDockerExtension
import cloud.localstack.docker.annotation.LocalstackDockerProperties
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import grails.testing.gorm.DomainUnitTest
import org.junit.jupiter.api.extension.ExtendWith

import static cloud.localstack.deprecated.TestUtils.DEFAULT_REGION

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = [ "s3" ], imageTag = '0.12.11')
class S3StorageLocationSpec extends StorageLocationSpec implements DomainUnitTest<S3StorageLocation> {

    List<S3StorageLocation> getStorageLocations() {[
            new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bouquet', prefix: '', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/prefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix2', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix3/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix4/subprefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/prefix5/subprefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: 'prefix6/subprefix/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
            ,new S3StorageLocation(region: DEFAULT_REGION, bucket: 'bucket', prefix: '/prefix7/subprefix/', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: Localstack.INSTANCE.endpointS3).save()
    ]}
    S3StorageLocation alternateStorageLocation

    def setupSpec() {

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().
                withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(Localstack.INSTANCE.endpointS3, Constants.DEFAULT_REGION)).
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(Constants.TEST_ACCESS_KEY, Constants.TEST_SECRET_KEY))).
                withClientConfiguration(
                        new ClientConfiguration()
                                .withValidateAfterInactivityMillis(200))
        builder.setPathStyleAccessEnabled(true)
        AmazonS3 clientS3 = builder.build()
        clientS3.createBucket('bouquet')
        clientS3.createBucket('bucket')
        clientS3.createBucket('other-bucket')
    }

    def setup() {
        def localstack = Localstack.INSTANCE
        alternateStorageLocation = new S3StorageLocation(region: DEFAULT_REGION, bucket: 'other-bucket', prefix: '/other/prefix', accessKey: Constants.TEST_ACCESS_KEY, secretKey: Constants.TEST_SECRET_KEY, publicRead: false, redirect: false, pathStyleAccess: true, hostname: localstack.endpointS3).save()
    }

}
