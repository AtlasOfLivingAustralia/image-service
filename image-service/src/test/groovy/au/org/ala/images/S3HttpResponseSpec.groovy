package au.org.ala.images

import cloud.localstack.Constants
import cloud.localstack.Localstack
import cloud.localstack.ServiceName
import cloud.localstack.docker.LocalstackDockerExtension
import cloud.localstack.docker.annotation.LocalstackDockerProperties
import org.junit.jupiter.api.extension.ExtendWith
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import spock.lang.Specification

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = [ ServiceName.S3 ], imageTag = '4.1.1')
class S3HttpResponseSpec extends Specification {
    def setupSpec() {

        S3Client clientS3 = S3Client.builder()
                .endpointOverride(URI.create(Localstack.INSTANCE.endpointS3))
                .region(Region.of(Constants.DEFAULT_REGION))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(Constants.TEST_ACCESS_KEY, Constants.TEST_SECRET_KEY)))
                .forcePathStyle(true)
                .build()

        clientS3.createBucket(CreateBucketRequest.builder().bucket('example').build())
        clientS3.putObject(PutObjectRequest.builder().bucket('example').key('key').build(), RequestBody.fromString('content'))
        try (def object = clientS3.getObject(GetObjectRequest.builder().bucket('example').key('key').build())) {
            // just to verify it works
        }

        def localstack = Localstack.INSTANCE

    }

    def "should throw an exception if the URL scheme is not 's3'"() {
        given:
        def uri = new URI("http://example.com")

        when:
        new S3HttpResponse(uri)

        then:
        thrown(IllegalArgumentException)
    }

    def "should throw an exception if the URL path is missing"() {
        given:
        def uri = new URI("s3://example.s3.region.amazonaws.com")

        when:
        new S3HttpResponse(uri)

        then:
        thrown(IllegalArgumentException)
    }

    def "should throw an exception if the URL host is missing"() {
        given:
        def uri = new URI("s3:///key")

        when:
        new S3HttpResponse(uri)

        then:
        thrown(IllegalArgumentException)
    }

    def "should return the content of the object in the bucket with the given key"() {
        given:

        def userinfo = "${Constants.TEST_ACCESS_KEY}:${Constants.TEST_SECRET_KEY}"
        def uri = new URI("s3://${userinfo}@example.s3.${Constants.DEFAULT_REGION}.amazonaws.com/key")

        when:
        def response = new S3HttpResponse(uri)
        response.setEndpoint(Localstack.INSTANCE.endpointS3)
        response.setPathStyleAccessEnabled(true)
        def content = response.getInputStream().text

        then:
        content == "content"
    }
}
