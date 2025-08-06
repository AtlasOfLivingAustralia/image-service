package au.org.ala.images

import cloud.localstack.Constants
import cloud.localstack.Localstack
import cloud.localstack.ServiceName
import cloud.localstack.docker.LocalstackDockerExtension
import cloud.localstack.docker.annotation.LocalstackDockerProperties
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.junit.jupiter.api.extension.ExtendWith
import spock.lang.Ignore
import spock.lang.Specification

@Ignore
@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = [ ServiceName.S3 ], imageTag = '4.1.1')
class S3URLConnectionSpec extends Specification {

    def setupSpec() {

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().
                withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(Localstack.INSTANCE.endpointS3, Constants.DEFAULT_REGION)).
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(Constants.TEST_ACCESS_KEY, Constants.TEST_SECRET_KEY))).
                withClientConfiguration(
                        new ClientConfiguration()
                                .withValidateAfterInactivityMillis(200))
        builder.setPathStyleAccessEnabled(true)
        AmazonS3 clientS3 = builder.build()
        clientS3.createBucket('example')
        clientS3.putObject('example', 'key', 'content')
        clientS3.getObject('example', 'key')

        def localstack = Localstack.INSTANCE

    }

    def cleanupSpec() {
        Localstack.INSTANCE.stop()
    }

    def "should throw an exception if the URL scheme is not 's3'"() {
        given:
        def url = new URI("http://example.com").toURL()

        when:
        new S3URLConnection(url)

        then:
        thrown(IllegalArgumentException)
    }

    def "should throw an exception if the URL path is missing"() {
        given:
        def url = new URI("s3://example.s3.region.amazonaws.com").toURL()

        when:
        new S3URLConnection(url)

        then:
        thrown(IllegalArgumentException)
    }

    def "should throw an exception if the URL host is missing"() {
        given:
        def url = new URI("s3:///key").toURL()

        when:
        new S3URLConnection(url)

        then:
        thrown(IllegalArgumentException)
    }

    def "should return the content of the object in the bucket with the given key"() {
        given:

        def userinfo = "${Constants.TEST_ACCESS_KEY}:${Constants.TEST_SECRET_KEY}"
        def url = new URI("s3://${userinfo}@example.s3.${Constants.DEFAULT_REGION}.amazonaws.com/key").toURL()

        when:
        def connection = new S3URLConnection(url)
        connection.setEndpoint(Localstack.INSTANCE.endpointS3)
        connection.setPathStyleAccessEnabled(true)
        def content = connection.getInputStream().text

        then:
        content == "content"
    }

}
