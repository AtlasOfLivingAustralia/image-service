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
import spock.lang.Specification


@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = [ ServiceName.S3 ], imageTag = '4.1.1')
class S3URLConnectionSpec extends Specification {

    def setupSpec() {

        System.out.println(Localstack.INSTANCE.getServicePort(ServiceName.S3))
        System.out.println(Localstack.INSTANCE.getEdgePort())
        System.out.println(Localstack.INSTANCE.getEndpointS3())
        System.out.println(Localstack.INSTANCE.getEndpointAPIGateway())

        def endpoint1 = "http://localhost:${Localstack.INSTANCE.getEdgePort()}"
        println "Using LocalStack S3 endpoint: $endpoint1"

        def host = System.getenv("LOCALSTACK_HOST") ?: "host.docker.internal"  // or a Docker network alias
        def endpoint2 = "http://${host}:${Localstack.INSTANCE.getEdgePort()}"

        println(endpoint2)

        new URL(endpoint1).openConnection().with {
            connectTimeout = 2000
            connect()
            println "Successfully connected to LocalStack S3 endpoint"
        }

        AmazonS3ClientBuilder builder1 = AmazonS3ClientBuilder.standard().
                withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint1, Constants.DEFAULT_REGION)).
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(Constants.TEST_ACCESS_KEY, Constants.TEST_SECRET_KEY))).
                withClientConfiguration(
                        new ClientConfiguration()
                                .withValidateAfterInactivityMillis(200))
        builder1.setPathStyleAccessEnabled(true)
        AmazonS3 clientS31 = builder1.build()
        clientS31.createBucket('example1')

        String endpoint = System.getenv("USE_EXTERNAL_LOCALSTACK")?.toBoolean()
                ? "http://localhost:4566"
                : Localstack.INSTANCE.endpointS3

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().
                withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, Constants.DEFAULT_REGION)).
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
