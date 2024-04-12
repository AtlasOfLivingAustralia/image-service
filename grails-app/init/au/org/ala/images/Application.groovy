package au.org.ala.images

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.rekognition.AmazonRekognitionClient
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime
import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import org.springframework.context.annotation.Bean
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder

class Application extends GrailsAutoConfiguration {
    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

    @Bean
    AWSCredentialsProvider awsCredentialsProvider() {
        def accessKey = grailsApplication.config.getProperty('aws.access-key') ?: System.getenv('AWS_ACCESS_KEY_ID')
        def secretKey = grailsApplication.config.getProperty('aws.secret-key') ?: System.getenv('AWS_SECRET_ACCESS_KEY')
        def sessionToken = grailsApplication.config.getProperty('aws.session-token')


        if (accessKey && secretKey) {
            def credentials
            if (sessionToken) {
                credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken)
            } else {
                credentials = new BasicAWSCredentials(accessKey, secretKey)
            }
            return new AWSStaticCredentialsProvider(credentials)
        } else {
            return DefaultAWSCredentialsProviderChain.instance
        }
    }

    @Bean('awsRegion')
    Region awsRegion() {
        def region = grailsApplication.config.getProperty('aws.region', String, "ap-southeast-2")
        return region ? Region.getRegion(Regions.fromName(region)) : Regions.currentRegion
    }

    @Bean
    AmazonRekognitionClient rekognitionClient(AWSCredentialsProvider awsCredentialsProvider, Region awsRegion) {
        return AmazonRekognitionClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .withRegion(awsRegion.toString())
                .build();
    }

    @Bean
    AmazonS3Client s3Client(AWSCredentialsProvider awsCredentialsProvider, Region awsRegion) {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .withRegion(awsRegion.toString())
                .build();
    }

    @Bean
    AmazonSageMakerRuntime sageMakerRuntime(AWSCredentialsProvider awsCredentialsProvider, Region awsRegion) {
        return AmazonSageMakerRuntimeClientBuilder.standard()
        .withRegion(awsRegion.toString())
        .withCredentials(awsCredentialsProvider)
        .build()
    }

}