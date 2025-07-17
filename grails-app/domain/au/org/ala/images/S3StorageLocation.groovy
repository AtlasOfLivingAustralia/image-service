package au.org.ala.images

import au.org.ala.images.storage.S3StorageOperations
import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.util.ByteSinkFactory
import com.amazonaws.AmazonClientException
import com.amazonaws.ClientConfiguration
import com.amazonaws.HttpMethod
import com.amazonaws.Protocol
import com.amazonaws.SdkClientException
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3ObjectSummary
import groovy.transform.EqualsAndHashCode
import net.lingala.zip4j.io.inputstream.ZipInputStream
import org.apache.commons.io.FilenameUtils

@EqualsAndHashCode(includes = ['region', 'bucket', 'prefix'])
class S3StorageLocation extends StorageLocation {

    String region
    String bucket
    String prefix
    String accessKey
    String secretKey
    boolean containerCredentials
    boolean publicRead
    boolean redirect
    String cloudfrontDomain

    // for testing only, not exposed to UI
    boolean pathStyleAccess = false
    String hostname = ''

    static transients = ['_s3Client', 'storageOperations', '$storageOperations']

    static constraints = {
        prefix nullable: false, blank: true
        pathStyleAccess nullable: true
        hostname nullable: true
    }

    static mapping = {
        cache true
    }

    @Delegate @Lazy S3StorageOperations storageOperations = {
        new S3StorageOperations(
                bucket: bucket,
                region: region,
                prefix: prefix,
                accessKey: accessKey,
                secretKey: secretKey,
                containerCredentials: containerCredentials,
                publicRead: publicRead,
                redirect: redirect,
                pathStyleAccess: pathStyleAccess,
                hostname: hostname,
                cloudfrontDomain: cloudfrontDomain
        )
    }()

    StorageOperations asStandaloneStorageOperations() {
        storageOperations
    }

    @Override
    String toString() {
        "S3($id): $region:$bucket:${prefix ?: ''}"
    }
}
