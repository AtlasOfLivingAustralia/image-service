package au.org.ala.images

import au.org.ala.images.storage.S3StorageOperations
import au.org.ala.images.storage.StorageOperations
import au.org.ala.images.util.ByteSinkFactory
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
    // When true, explicitly request S3 canned Private ACL on upload/updates.
    // When false (and publicRead is also false), no ACL header is added and
    // bucket defaults/policies apply.
    boolean privateAcl
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
        cloudfrontDomain nullable: true, blank: true
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
                privateAcl: privateAcl,
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
