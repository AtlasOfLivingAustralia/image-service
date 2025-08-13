package au.org.ala.images.storage

import au.org.ala.images.AuditService
import au.org.ala.images.DefaultStoragePathStrategy
import au.org.ala.images.ImageInfo
import au.org.ala.images.Range
import au.org.ala.images.S3ByteSinkFactory
import au.org.ala.images.StoragePathStrategy
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
import com.amazonaws.services.s3.model.ObjectTagging
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.model.Tag
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j
import net.lingala.zip4j.io.inputstream.ZipInputStream
import org.apache.commons.io.FilenameUtils

@CompileStatic
@Slf4j
@EqualsAndHashCode(includes = ['region', 'bucket', 'prefix'])
class S3StorageOperations implements StorageOperations {

    String region
    String bucket
    String prefix
    String accessKey
    String secretKey
    boolean containerCredentials
    boolean publicRead
    boolean redirect
    String cloudfrontDomain

    boolean pathStyleAccess
    String hostname = ''

    private AmazonS3 _s3Client

    private AmazonS3 getS3Client() {
        if (!_s3Client) {

            def provider = containerCredentials ? new DefaultAWSCredentialsProviderChain() : new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))

            def builder = AmazonS3ClientBuilder.standard()
                    .withCredentials(provider)
                    .withClientConfiguration(buildClientConfiguration([:], [:]))
            if (pathStyleAccess) {
                builder.pathStyleAccessEnabled = true
            }
            if (hostname) {
                builder.endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(hostname, region)
            } else {
                builder.region = region
            }

            _s3Client = builder.build()
        }
        _s3Client
    }

    static ClientConfiguration buildClientConfiguration(Map defaultConfig, Map serviceConfig) {
        // TODO add config for S3 clients
        Map config = [
                connectionTimeout: defaultConfig.connectionTimeout ?: 0,
                maxConnections: defaultConfig.maxConnections ?: 0,
                maxErrorRetry: defaultConfig.maxErrorRetry ?: 0,
                protocol: defaultConfig.protocol ?: '',
                socketTimeout: defaultConfig.socketTimeout ?: 0,
                userAgent: defaultConfig.userAgent ?: '',
                proxyDomain: defaultConfig.proxyDomain ?: '',
                proxyHost: defaultConfig.proxyHost ?: '',
                proxyPassword: defaultConfig.proxyPassword ?: '',
                proxyPort: defaultConfig.proxyPort ?: 0,
                proxyUsername: defaultConfig.proxyUsername ?: '',
                proxyWorkstation: defaultConfig.proxyWorkstation ?: ''
        ]
        if (serviceConfig) {
            if (serviceConfig.connectionTimeout) config.connectionTimeout = serviceConfig.connectionTimeout
            if (serviceConfig.maxConnections) config.maxConnections = serviceConfig.maxConnections
            if (serviceConfig.maxErrorRetry) config.maxErrorRetry = serviceConfig.maxErrorRetry
            if (serviceConfig.protocol) config.protocol = serviceConfig.protocol
            if (serviceConfig.socketTimeout) config.socketTimeout = serviceConfig.socketTimeout
            if (serviceConfig.userAgent) config.userAgent = serviceConfig.userAgent
            if (serviceConfig.proxyDomain) config.proxyDomain = serviceConfig.proxyDomain
            if (serviceConfig.proxyHost) config.proxyHost = serviceConfig.proxyHost
            if (serviceConfig.proxyPassword) config.proxyPassword = serviceConfig.proxyPassword
            if (serviceConfig.proxyPort) config.proxyPort = serviceConfig.proxyPort
            if (serviceConfig.proxyUsername) config.proxyUsername = serviceConfig.proxyUsername
            if (serviceConfig.proxyWorkstation) config.proxyWorkstation = serviceConfig.proxyWorkstation
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration()
        if (config.connectionTimeout) clientConfiguration.connectionTimeout = config.connectionTimeout as Integer
        if (config.maxConnections) clientConfiguration.maxConnections = config.maxConnections as Integer
        if (config.maxErrorRetry) clientConfiguration.maxErrorRetry = config.maxErrorRetry as Integer
        if (config.protocol) {
            if (config.protocol.toString().toUpperCase() == 'HTTP') clientConfiguration.protocol = Protocol.HTTP
            else clientConfiguration.protocol = Protocol.HTTPS
        }
        if (config.socketTimeout) clientConfiguration.socketTimeout = config.socketTimeout as Integer
        if (config.userAgent) clientConfiguration.userAgent = config.userAgent
        if (config.proxyDomain) clientConfiguration.proxyDomain = config.proxyDomain
        if (config.proxyHost) clientConfiguration.proxyHost = config.proxyHost
        if (config.proxyPassword) clientConfiguration.proxyPassword = config.proxyPassword
        if (config.proxyPort) clientConfiguration.proxyPort = config.proxyPort as Integer
        if (config.proxyUsername) clientConfiguration.proxyUsername = config.proxyUsername
        if (config.proxyWorkstation) clientConfiguration.proxyWorkstation = config.proxyWorkstation
        clientConfiguration
    }

    @Override
    boolean isSupportsRedirect() {
        redirect
    }

    @Override
    URI redirectLocation(String path) {
//        def permission = s3Client.getObjectAcl(bucket, path).grantsAsList.find { grant ->
//            grant.grantee == GroupGrantee.AllUsers && [Permission.Read, Permission.Write, Permission.FullControl].contains(grant.permission)
//        }
        if (cloudfrontDomain) {
            // If a CloudFront domain is set, use that for redirects
            new URI("https://${cloudfrontDomain}/${path}")
        } else if (publicRead) {
            s3Client.getUrl(bucket, path).toURI()
        } else {
            // Set the presigned URL to expire after one hour.
            Date expiration = new Date()
            long expTimeMillis = expiration.getTime()
            expTimeMillis += 1000 * 60 * 60;
            expiration.setTime(expTimeMillis)

            // Generate the presigned URL.
            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(bucket, path)
                            .withMethod(HttpMethod.GET)
                            .withExpiration(expiration)
            s3Client.generatePresignedUrl(generatePresignedUrlRequest).toURI()
        }
    }

    def updateACL() {
        walkPrefix(storagePathStrategy().basePath()) { S3ObjectSummary objectSummary ->
            def path = objectSummary.key
            try {
                s3Client.setObjectAcl(bucket, path, publicRead ? CannedAccessControlList.PublicRead : CannedAccessControlList.Private)

                // update cache control metadata
                def objectMetadata = s3Client.getObjectMetadata(bucket, path)
                objectMetadata.cacheControl = (publicRead ? 'public,s-maxage=31536000' : 'private') + ',max-age=31536000'

                CopyObjectRequest request = new CopyObjectRequest(bucket, path, bucket, path)
                        .withNewObjectMetadata(objectMetadata)

                s3Client.copyObject(request)
            } catch (AmazonS3Exception e) {
                log.error('Error updating ACL for {}, public: {}, error: {}', path, publicRead, e.message)
            }
        }
    }

    private ObjectMetadata generateMetadata(String contentType, String contentDisposition = null, Long length = null) {
        ObjectMetadata metadata = new ObjectMetadata()
        metadata.setContentType(contentType)
        if (contentDisposition) {
            metadata.setContentDisposition(contentDisposition)
        }
        if (length != null) {
            metadata.setContentLength(length)
        }
        def acl
        if (publicRead) {
            acl = CannedAccessControlList.PublicRead
        } else {
            acl = CannedAccessControlList.Private
        }
        metadata.setHeader('x-amz-acl', acl.toString())
        metadata.cacheControl = (publicRead ? 'public,s-maxage=31536000' : 'private') + ',max-age=31536000'
//        metadata.setHeader('Expires', 'access + 1 year???')
        return metadata
    }

    @Override
    boolean verifySettings() {
        try {
            boolean result = s3Client.doesBucketExistV2(bucket)
            if (result) {
                String key = storagePathStrategy().basePath() + '/' + UUID.randomUUID().toString()
                def putResult = s3Client.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(new byte[1]), generateMetadata('application/octet-stream', null, 1)))
                s3Client.deleteObject(bucket, key)
            }
            return result
        } catch (SdkClientException e) {
            log.error("Exception while verifying S3 bucket {}", this, e)
            return false
        }
    }

    @Override
    void store(String uuid, InputStream stream, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        String path = createOriginalPathFromUUID(uuid)
        storeInternal(stream, path, contentType, contentDisposition, length, [imageType: 'original'])
    }

    @Override
    byte[] retrieve(String uuid) {
        if (uuid) {
            def imagePath = createOriginalPathFromUUID(uuid)
            def bytes
            try {
                def s3object = s3Client.getObject(new GetObjectRequest(bucket, imagePath))
                def inputStream = s3object.objectContent
                bytes = inputStream.withStream { it.bytes }
            } catch (AmazonS3Exception e) {
                if (e.statusCode == 404) {
                    throw new FileNotFoundException("S3 path $imagePath")
                } else {
                    throw e
                }
            }
            return bytes
        }
        return null
    }

    @Override
    InputStream inputStream(String path, Range range) throws FileNotFoundException {
        def request = new GetObjectRequest(bucket, path)
        if (range != null && !range.empty) {
            request.setRange(range.start(), range.end())
        }
        try {
            def s3Object = s3Client.getObject(request)
            return new AbortingS3ObjectInputStream(s3Object.objectContent)
        } catch (AmazonS3Exception e) {
            if (e.statusCode == 404) {
                throw new FileNotFoundException("S3 path $path")
            } else {
                throw e
            }
        }
    }

    /**
     * Wrap an S3ObjectInputStream to abort the S3Object when the stream is closed.
     *
     * The S3ObjectInputStream will complain at the WARN level when a stream is
     * closed that hasn't been fully read. As we will occasionally open a stream
     * just to read the header data for an image but want to deal in generic
     * InputStreams that don't support the AWS SDK specific abort() operation,
     * this class will always abort the stream on close to avoid the warning.
     *
     * TODO Find a way of requesting only the required image header byte range
     */
    @Slf4j
    static private class AbortingS3ObjectInputStream extends FilterInputStream {

        private S3ObjectInputStream inputStream

        AbortingS3ObjectInputStream(S3ObjectInputStream inputStream) {
            super(inputStream)
            this.inputStream = inputStream
        }

        @Override
        void close() throws IOException {
            def available = inputStream.delegateStream.available()
            if (available > 0) {
                log.debug('Closing S3ObjectInputStream with {} bytes available', available)
                inputStream.abort()
            }
            super.close()
        }
    }

    @Override
    boolean stored(String uuid) {
        return s3Client.doesObjectExist(bucket, createOriginalPathFromUUID(uuid))
    }

    @Override
    boolean thumbnailExists(String uuid, String type) {
        return s3Client.doesObjectExist(bucket, createThumbLargePathFromUUID(uuid, type))
    }

    @Override
    boolean tileExists(String uuid, int x, int y, int z) {
        return s3Client.doesObjectExist(bucket, createTilesPathFromUUID(uuid, x, y, z))
    }

    @Override
    void storeTileZipInputStream(String uuid, String zipInputFileName, String contentType, long length = 0, ZipInputStream zipInputStream) {
        def path = FilenameUtils.normalize(createTilesPathFromUUID(uuid) + '/' + zipInputFileName)
        zipInputStream.withStream { stream ->
            s3Client.putObject(bucket, path, stream, generateMetadata(contentType, null, length))
        }
    }

    long consumedSpace(String uuid) {
        return getConsumedSpaceInternal(storagePathStrategy().createPathFromUUID(uuid, ''))
    }

    @Override
    boolean deleteStored(String uuid) {

        walkPrefix(storagePathStrategy().createPathFromUUID(uuid, '')) { S3ObjectSummary s3ObjectSummary ->
            s3Client.deleteObject(bucket, s3ObjectSummary.key)
        }

        AuditService.submitLog(uuid, "Image deleted from store", "N/A")

        return true
    }

    private long getConsumedSpaceInternal(String prefix) {
        ObjectListing objectListing = null
        long size = 0
        List<String> extraPrefixes = []
        def more = true
        while (more) {

            objectListing = (objectListing == null) ? s3Client.listObjects(bucket, prefix) : s3Client.listNextBatchOfObjects(objectListing)

            size += (Long) (objectListing.objectSummaries.sum { S3ObjectSummary o -> o.size } ?: 0L)

            extraPrefixes += objectListing.commonPrefixes

            more = objectListing.isTruncated()
        }

        return size + (Long)((extraPrefixes.sum { getConsumedSpaceInternal(it) }) ?: 0L)
    }

    private <T> List<T> walkPrefix(String prefix, Closure<T> f) {
        ObjectListing objectListing = null
        List<T> results = []
        List<String> extraPrefixes = []
        def more = true
        while (more) {
            objectListing = (objectListing == null) ? s3Client.listObjects(bucket, prefix) : s3Client.listNextBatchOfObjects(objectListing)

            results += objectListing.objectSummaries.each(f)

            extraPrefixes += objectListing.commonPrefixes

            more = objectListing.isTruncated()
        }

        results.addAll((List<T>)extraPrefixes.collectMany { String extraPrefix -> walkPrefix(extraPrefix, f) })
        return results
    }

    StoragePathStrategy storagePathStrategy() {
        new DefaultStoragePathStrategy(prefix ?: '', true, false)
    }

    @Override
    ByteSinkFactory thumbnailByteSinkFactory(String uuid) {
        byteSinkFactory(uuid, [imageType: 'thumbnail'])
    }

    @Override
    ByteSinkFactory tilerByteSinkFactory(String uuid) {
        byteSinkFactory(uuid, [imageType: 'tile'], 'tms')
    }

    ByteSinkFactory byteSinkFactory(String uuid, Map<String, String> tags, String... prefixes) {
        return new S3ByteSinkFactory(s3Client, storagePathStrategy(), bucket, uuid, tags, prefixes)
    }

    @Override
    void storeAnywhere(String uuid, InputStream stream, String relativePath, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        def path = storagePathStrategy().createPathFromUUID(uuid, relativePath)
        storeInternal(stream, path, contentType, contentDisposition, length, [:])
    }

    private storeInternal(InputStream stream, String absolutePath, String contentType, String contentDisposition, Long length, Map<String, String> tags) {
        def client = s3Client
        try {
            def result = stream.withStream {
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, absolutePath, it, generateMetadata(contentType, contentDisposition, length))
                if (tags) {
                    def tagSet = tags.collect { new Tag(it.key, it.value) }
                    putObjectRequest.setTagging(new ObjectTagging(tagSet))
                }
                client.putObject(putObjectRequest)
            }
            log.debug("Uploaded {} to S3 {}:{}} with result etag {}}", absolutePath, region, bucket, result.ETag)
        } catch (AmazonS3Exception exception) {
            log.warn 'An amazon S3 exception was caught while storing input stream', exception
            throw new RuntimeException(exception)
        } catch (AmazonClientException exception) {
            log.warn 'An amazon client exception was caught while storing input stream', exception
            throw new RuntimeException(exception)
        }
    }

    @Override
    void migrateTo(String uuid, String contentType, StorageOperations destination) {
        def basePath = createBasePathFromUUID(uuid)
        walkPrefix(basePath) { S3ObjectSummary s3ObjectSummary ->
            def s3Object = s3Client.getObject(bucket, s3ObjectSummary.key)
            destination.storeAnywhere(uuid, s3Object.objectContent, s3ObjectSummary.key - basePath, s3Object.objectMetadata.contentType, s3Object.objectMetadata.contentDisposition, s3ObjectSummary.size)
        }
    }

    @Override
    long storedLength(String path) throws FileNotFoundException {
        try {
            def metadata = s3Client.getObjectMetadata(bucket, path)
            return metadata.contentLength
        } catch (AmazonS3Exception e) {
            if (e.statusCode == 404) {
                throw new FileNotFoundException("S3 path $path")
            } else {
                throw e
            }
        }
    }

    @Override
    ImageInfo originalImageInfo(String uuid) {
        def path = createOriginalPathFromUUID(uuid)
        return imageInfoInternal(path).tap { it.imageIdentifier = uuid }
    }

    @Override
    ImageInfo thumbnailImageInfo(String uuid, String type) {
        def path = createThumbLargePathFromUUID(uuid, type)
        return imageInfoInternal(path).tap { it.imageIdentifier = uuid }
    }

    @Override
    ImageInfo tileImageInfo(String uuid, int x, int y, int z) {
        def path = createTilesPathFromUUID(uuid, x, y, z)
        return imageInfoInternal(path).tap { it.imageIdentifier = uuid }
    }

    private ImageInfo imageInfoInternal(String path) {
        try {
            def metadata = s3Client.getObjectMetadata(bucket, path)
            def contentLength = metadata.contentLength
            return new ImageInfo(
                    exists: true,
                    length: contentLength,
                    etag: metadata.ETag,
                    lastModified: metadata.lastModified,
                    contentType: metadata.contentType,
                    extension: FilenameUtils.getExtension(path),
                    redirectUri: supportsRedirect ? redirectLocation(path) : null,
                    inputStreamSupplier: { Range range -> inputStream(path, range ?: Range.emptyRange(contentLength)) }
            )
        } catch (AmazonS3Exception e) {
            if (e.statusCode == 404) {
                return new ImageInfo(exists: false)
            } else {
                throw e
            }
        }
    }

    @Override
    String toString() {
        "S3StorageOperations: $region:$bucket:${prefix ?: ''}"
    }
}
