package au.org.ala.images.storage

import au.org.ala.images.AuditService
import au.org.ala.images.DefaultStoragePathStrategy
import au.org.ala.images.ImageInfo
import au.org.ala.images.Range
import au.org.ala.images.S3ByteSinkFactory
import au.org.ala.images.StoragePathStrategy
import au.org.ala.images.util.ByteSinkFactory
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.github.benmanes.caffeine.cache.RemovalCause
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import org.slf4j.event.Level
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.metrics.LoggingMetricPublisher
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.DefaultRetryStrategy
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.crt.S3CrtConnectionHealthConfiguration
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration
import software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest as V2GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import software.amazon.awssdk.services.s3.model.CopyObjectRequest as V2CopyObjectRequest
import software.amazon.awssdk.services.s3.model.MetadataDirective
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest
import software.amazon.awssdk.services.s3.S3Utilities
import software.amazon.awssdk.services.s3.model.GetUrlRequest
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody
import software.amazon.awssdk.transfer.s3.model.UploadRequest

import java.time.Duration
import java.util.function.Consumer
import com.google.common.annotations.VisibleForTesting
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
    // When true, explicitly attach the S3 canned Private ACL on upload metadata.
    // When false (and publicRead is also false), no ACL header will be attached, allowing
    // the bucket policy or default object ACLs to apply implicitly.
    boolean privateAcl
    boolean redirect
    String cloudfrontDomain

    boolean pathStyleAccess
    String hostname = ''

    // TODO configure connection parameters via config
    private static final int maxConnections = Integer.getInteger('au.org.ala.images.s3.max.connections', 500)
    private static final int maxErrorRetry = Integer.getInteger('au.org.ala.images.s3.max.retry', 3)
    private static final int apiCallAttemptTimeout = Integer.getInteger('au.org.ala.images.s3.attempt.timeout', 2)
    private static final int apiCallTimeout = Integer.getInteger('au.org.ala.images.s3.call.timeout', 10)
    private static final boolean useCrtAsyncClient = Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.async.crt', 'true'))
    private static final int crtConnectionTimeout = Integer.getInteger('au.org.ala.images.s3.async.crt.connection.timeout', 2)
    private static final boolean publishCloudwatchMetrics = Boolean.parseBoolean(System.getProperty('au.org.ala.images.s3.cloudwatch.metrics', 'false'))

    // TODO configure cache parameters via config
    private static final LoadingCache<CacheKey, S3Client> s3ClientCache = Caffeine<String, S3Client>.from("maximumSize=10,refreshAfterWrite=1h").removalListener {
        CacheKey key, S3Client client, RemovalCause cause ->
        client?.close()
    }.build { CacheKey key ->
        return s3ClientCacheLoader(key)
    }

    private static final LoadingCache<CacheKey, S3AsyncClient> s3AsyncClientCache = Caffeine<String, S3AsyncClient>.from("maximumSize=10,refreshAfterWrite=1h").removalListener {
        CacheKey key, S3AsyncClient client, RemovalCause cause ->
            s3TransferManagerCache.invalidate(key) // also remove any associated transfer manager
            client?.close()
    }.build { CacheKey key ->
        return s3AsyncClientCacheLoader(key)
    }

    private static final LoadingCache<CacheKey, S3TransferManager> s3TransferManagerCache = Caffeine<String, S3TransferManager>.from("maximumSize=10,refreshAfterWrite=1h").removalListener {
        CacheKey key, S3TransferManager manager, RemovalCause cause ->
            manager?.close()
    }.build { CacheKey key ->
        return s3TransferManagerCacheLoader(key)
    }

    static final void clearS3ClientCache() {
        s3TransferManagerCache.invalidateAll()
        s3AsyncClientCache.invalidateAll()
        s3ClientCache.invalidateAll()
    }

    @TupleConstructor
    @EqualsAndHashCode
    @ToString
    private static final class CacheKey {
        String region
        boolean containerCredentials
        String accessKey
        String secretKey
        String bucket
        String hostname
        boolean pathStyleAccess
    }

    private CacheKey getCacheKeyObject() {
        return new CacheKey(region, containerCredentials, accessKey, secretKey, bucket, hostname, pathStyleAccess)
    }

    private String getCacheKey() {
        return "${region}:${accessKey}:${bucket}:${hostname}:${pathStyleAccess}"
    }

    @VisibleForTesting
    protected S3Client getS3Client() {
        final cacheKey = getCacheKeyObject()
        return s3ClientCache.get(cacheKey)
    }

    private static S3Client s3ClientCacheLoader(CacheKey key) {
        def containerCredentials = key.containerCredentials
        def accessKey = key.accessKey
        def secretKey = key.secretKey
        def region = key.region
        def bucket = key.bucket
        def hostname = key.hostname
        def pathStyleAccess = key.pathStyleAccess

        def credProvider = containerCredentials ? DefaultCredentialsProvider.create() : StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

        MetricPublisher metricsPub
        if (publishCloudwatchMetrics) {
            metricsPub = CloudWatchMetricPublisher.builder().namespace("image-service/S3").build()
        } else {
            metricsPub = LoggingMetricPublisher.create(Level.INFO, LoggingMetricPublisher.Format.PRETTY)
        }

        // Configure HTTP Client with max connections
        def httpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(maxConnections)

        // Configure Retry Policy
        def overrideConfig = ClientOverrideConfiguration.builder()
                .retryStrategy(DefaultRetryStrategy.standardStrategyBuilder().maxAttempts(maxErrorRetry).build())
                .addMetricPublisher(metricsPub)
                .apiCallAttemptTimeout(Duration.ofSeconds(apiCallAttemptTimeout))
                .apiCallTimeout(Duration.ofSeconds(apiCallTimeout))
                .build()

        def builder = S3Client.builder()
                .defaultsMode(DefaultsMode.AUTO)
                .credentialsProvider(credProvider)
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig)

        if (region) {
            builder = builder.region(Region.of(region))
        }
        if (hostname) {
            builder = builder.endpointOverride(URI.create(hostname))
        }
        if (pathStyleAccess) {
            builder = builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        }
        return builder.build()
    }

    @VisibleForTesting
    protected S3AsyncClient getS3AsyncClient() {
        final cacheKey = getCacheKeyObject()
        return s3AsyncClientCache.get(cacheKey)
    }

    private static S3AsyncClient s3AsyncClientCacheLoader(CacheKey key) {
        def containerCredentials = key.containerCredentials
        def accessKey = key.accessKey
        def secretKey = key.secretKey
        def region = key.region
        def bucket = key.bucket
        def hostname = key.hostname
        def pathStyleAccess = key.pathStyleAccess

        def credProvider = containerCredentials ? DefaultCredentialsProvider.create() : StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

        def client

        if (!useCrtAsyncClient) {
            log.info("Using standard S3AsyncClient builder for S3 access")
            // Metrics only available for the standard async client, the crt client does not support metrics publishing
            MetricPublisher metricsPub
            if (publishCloudwatchMetrics) {
                metricsPub = CloudWatchMetricPublisher.builder().namespace("image-service/S3").build()
            } else {
                metricsPub = LoggingMetricPublisher.create(Level.INFO, LoggingMetricPublisher.Format.PRETTY)
            }

            // Configure Retry Policy
            def overrideConfig = ClientOverrideConfiguration.builder()
                    .retryStrategy(DefaultRetryStrategy.standardStrategyBuilder().maxAttempts(maxErrorRetry).build())
                    .addMetricPublisher(metricsPub)
                    .apiCallAttemptTimeout(Duration.ofSeconds(apiCallAttemptTimeout))
                    .apiCallTimeout(Duration.ofSeconds(apiCallTimeout))
                    .build()
            def builder = S3AsyncClient.builder()
                    .credentialsProvider(credProvider)
                    .overrideConfiguration(overrideConfig)
                    .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(maxConnections))

            if (region) {
                builder = builder.region(Region.of(region))
            }
            if (hostname) {
                builder = builder.endpointOverride(URI.create(hostname))
            }
            if (pathStyleAccess) {
                builder = builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            }
            client = builder.build()
        } else {
            log.info("Using CRT S3AsyncClient builder for S3 access")

            def builder = S3AsyncClient.crtBuilder()
                    .httpConfiguration(
                            S3CrtHttpConfiguration.builder()
                                    .connectionTimeout(Duration.ofSeconds(crtConnectionTimeout))
                                    .build()
                    )
                    .credentialsProvider(credProvider)
                    .retryConfiguration(S3CrtRetryConfiguration.builder().numRetries(maxErrorRetry).build())
//                        .maxConcurrency(maxConnections)

            if (region) {
                builder = builder.region(Region.of(region))
            }
            if (hostname) {
                builder = builder.endpointOverride(URI.create(hostname))
            }
            if (pathStyleAccess) {
                builder = builder.forcePathStyle(pathStyleAccess)
            }
            client = builder.build()
        }
        return client
    }

    @VisibleForTesting
    protected S3TransferManager getS3TransferManager() {
        final cacheKey = getCacheKeyObject()
        // Ensure the underlying AsyncClient is kept alive in the cache whenever the TransferManager is requested
//        getS3AsyncClient(cacheKey)
        return s3TransferManagerCache.get(cacheKey)
    }

    private static S3TransferManager s3TransferManagerCacheLoader(CacheKey key) {
        def s3AsyncClient = s3AsyncClientCache.get(key)
        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build()
    }

    @VisibleForTesting
    protected S3Presigner getS3Presigner() {
            def credProvider = containerCredentials ? DefaultCredentialsProvider.create() : StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
            def builder = S3Presigner.builder().credentialsProvider(credProvider)
            if (region) {
                builder = builder.region(Region.of(region))
            }
            if (hostname) {
                builder = builder.endpointOverride(URI.create(hostname))
            }
            return builder.build()
    }

    @Override
    boolean isSupportsRedirect() {
        redirect
    }

    @Override
    URI redirectLocation(String path) {
        if (cloudfrontDomain) {
            // If a CloudFront domain is set, use that for redirects
            new URI("https://${cloudfrontDomain}/${path}")
        } else if (publicRead) {
            S3Utilities utils = s3Client.utilities()
            def url = utils.getUrl(GetUrlRequest.builder().bucket(bucket).key(path).build())
            url.toURI()
        } else {
            // Presign with AWS SDK v2 for 1 hour
            def presignReq = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofHours(1))
                    .getObjectRequest { b -> b.bucket(bucket).key(path) }
                    .build()
            def presigned = s3Presigner.withCloseable {
                it.presignGetObject(presignReq)
            }
            presigned.url().toURI()
        }
    }

    def updateACL() {
        walkPrefix(storagePathStrategy().basePath()) { Map objSummary ->
            final String path = (objSummary['key'] as String)
            try {
                // Update ACL if requested
                if (publicRead) {
                    final String bkt = this.bucket
                    final String pth = path
                    s3Client.putObjectAcl({ PutObjectAclRequest.Builder b -> b.bucket(bkt).key(pth).acl(ObjectCannedACL.PUBLIC_READ) } as Consumer<PutObjectAclRequest.Builder>)
                } else if (privateAcl) {
                    final String bkt = this.bucket
                    final String pth = path
                    s3Client.putObjectAcl({ PutObjectAclRequest.Builder b -> b.bucket(bkt).key(pth).acl(ObjectCannedACL.PRIVATE) } as Consumer<PutObjectAclRequest.Builder>)
                }

                // Update Cache-Control by copying object to itself with REPLACE
                def head = s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bucket).key(path) } as Consumer<HeadObjectRequest.Builder>)
                def cacheControl = null
                if (publicRead) cacheControl = 'public,s-maxage=31536000,max-age=31536000'
                else if (privateAcl) cacheControl = 'private,max-age=31536000'

                final String bkt2 = this.bucket
                final String pth2 = path
                s3Client.copyObject({ V2CopyObjectRequest.Builder b ->
                    b.copySource("${bkt2}/${pth2}")
                     .destinationBucket(bkt2)
                     .destinationKey(pth2)
                     .metadataDirective(MetadataDirective.REPLACE)
                     .contentType(head.contentType())
                     .cacheControl(cacheControl)
                } as Consumer<V2CopyObjectRequest.Builder>)
            } catch (S3Exception e) {
                log.error('Error updating ACL for {}, public: {}, privateAcl: {}, error: {}', path, publicRead, privateAcl, e.message)
            }
        }
    }

    // Removed v1 ObjectMetadata helper; headers are set directly via v2 PutObjectRequest

    @Override
    boolean verifySettings() {
        try {
            // Check bucket existence via headBucket
            final String bkt = this.bucket
            s3Client.headBucket({ HeadBucketRequest.Builder b -> b.bucket(bkt) } as Consumer<HeadBucketRequest.Builder>)
            // Put a tiny object then delete
            final String key = storagePathStrategy().basePath() + '/' + UUID.randomUUID().toString()
            final String ct = 'application/octet-stream'
            s3Client.putObject({ PutObjectRequest.Builder b -> b.bucket(bkt).key(key).contentType(ct) } as Consumer<PutObjectRequest.Builder>,
                    RequestBody.fromBytes(new byte[1]))
            s3Client.deleteObject({ DeleteObjectRequest.Builder b -> b.bucket(bkt).key(key) } as Consumer<DeleteObjectRequest.Builder>)
            return true
        } catch (S3Exception e) {
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
                final String bkt = this.bucket
                final String imgKey = imagePath
                def resp = s3Client.getObject({ V2GetObjectRequest.Builder b -> b.bucket(bkt).key(imgKey) } as Consumer<V2GetObjectRequest.Builder>)
                bytes = resp.withStream { it.bytes }
            } catch (S3Exception e) {
                if (e.statusCode() == 404) {
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
        try {
            def consumer = { V2GetObjectRequest.Builder b ->
                b.bucket(bucket).key(path)
                if (range != null && !range.empty) {
                    b.range("bytes=${range.start()}-${range.end()}")
                }
            } as Consumer<V2GetObjectRequest.Builder>
            def s3Object = s3Client.getObject(consumer)
            return s3Object
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw new FileNotFoundException("S3 path $path")
            } else {
                throw e
            }
        }
    }

    // v1 AbortingS3ObjectInputStream removed; v2 getObject returns a standard InputStream that can be closed directly.

    @Override
    boolean stored(String uuid) {
        def key = createOriginalPathFromUUID(uuid)
        try {
            final String bkt = this.bucket
            final String kk = key
            s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(kk) } as Consumer<HeadObjectRequest.Builder>)
            return true
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false
            throw e
        }
    }

    @Override
    boolean thumbnailExists(String uuid, String type) {
        def key = createThumbLargePathFromUUID(uuid, type)
        try {
            final String bkt = this.bucket
            final String kk = key
            s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(kk) } as Consumer<HeadObjectRequest.Builder>)
            return true
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false
            throw e
        }
    }

    @Override
    boolean tileExists(String uuid, int x, int y, int z) {
        def key = createTilesPathFromUUID(uuid, x, y, z)
        try {
            final String bkt = this.bucket
            final String kk = key
            s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(kk) } as Consumer<HeadObjectRequest.Builder>)
            return true
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return false
            throw e
        }
    }

    @Override
    void storeTileZipInputStream(String uuid, String zipInputFileName, String contentType, long length = 0, ZipInputStream zipInputStream) {
        def path = FilenameUtils.normalize(createTilesPathFromUUID(uuid) + '/' + zipInputFileName)
        zipInputStream.withStream { stream ->
            final String bkt = this.bucket
            final String pth = path
            final String ct = contentType
            s3Client.putObject(
                    { PutObjectRequest.Builder b -> b.bucket(bkt).key(pth).contentType(ct) } as Consumer<PutObjectRequest.Builder>,
                    RequestBody.fromInputStream(stream, length)
            )
        }
    }

    long consumedSpace(String uuid) {
        return getConsumedSpaceInternal(storagePathStrategy().createPathFromUUID(uuid, ''))
    }

    @Override
    boolean deleteStored(String uuid) {
        final String bkt = this.bucket
        walkPrefix(storagePathStrategy().createPathFromUUID(uuid, '')) { Map obj ->
            final String key = obj['key'] as String
            s3Client.deleteObject({ DeleteObjectRequest.Builder b -> b.bucket(bkt).key(key) } as Consumer<DeleteObjectRequest.Builder>)
        }
        AuditService.submitLog(uuid, "Image deleted from store", "N/A")
        return true
    }

    private long getConsumedSpaceInternal(String prefix) {
        long size = 0
        List<String> extraPrefixes = []
        final String bkt = this.bucket
        ListObjectsV2Iterable pages = s3Client.listObjectsV2Paginator({ ListObjectsV2Request.Builder b -> b.bucket(bkt).prefix(prefix).delimiter('/') } as Consumer<ListObjectsV2Request.Builder>)
        pages.each { page ->
            size += (Long) (page.contents().collect { it.size() }.sum() ?: 0L)
            extraPrefixes.addAll(page.commonPrefixes().collect { it.prefix() })
        }
        return size + (Long)((extraPrefixes.sum { getConsumedSpaceInternal(it) }) ?: 0L)
    }

    private <T> List<T> walkPrefix(String prefix, Closure<T> f) {
        List<T> results = []
        List<String> extraPrefixes = []
        final String bkt = this.bucket
        ListObjectsV2Iterable pages = s3Client.listObjectsV2Paginator({ ListObjectsV2Request.Builder b -> b.bucket(bkt).prefix(prefix).delimiter('/') } as Consumer<ListObjectsV2Request.Builder>)
        pages.each { page ->
            page.contents().each { obj ->
                Map summary = [key: obj.key(), size: obj.size()]
                results += f.call(summary)
            }
            extraPrefixes.addAll(page.commonPrefixes().collect { it.prefix() })
        }
        results.addAll((List<T>) extraPrefixes.collectMany { String extraPrefix -> walkPrefix(extraPrefix, f) })
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
        return new S3ByteSinkFactory(s3AsyncClient, s3TransferManager, storagePathStrategy(), bucket, uuid, tags, prefixes)
    }

    @Override
    void storeAnywhere(String uuid, InputStream stream, String relativePath, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        def path = storagePathStrategy().createPathFromUUID(uuid, relativePath)
        storeInternal(stream, path, contentType, contentDisposition, length, [:])
    }

    private storeInternal(InputStream stream, String absolutePath, String contentType, String contentDisposition, Long length, Map<String, String> tags) {
        try {
            stream.withStream { InputStream input ->
                def transferManager = getS3TransferManager()
                def blockingBody = BlockingOutputStreamAsyncRequestBody.builder().build()

                PutObjectRequest.Builder b = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(absolutePath)

                if (contentType) b.contentType(contentType)
                if (contentDisposition) b.contentDisposition(contentDisposition)

                // Cache-Control headers based on ACL flags (preserved behavior)
                if (publicRead) {
                    b.cacheControl('public,s-maxage=31536000,max-age=31536000')
                    b.acl(ObjectCannedACL.PUBLIC_READ)
                } else if (privateAcl) {
                    b.cacheControl('private,max-age=31536000')
                    b.acl(ObjectCannedACL.PRIVATE)
                }
                if (tags) {
                    def tagSet = tags.collect { k, v ->
                        Tag.builder().key(k).value(v).build()
                    }
                    if (tagSet) b.tagging(Tagging.builder().tagSet(tagSet).build())
                }

                def uploadReq = UploadRequest.builder()
                        .putObjectRequest(b.build())
                        .requestBody(blockingBody)
                        .build()

                def uploadFuture = transferManager.upload(uploadReq).completionFuture()

                blockingBody.outputStream().withStream { os ->
                    input.transferTo(os)
                }

                def result = uploadFuture.join()
                log.debug("Uploaded {} to S3 {}:{} with result etag {}", absolutePath, region, bucket, result.response().eTag())
            }
        } catch (Exception exception) {
            log.warn 'An exception was caught while storing input stream', exception
            throw new RuntimeException(exception)
        }
    }

    @Override
    void migrateTo(String uuid, String contentType, StorageOperations destination) {
        def basePath = createBasePathFromUUID(uuid)
        final String bkt = this.bucket
        walkPrefix(basePath) { Map obj ->
            final String k = obj['key'] as String
            def head = s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(k) } as Consumer<HeadObjectRequest.Builder>)
            def objectStream = s3Client.getObject({ V2GetObjectRequest.Builder b -> b.bucket(bkt).key(k) } as Consumer<V2GetObjectRequest.Builder>)
            destination.storeAnywhere(uuid, objectStream, k - basePath, head.contentType(), head.contentDisposition(), (obj['size'] as long))
        }
    }

    @Override
    long storedLength(String path) throws FileNotFoundException {
        try {
            final String bkt = this.bucket
            final String pth = path
            def head = s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(pth) } as Consumer<HeadObjectRequest.Builder>)
            return head.contentLength()
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
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
            final String bkt = this.bucket
            final String pth = path
            def head = s3Client.headObject({ HeadObjectRequest.Builder b -> b.bucket(bkt).key(pth) } as Consumer<HeadObjectRequest.Builder>)
            def contentLength = head.contentLength()
            Date lastMod = head.lastModified() != null ? Date.from(head.lastModified()) : null
            return new ImageInfo(
                    exists: true,
                    length: contentLength,
                    etag: head.eTag(),
                    lastModified: lastMod,
                    contentType: head.contentType(),
                    extension: FilenameUtils.getExtension(path),
                    redirectUri: supportsRedirect ? redirectLocation(path) : null,
                    inputStreamSupplier: { Range range -> inputStream(path, range ?: Range.emptyRange(contentLength)) }
            )
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return new ImageInfo(exists: false)
            } else {
                throw e
            }
        }
    }

    void clearTilesForImage(String uuid) {
        def basePath = createTilesPathFromUUID(uuid)
        final String bkt = this.bucket
        walkPrefix(basePath) { Map summary ->
            final String key = summary['key'] as String
            s3Client.deleteObject({ DeleteObjectRequest.Builder b -> b.bucket(bkt).key(key) } as Consumer<DeleteObjectRequest.Builder>)
        }
    }

    @Override
    String toString() {
        "S3StorageOperations: $region:$bucket:${prefix ?: ''}"
    }
}
