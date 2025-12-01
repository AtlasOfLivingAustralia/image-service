package au.org.ala.images

import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.storage.StorageOperations
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.io.ByteSource
import grails.gorm.transactions.NotTransactional
import groovy.util.logging.Slf4j
import org.grails.orm.hibernate.cfg.GrailsHibernateUtil
import org.springframework.beans.factory.annotation.Value

import javax.annotation.PostConstruct
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor

@Slf4j
class IiifImageService {

    @Value('${image.store.lookup.cache.iiifConfig:maximumSize=10000,expireAfterAccess=30m}')
    String iiifCacheConfig = 'maximumSize=10000,expireAfterAccess=30m'

    Cache<IiifCacheKey, ImageInfo> iiifCache

    @PostConstruct
    @NotTransactional
    def init() {
        iiifCache = Caffeine.from(iiifCacheConfig).build()
    }

    /**
     * Value type used as the cache key for IIIF render requests. Replaces Map-based keys
     * to provide stable equals/hashCode, lower allocation overhead and better type safety.
     */
    @TupleConstructor
    @EqualsAndHashCode
    @ToString
    static final class IiifCacheKey {
        final String identifier
        final IiifImageProcessor.Region region
        final IiifImageProcessor.Size size
        final IiifImageProcessor.Rotation rotation
        final IiifImageProcessor.Quality quality
        final IiifImageProcessor.Format format
    }

    static class IiifRenderResult {
        ImageInfo imageInfo
        String mime
        String etag
        Date lastModified

        Integer errorStatus
        String errorMessage

        boolean isError() { return errorStatus != null }
    }

    @NotTransactional
    IiifRenderResult render(String identifier, String region, String size, String rotationParam,
                            String quality, String format, boolean invalidate) {
        IiifRenderResult result = new IiifRenderResult()

        if (!identifier || !region || !size || !rotationParam || !quality || !format) {
            result.errorStatus = 400
            result.errorMessage = 'Missing required parameters'
            return result
        }

        IiifImageProcessor.Region iiifRegion
        try {
            iiifRegion = IiifImageProcessor.Region.parse(region)
        } catch (IllegalArgumentException e) {
            result.errorStatus = 400
            result.errorMessage = 'Invalid region parameter'
            return result
        }

        IiifImageProcessor.Size iiifSize
        try {
            iiifSize = IiifImageProcessor.Size.parse(size)
        } catch (IllegalArgumentException e) {
            result.errorStatus = 400
            result.errorMessage = 'Invalid size parameter'
            return result
        }

        IiifImageProcessor.Rotation iiifRotation
        try {
            iiifRotation = IiifImageProcessor.Rotation.parse(rotationParam)
        } catch (IllegalArgumentException e) {
            result.errorStatus = 400
            result.errorMessage = 'Invalid rotation parameter'
            return result
        }

        IiifImageProcessor.Quality iiifQuality
        try {
            iiifQuality = IiifImageProcessor.Quality.parse(quality)
        } catch (IllegalArgumentException e) {
            result.errorStatus = 400
            result.errorMessage = 'Invalid quality parameter'
            return result
        }

        IiifImageProcessor.Format iiifFormat
        try {
            iiifFormat = IiifImageProcessor.Format.parse(format)
        } catch (IllegalArgumentException e) {
            result.errorStatus = 400
            result.errorMessage = 'Unsupported format'
            return result
        }

        def iiifKey = new IiifCacheKey(identifier, iiifRegion, iiifSize, iiifRotation, iiifQuality, iiifFormat)
        if (invalidate) {
            iiifCache.invalidate(iiifKey)
        }
        iiifCache.get(iiifKey) { IiifCacheKey key ->
            cacheLoader(key.identifier, key.region, key.size, key.rotation, key.quality, key.format)
        }?.with { imageInfo ->
            if (imageInfo) {
                result.imageInfo = imageInfo
                result.mime = iiifFormat.mimeType
                result.lastModified = imageInfo.lastModified
                String baseEtag = imageInfo.etag
                result.etag = baseEtag + ':' + [region, size, rotationParam, quality, format].join('/')
                return result
            } else {
                result.errorStatus = 404
                result.errorMessage = 'Image not found'
                return result
            }
        }
    }

    private static ImageInfo cacheLoader(String identifier, IiifImageProcessor.Region region,
                                        IiifImageProcessor.Size size,
                                        IiifImageProcessor.Rotation rotation,
                                        IiifImageProcessor.Quality quality,
                                        IiifImageProcessor.Format format) {
        StorageOperations operations = null
        Image.withNewTransaction(readOnly: true) {
            def image = Image.findByImageIdentifier(identifier, [ cache: true, fetch: [ storageLocation: 'join' ] ])
            if (image) {
                operations = GrailsHibernateUtil.unwrapIfProxy(image.storageLocation).asStandaloneStorageOperations()
            }
        }
        
        if (!operations) {
            return null // don't cache this value
        }

        String type = region.canonical()+'_'+size.canonical()+'_'+rotation.canonical()+'_'+quality.canonical()+'_'+format.canonical()

        def imageInfo = operations.thumbnailImageInfo(identifier, type)

        if (!imageInfo.exists) {
            IiifImageProcessor iif = new IiifImageProcessor()

            def byteSource = new ByteSource() {
                @Override
                InputStream openStream() throws IOException {
                    return operations.originalInputStream(identifier, null)
                }
            }
            // this image-utils call doesn't support a bytesinkfactory, so we're doing it ourselves
            // TODO should thumbnail_+type come from the StoragePathStrategy?
            def result = operations.thumbnailByteSinkFactory(identifier).getByteSinkForNames('thumbnail_'+type).openBufferedStream().withStream { out ->
                iif.process(byteSource, region, size, rotation, quality, format, out)
            }
            // TODO Save an ImageThumbnail record for this generated thumbnail?
            Image.async.withNewTransaction {
                def image = Image.findByImageIdentifier(identifier)
                new ImageThumbnail(image: image, name: type, width: result.width, height: result.height,
                        isSquare: region.type == IiifImageProcessor.Region.Type.SQUARE).save(failOnError: true)
            }
            imageInfo = operations.thumbnailImageInfo(identifier, type)
        }

        return imageInfo.exists ? imageInfo : null
    }

}
