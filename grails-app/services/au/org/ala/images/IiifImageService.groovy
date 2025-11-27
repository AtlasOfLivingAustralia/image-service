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

    def imageStoreService

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

    /**
     * Invalidate a specific IIIF cache entry so that a subsequent render call regenerates it.
     * Parsing errors are ignored and will result in a no-op.
     */
    @NotTransactional
    void invalidateCacheEntry(String identifier, String region, String size, String rotationParam,
                              String quality, String format) {
        try {
            def iiifRegion = IiifImageProcessor.Region.parse(region)
            def iiifSize = IiifImageProcessor.Size.parse(size)
            def iiifRotation = IiifImageProcessor.Rotation.parse(rotationParam)
            def iiifQuality = IiifImageProcessor.Quality.parse(quality)
            def iiifFormat = IiifImageProcessor.Format.parse(format)
            iiifCache.invalidate(new IiifCacheKey(identifier, iiifRegion, iiifSize, iiifRotation, iiifQuality, iiifFormat))
        } catch (Exception ignored) {
            // ignore invalid params for invalidation
        }
    }

    private static IiifImageProcessor.Format applyFormat(String fmt) {
        if (fmt == null) return null
        switch (fmt.toLowerCase()) {
            case 'jpg':
            case 'jpeg': return IiifImageProcessor.Format.JPG
            case 'png': return IiifImageProcessor.Format.PNG
            case 'webp': return IiifImageProcessor.Format.WEBP
            default: return null
        }
    }

    private static byte[] encodeImage(BufferedImage img, String fmt) {
        def writers = ImageIO.getImageWritersByFormatName(fmt)
        if (!writers?.hasNext()) {
            if (fmt.equalsIgnoreCase('jpeg') || fmt.equalsIgnoreCase('jpg')) {
                return ImageUtils.imageToBytes(img)
            }
            throw new IllegalStateException("No ImageIO writer for format ${fmt}")
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream(ImageUtils.IMAGE_BUF_INIT_SIZE)
        ImageIO.write(img, fmt, baos)
        return baos.toByteArray()
    }

    private static IiifImageProcessor.Region applyRegion(String region) {

        if (!region || region == 'full') {
            return IiifImageProcessor.Region.full()
        } else if (region == 'square') {
            return IiifImageProcessor.Region.square()
        } else if (region.startsWith('pct:')) {
            String coords = region.substring('pct:'.length())
            def parts = coords.split(',')*.trim()
            if (parts.size() != 4) throw new IllegalArgumentException("pct region must have 4 parts")
            double x = parts[0] as double
            double y = parts[1] as double
            double w = parts[2] as double
            double h = parts[3] as double
            return IiifImageProcessor.Region.percent(x, y, w, h)
//                int px = Math.round((float)(x / 100d * src.width))
//                int py = Math.round((float)(y / 100d * src.height))
//                int pw = Math.round((float)(w / 100d * src.width))
//                int ph = Math.round((float)(h / 100d * src.height))
//                px = Math.max(0, Math.min(px, src.width-1))
//                py = Math.max(0, Math.min(py, src.height-1))
//                pw = Math.max(1, Math.min(pw, src.width - px))
//                ph = Math.max(1, Math.min(ph, src.height - py))
//                return src.getSubimage(px, py, pw, ph)
        } else {
            def parts = region.split(',')*.trim()
            if (parts.size() != 4) throw new IllegalArgumentException("absolute region must have 4 parts")
            int x = Math.max(0, parts[0] as int)
            int y = Math.max(0, parts[1] as int)
            int w = parts[2] as int
            int h = parts[3] as int
            return IiifImageProcessor.Region.absolute(x, y, w, h)
//                w = Math.max(1, Math.min(w, src.width - x))
//                h = Math.max(1, Math.min(h, src.height - y))
//                return src.getSubimage(x, y, w, h)
        }
    }

    private static IiifImageProcessor.Size applySize(String size) {
        boolean bestFit = false
        if (size.startsWith('!')) { bestFit = true; size = size.substring(1) }
        boolean upscaling = false
        if (size.startsWith('^')) { upscaling = true; size = size.substring(1) }

        if (!size || size == 'max') {
            return IiifImageProcessor.Size.max(upscaling)
        } else if (size.startsWith('pct:')) {
            double pct = (size.substring('pct:'.length()) as double) / 100d
            return IiifImageProcessor.Size.percent(pct, upscaling)
//                int w = Math.max(1, (int)Math.round(src.width * pct))
//                int h = Math.max(1, (int)Math.round(src.height * pct))
//                return ImageUtils.scale(src, w, h)
        } else {
            def parts = size.split(',')*.trim()
            if (parts.size() != 2) throw new IllegalArgumentException("Invalid size parameter")
            String sw = parts[0]
            String sh = parts[1]
            if (sw && sh) {
                int w = sw as int
                int h = sh as int
                if (bestFit) {
                    return IiifImageProcessor.Size.bestFit(w, h, upscaling)
                } else {
                    return IiifImageProcessor.Size.exact(w, h, upscaling)
                }
            } else if (sw) {
                int w = sw as int
                return IiifImageProcessor.Size.width(w, upscaling)
            } else if (sh) {
                int h = sh as int
                return IiifImageProcessor.Size.height(h, upscaling)
            } else {
                throw new IllegalArgumentException("Invalid size parameter")
            }
        }
    }

    private static IiifImageProcessor.Rotation applyRotation(String rotationParam) {
        boolean mirror = false
        String r = rotationParam
        if (r.startsWith('!')) { mirror = true; r = r.substring(1) }

        double angle
        try {
            angle = Double.parseDouble(r)
            int intAngle = (int)Math.round(angle)
            if (intAngle % 360 == 0) {
                return IiifImageProcessor.Rotation.none()
            }
            new IiifImageProcessor.Rotation(mirror, angle)
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid rotation parameter")
        }
//        BufferedImage img = src
//        if (mirror) {
//            java.awt.geom.AffineTransform tx = new java.awt.geom.AffineTransform()
//            tx.scale(-1.0d, 1.0d)
//            tx.translate(-img.width, 0)
//            java.awt.image.AffineTransformOp op = new java.awt.image.AffineTransformOp(tx, java.awt.image.AffineTransformOp.TYPE_NEAREST_NEIGHBOR)
//            BufferedImage dest = new BufferedImage(img.width, img.height, img.type)
//            op.filter(img, dest)
//            img = dest
//        }
//        int intAngle = (int)Math.round(angle)
//        if (intAngle % 360 == 0) return img
//        return ImageUtils.rotateImage(img, intAngle)
    }
}
