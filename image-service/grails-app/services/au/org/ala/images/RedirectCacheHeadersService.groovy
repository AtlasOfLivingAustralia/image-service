package au.org.ala.images

import org.springframework.beans.factory.annotation.Value

import javax.servlet.http.HttpServletResponse

/**
 * Cache headers for 302 redirects to storage, shared by ImageController and IiifController.
 * These describe the redirect, not the image bytes, so no ETag/Last-Modified and never "never expires":
 * the target may move or (if presigned) expire.
 */
class RedirectCacheHeadersService {

    static transactional = false

    static final String NO_STORE = 'no-store, no-cache, must-revalidate'

    @Value('${images.cache.headers:true}')
    boolean cacheHeaders = true

    @Value('${images.redirect.cache.maxAgeSeconds:3600}')
    long redirectMaxAgeSeconds = 3600

    @Value('${images.redirect.cache.presignedMaxAgeSeconds:300}')
    long presignedRedirectMaxAgeSeconds = 300

    // never let a cached redirect be used this close to its presigned URL expiring
    @Value('${images.redirect.cache.expirySafetyMarginSeconds:60}')
    long redirectExpirySafetyMarginSeconds = 60

    /**
     * @return the Cache-Control value for a redirect to imageInfo.redirectUri, or null to send none
     */
    String cacheControl(ImageInfo imageInfo, boolean noCache) {
        long maxAge
        boolean shared
        if (imageInfo.redirectExpires) {
            long remaining = (imageInfo.redirectExpires.time - System.currentTimeMillis()).intdiv(1000L) - redirectExpirySafetyMarginSeconds
            maxAge = Math.min(presignedRedirectMaxAgeSeconds, remaining)
            shared = false
        } else {
            maxAge = redirectMaxAgeSeconds
            shared = true
        }
        if (noCache || maxAge <= 0) {
            return NO_STORE
        }
        cacheHeaders ? "${shared ? 'public' : 'private'}, max-age=${maxAge}".toString() : null
    }

    void apply(HttpServletResponse response, ImageInfo imageInfo, boolean noCache) {
        String value = cacheControl(imageInfo, noCache)
        if (value == NO_STORE) {
            response.setHeader('Cache-Control', NO_STORE)
            response.setHeader('Pragma', 'no-cache')
            response.setDateHeader('Expires', 0)
        } else if (value) {
            response.setHeader('Cache-Control', value)
        }
    }
}
