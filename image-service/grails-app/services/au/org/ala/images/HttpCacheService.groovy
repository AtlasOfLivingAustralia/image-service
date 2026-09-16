package au.org.ala.images

import grails.gorm.transactions.NotTransactional
import jakarta.servlet.http.HttpServletResponse

class HttpCacheService {

    private static final String HEADER_LAST_MODIFIED = 'Last-Modified'

    @NotTransactional
    void cache(HttpServletResponse response, boolean allow) {
        if (allow) {
            throw new IllegalArgumentException("Call to [cache] with [true] doesn't make sense; pass a map of cache settings")
        }

        response.setHeader('Cache-Control', 'no-cache, no-store')
        response.setDateHeader('Expires', System.currentTimeMillis() - 86_400_000L)
        response.setHeader('Pragma', 'no-cache')
    }

    @NotTransactional
    void cache(HttpServletResponse response, Map options) {
        Date now = new Date()
        Date expiresOn
        Long maxAge

        if (options.validFor != null) {
            maxAge = Math.max(0L, options.validFor as long)
            expiresOn = new Date(now.time + maxAge * 1000L)
        } else if (options.validUntil != null) {
            expiresOn = options.validUntil as Date
            maxAge = Math.max(0L, Math.round((expiresOn.time - now.time) / 1000d))
        } else if (options.neverExpires) {
            // Preserve the cache-headers plugin's definition of "never": one year.
            maxAge = 365L * 24 * 60 * 60
            expiresOn = new Date(now.time + maxAge * 1000L)
        }

        List<String> cacheControl = []
        if (options.store != null && !options.store) {
            cacheControl << 'no-store'
        }

        if (options.shared) {
            cacheControl << 'public'
            if (options.auth) {
                cacheControl << 'no-cache'
            }
        } else {
            cacheControl << 'private'
        }

        if (maxAge != null) {
            if (options.shared) {
                cacheControl << "s-maxage=${maxAge}"
            }
            cacheControl << "max-age=${maxAge}"
        }

        response.setHeader('Cache-Control', cacheControl.join(', '))
        if (expiresOn != null) {
            response.setDateHeader('Expires', expiresOn.time)
        }
        if (!response.containsHeader(HEADER_LAST_MODIFIED)) {
            response.setDateHeader(HEADER_LAST_MODIFIED, now.time)
        }
    }
}
