package au.org.ala.images

import grails.artefact.Interceptor
import grails.core.GrailsApplication
import groovy.util.logging.Slf4j

import java.util.Locale

/**
 * Interceptor to apply global no-cache headers for HTML pages and non-cacheable web services.
 */
@Slf4j
class CacheControlInterceptor implements Interceptor {

    GrailsApplication grailsApplication

    private static final List<String> IMAGE_CACHEABLE_ACTIONS = ['proxyImage', 'proxyImageThumbnail', 'proxyImageThumbnailType', 'proxyImageTile', 'getOriginalFile']
    private static final List<String> IIIF_CACHEABLE_ACTIONS = ['renderImage', 'info']
    private static final List<String> GZIP_CONTENT_TYPES = ['application/gzip', 'application/x-gzip']

    CacheControlInterceptor() {
        matchAll()
    }

    boolean before() {
        if (!isNoCacheInterceptorEnabled()) {
            return true
        }

        if (isCacheableAction() || hasNoCacheOptOutAnnotation()) {
            return true
        }

        String uri = request.requestURI
        String contextPath = request.contextPath ?: ""
        boolean isWebService = isWebServiceRequest(uri, contextPath)

        // Apply early for WS endpoints because some actions render/commit before after() executes.
        if (isWebService) {
            applyNoCacheHeaders()
        }

        if (hasNoCacheAnnotation()) {
            applyNoCacheHeaders()
        }
        true
    }

    boolean after() {
        if (!isNoCacheInterceptorEnabled()) {
            return true
        }

        if (isCacheableAction() || hasNoCacheOptOutAnnotation()) {
            return true
        }

        boolean noCacheAnnotated = hasNoCacheAnnotation()

        if (response.isCommitted()) {
            if (!noCacheAnnotated) {
                log.debug("Skipping no-cache headers for committed response to ${request.method} ${request.requestURI}")
            }
            return true
        }

        // response.status may be 0 (servlet default, treated as 200) or an explicit status code.
        // Apply no-cache logic for all non-error responses.
        int status = response.status
        if (status == 0 || status < 400) {
            String contentType = resolveEffectiveContentType()
            String uri = request.requestURI
            String contextPath = request.contextPath ?: ""
            String contentDisposition = response.getHeader('Content-disposition')

            boolean isNoCacheContentType = isNoCacheContentType(contentType)
            boolean isGzippedCsv = isGzippedCsv(contentType, contentDisposition)
            boolean isWebService = isWebServiceRequest(uri, contextPath)
            boolean inferNoCacheFromMissingContentType = isLikelyNoCacheWhenContentTypeMissing(
                    contentType,
                    isWebService,
                    contentDisposition,
                    request.getHeader('Accept'),
                    modelAndView?.viewName
            )

            if (noCacheAnnotated || isNoCacheContentType || isGzippedCsv || isWebService || inferNoCacheFromMissingContentType) {
                log.info("Applying no-cache headers for response to ${request.method} ${uri} with content type '${contentType}' and content disposition '${contentDisposition}'")
                applyNoCacheHeaders()
            }
        }
        true
    }

    protected String resolveEffectiveContentType() {
        String responseContentType = response?.contentType
        String viewContentType = modelAndView?.view?.contentType
        normaliseContentType(responseContentType ?: viewContentType)
    }

    protected static boolean isLikelyNoCacheWhenContentTypeMissing(String contentType,
                                                                    boolean isWebService,
                                                                    String contentDisposition,
                                                                    String acceptHeader,
                                                                    String viewName) {
        if (contentType) {
            return false
        }

        if (isWebService) {
            return true
        }

        if (isAttachmentContentDisposition(contentDisposition)) {
            return false
        }

        if (acceptsNoCacheContentType(acceptHeader)) {
            return true
        }

        viewName != null
    }

    protected static boolean isWebServiceRequest(String uri, String contextPath) {
        uri?.startsWith(contextPath + '/ws/') || uri == (contextPath + '/ws')
    }

    protected static boolean isAttachmentContentDisposition(String contentDisposition) {
        contentDisposition?.toLowerCase(Locale.ENGLISH)?.contains('attachment')
    }

    protected static boolean acceptsNoCacheContentType(String acceptHeader) {
        if (!acceptHeader) {
            return false
        }

        List<String> acceptedTypes = acceptHeader
                .toLowerCase(Locale.ENGLISH)
                .split(',')
                .collect { it?.split(';')?.first()?.trim() }

        acceptedTypes.any { acceptedType ->
            acceptedType in ['text/html', 'application/xhtml+xml', 'application/json', 'text/json', 'application/xml', 'text/xml'] ||
                    acceptedType?.endsWith('+json') ||
                    acceptedType?.endsWith('+xml')
        }
    }

    protected static String normaliseContentType(String contentType) {
        contentType?.toLowerCase(Locale.ENGLISH)?.split(';')?.first()?.trim()
    }

    protected static boolean isNoCacheContentType(String contentType) {
        if (!contentType) {
            return false
        }

        contentType in ['text/html', 'application/xhtml+xml', 'application/json', 'text/json', 'application/xml', 'text/xml'] ||
                contentType.endsWith('+json') ||
                contentType.endsWith('+xml')
    }

    protected static boolean isGzippedCsv(String contentType, String contentDisposition) {
        boolean isGzip = contentType in GZIP_CONTENT_TYPES
        boolean isCsvGzipAttachment = contentDisposition?.toLowerCase(Locale.ENGLISH)?.contains('.csv.gz')
        isGzip && isCsvGzipAttachment
    }

    protected boolean isNoCacheInterceptorEnabled() {
        Boolean enabled = grailsApplication?.config?.getProperty('images.nocacheInterceptor.enabled', Boolean, null)
        isNoCacheEnabled(enabled)
    }

    protected static boolean isNoCacheEnabled(Boolean enabled) {
        enabled == null || enabled
    }

    protected boolean isCacheableAction() {
        (controllerName == 'image' && actionName in IMAGE_CACHEABLE_ACTIONS) ||
                (controllerName == 'iiif' && actionName in IIIF_CACHEABLE_ACTIONS)
    }

    protected boolean hasNoCacheOptOutAnnotation() {
        Class controllerClass = resolveControllerClass()
        isNoCacheOptOut(controllerClass, actionName)
    }

    protected boolean hasNoCacheAnnotation() {
        Class controllerClass = resolveControllerClass()
        isNoCache(controllerClass, actionName)
    }

    protected Class resolveControllerClass() {
        def controllerArtefact = grailsApplication?.getArtefactByLogicalPropertyName('Controller', controllerName)
        controllerArtefact?.clazz as Class
    }

    protected static boolean isNoCacheOptOut(Class controllerClass, String actionName) {
        if (!controllerClass) {
            return false
        }
        if (controllerClass.getAnnotation(NoCacheOptOut)) {
            return true
        }
        if (!actionName) {
            return false
        }

        def actionMethod = controllerClass.declaredMethods.find { it.name == actionName }
        actionMethod?.getAnnotation(NoCacheOptOut) != null
    }

    protected static boolean isNoCache(Class controllerClass, String actionName) {
        if (!controllerClass) {
            return false
        }
        if (controllerClass.getAnnotation(NoCache)) {
            return true
        }
        if (!actionName) {
            return false
        }

        def actionMethod = controllerClass.declaredMethods.find { it.name == actionName }
        actionMethod?.getAnnotation(NoCache) != null
    }

    /**
     * Applies a full set of no-cache headers directly on the servlet response.
     * Equivalent to the cache-headers plugin's nocache() method, but safe to call
     * from an Interceptor (which does not have the plugin mixin available).
     * Headers chosen to prevent caching by browsers, CDNs (CloudFront, Fastly, etc.)
     * and any intermediate proxies.
     */
    protected void applyNoCacheHeaders() {
        // HTTP/1.1 – instructs every cache not to store or serve a cached copy
        header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        // HTTP/1.0 backwards compatibility
        header('Pragma', 'no-cache')
        // Expire immediately (epoch 0) for HTTP/1.0 proxies
        header('Expires', 0)
        // Tell CDNs / downstream caches to vary on these request headers
        header('Vary', 'Accept, Origin')
        // Security headers (belt-and-braces alongside no-cache)
        header('X-Content-Type-Options', 'nosniff')
        header('X-Frame-Options', 'SAMEORIGIN')
    }

    void afterView() {
        // no-op
    }
}
