package au.org.ala.images

import grails.artefact.Interceptor
import grails.core.GrailsApplication

import java.util.Locale

/**
 * Interceptor to apply global no-cache headers for HTML pages and non-cacheable web services.
 */
class CacheControlInterceptor implements Interceptor {

    GrailsApplication grailsApplication

    private static final List<String> IMAGE_CACHEABLE_ACTIONS = ['proxyImage', 'proxyImageThumbnail', 'proxyImageThumbnailType', 'proxyImageTile', 'getOriginalFile']
    private static final List<String> IIIF_CACHEABLE_ACTIONS = ['renderImage', 'info']
    private static final List<String> GZIP_CONTENT_TYPES = ['application/gzip', 'application/x-gzip']

    CacheControlInterceptor() {
        matchAll()
    }

    boolean before() {
        true
    }

    boolean after() {
        if (isCacheableAction() || hasNoCacheOptOutAnnotation()) {
            return true
        }

        if (response.status == 200) {
            String contentType = normaliseContentType(response.contentType)
            String uri = request.requestURI
            String contextPath = request.contextPath ?: ""
            String contentDisposition = response.getHeader('Content-disposition')

            boolean isNoCacheContentType = isNoCacheContentType(contentType)
            boolean isGzippedCsv = isGzippedCsv(contentType, contentDisposition)
            // Check if the URI starts with /ws/ (accounting for context path)
            boolean isWebService = uri.startsWith(contextPath + '/ws/') || uri == (contextPath + '/ws')

            if (isNoCacheContentType || isGzippedCsv || isWebService) {
                // Apply no-cache headers
                nocache()
                // Ensure manual overrides in case nocache() is not sufficient in after() context
                response.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate')
                response.setHeader('Pragma', 'no-cache')
                response.setDateHeader('Expires', 0)

                response.setHeader("Vary", "Accept, Origin")
                // Apply security headers as recommended
                response.setHeader("X-Content-Type-Options", "nosniff")
                response.setHeader("X-Frame-Options", "SAMEORIGIN")
            }
        }
        true
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

    protected boolean isCacheableAction() {
        (controllerName == 'image' && actionName in IMAGE_CACHEABLE_ACTIONS) ||
                (controllerName == 'iiif' && actionName in IIIF_CACHEABLE_ACTIONS)
    }

    protected boolean hasNoCacheOptOutAnnotation() {
        Class controllerClass = resolveControllerClass()
        isNoCacheOptOut(controllerClass, actionName)
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

    void afterView() {
        // no-op
    }
}
