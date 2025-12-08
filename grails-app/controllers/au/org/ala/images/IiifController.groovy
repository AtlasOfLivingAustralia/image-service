package au.org.ala.images

import au.org.ala.images.metrics.MetricsSupport
import grails.converters.JSON
import grails.core.GrailsApplication
import groovy.util.logging.Slf4j
import au.org.ala.plugins.openapi.Path
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.enums.ParameterIn
import io.swagger.v3.oas.annotations.headers.Header
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import javax.ws.rs.Produces

import javax.servlet.http.HttpServletResponse
import org.apache.commons.io.IOUtils

/**
 * IIIF Image API 3.0 controller (Level 2)
 *
 * Endpoints:
 *  - /iiif/{identifier}/info.json
 *  - /iiif/{identifier}/{region}/{size}/{rotation}/{quality}.{format}
 */
@Slf4j
class IiifController implements MetricsSupport {

    static responseFormats = ['json']

    GrailsApplication grailsApplication
    IiifImageService iiifImageService

    private static final String CONTEXT = 'http://iiif.io/api/image/3/context.json'
    private static final String PROTOCOL = 'http://iiif.io/api/image'

    /**
     * GET /iiif/{identifier}/info.json
     */
    @Operation(
            method = "GET",
            summary = "IIIF Image information (info.json)",
            description = "Returns an IIIF Image API 3.0 ImageService document describing the available image, sizes, tiles and formats for the given identifier.",
            parameters = [
                    @Parameter(name = "identifier", in = ParameterIn.PATH, required = true,
                            description = "IIIF image identifier",
                            schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(responseCode = "200", description = "ImageService info",
                            content = [@Content(mediaType = "application/json")],
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Origin', description = 'CORS header', schema = @Schema(type = 'string')),
                                    @Header(name = 'Access-Control-Allow-Methods', description = 'CORS header', schema = @Schema(type = 'string')),
                                    @Header(name = 'Access-Control-Allow-Headers', description = 'CORS header', schema = @Schema(type = 'string'))
                            ]),
                    @ApiResponse(responseCode = "400", description = "Missing identifier"),
                    @ApiResponse(responseCode = "404", description = "Image not found")
            ],
            tags = ["IIIF"]
    )
    @Path("/iiif/{identifier}/info.json")
    @Produces("application/json")
    def info() {
        return recordTime('iiif.info', 'Time to serve IIIF info.json') {
            incrementCounter('iiif.info.request', 'IIIF info.json requests')

            if (!isIiifEnabled()) {
                // Feature flag: return 404 for all IIIF paths when disabled
                incrementCounter('iiif.info.disabled', 'IIIF info requests when disabled')
                response.status = 404
                render(text: 'Not found', contentType: 'text/plain')
                return
            }
            String identifier = params.identifier
            if (!identifier) {
                incrementCounter('iiif.info.error', 'IIIF info errors', [reason: 'missing_identifier'])
                response.status = 400
                render(text: 'Missing identifier', contentType: 'text/plain')
                return
            }

            Image image = Image.findByImageIdentifier(identifier)
            if (!image) {
                incrementCounter('iiif.info.notfound', 'IIIF info not found')
                enableCors()
                response.status = 404
                render(text: 'Image not found', contentType: 'text/plain')
                return
            }

            String base = getBaseUrl()
            String serviceId = "${base}/iiif/${identifier}"

            Map<String, Object> body = [
                    '@context': CONTEXT,
                    id       : serviceId,
                    type     : 'ImageService3',
                    protocol : PROTOCOL,
                    width    : image.width ?: 0,
                    height   : image.height ?: 0,
                    profile  : 'level2', // included for compatibility with some clients
                    extraFormats : ['jpg','png','webp'],
                    extraQualities: ['default','color','gray','bitonal'],
            ]

            // Tiling info from existing TMS tiles if present
            if (image.zoomLevels && image.zoomLevels > 0) {
                List<Integer> scaleFactors = computeScaleFactors(image)
                body.tiles = [[width: 256, height: 256, scaleFactors: scaleFactors]]
            }

            incrementCounter('iiif.info.success', 'Successful IIIF info responses')
            enableCors()
            withCacheHeaders {
                render(body as JSON)
            }
        }
    }

    /**
     * GET /iiif/{identifier}/{region}/{size}/{rotation}/{quality}.{format}
     */
    @Operation(
            method = "GET",
            summary = "Render IIIF image",
            description = "Renders an image, region and size variant according to the IIIF Image API 3.0. Supports JPEG, PNG and WebP output.",
            parameters = [
                    @Parameter(name = "identifier", in = ParameterIn.PATH, required = true, description = "IIIF image identifier", schema = @Schema(implementation = String)),
                    @Parameter(name = "region", in = ParameterIn.PATH, required = true, description = "Region of the image to extract. Use 'full', 'square' or 'x,y,w,h' or 'pct:x,y,w,h' as per IIIF or custom extension 'ar:w,h' for a center cropped maximal region of the given aspect ratio."),
                    @Parameter(name = "size", in = ParameterIn.PATH, required = true, description = "Target size. Examples: 'full', 'max', 'w,', ',h', 'w,h', 'pct:n' as per IIIF."),
                    @Parameter(name = "rotation", in = ParameterIn.PATH, required = true, description = "Rotation to apply. Examples: '0', '90', '!0' for mirroring as per IIIF."),
                    @Parameter(name = "quality", in = ParameterIn.PATH, required = true, description = "Quality to apply. Supported: 'default', 'color', 'gray', 'bitonal'."),
                    @Parameter(name = "format", in = ParameterIn.PATH, required = true, description = "Output format/extension.", schema = @Schema(allowableValues = ["jpg", "png", "webp"]))
            ],
            responses = [
                    @ApiResponse(responseCode = "200", description = "Rendered image",
                            content = [
                                    @Content(mediaType = "image/jpeg"),
                                    @Content(mediaType = "image/png"),
                                    @Content(mediaType = "image/webp")
                            ],
                            headers = [
                                    @Header(name = 'ETag', description = 'Entity tag for cache validation', schema = @Schema(type = 'string')),
                                    @Header(name = 'Last-Modified', description = 'Last modified date', schema = @Schema(type = 'string')),
                                    @Header(name = 'Access-Control-Allow-Origin', description = 'CORS header', schema = @Schema(type = 'string')),
                                    @Header(name = 'Access-Control-Allow-Methods', description = 'CORS header', schema = @Schema(type = 'string')),
                                    @Header(name = 'Access-Control-Allow-Headers', description = 'CORS header', schema = @Schema(type = 'string'))
                            ]),
                    @ApiResponse(responseCode = "302", description = "Redirect to remote image location when applicable"),
                    @ApiResponse(responseCode = "304", description = "Not Modified (cache validation)"),
                    @ApiResponse(responseCode = "400", description = "Invalid parameters"),
                    @ApiResponse(responseCode = "404", description = "Image or resource not found")
            ],
            tags = ["IIIF"]
    )
    @Path("/iiif/{identifier}/{region}/{size}/{rotation}/{quality}.{format}")
    @Produces(["image/jpeg", "image/png", "image/webp"])
    def renderImage() {
        return recordTime('iiif.render', 'Time to render IIIF image', [format: params.format ?: 'unknown', region: params.region ?: 'unknown', size: params.size ?: 'unknown']) {
            incrementCounter('iiif.render.request', 'IIIF render requests', [format: params.format ?: 'unknown'])

            if (!isIiifEnabled()) {
                // Feature flag: return 404 for all IIIF paths when disabled
                incrementCounter('iiif.render.disabled', 'IIIF render requests when disabled')
                response.status = 404
                render(text: 'Not found', contentType: 'text/plain')
                return
            }
            String identifier = params.identifier
            String region = params.region
            String size = params.size
            String rotationParam = params.rotation
            String quality = params.quality
            String format = params.format ? String.valueOf(params.format).toLowerCase() : null
            // Support additional parameters consistent with ImageController:
            //  - i  (invalidate): bypass cached/generated derivative and regenerate
            //  - nr (no redirect): do not send a redirect response even if storage supports it
            boolean invalidate = params.containsKey('i')
            boolean noRedirect = params.containsKey('nr')

            def result = iiifImageService.render(identifier, region, size, rotationParam, quality, format, invalidate)

            if (result.isError()) {
                incrementCounter('iiif.render.error', 'IIIF render errors', [status: result.errorStatus?.toString() ?: 'unknown', format: format ?: 'unknown'])
                return iiifError(result.errorStatus as int, result.errorMessage)
            }

            enableCors()
            response.setHeader(ImageController.HEADER_ETAG, result.etag)
            if (result.lastModified) {
                response.setDateHeader(ImageController.HEADER_LAST_MODIFIED, result.lastModified.time)
            }
            if (grailsApplication.config.getProperty('images.cache.headers', Boolean, true)) {
                cache(shared: true, neverExpires: true)
            }

            if (!checkForModified(result.etag, result.lastModified)) {
                incrementCounter('iiif.render.notmodified', 'IIIF render not modified (304)', [format: format ?: 'unknown'])
                response.sendError(HttpServletResponse.SC_NOT_MODIFIED)
                return
            }

            def imageInfo = result.imageInfo

            if (!noRedirect && imageInfo.redirectUri) {
                incrementCounter('iiif.render.redirect', 'IIIF render redirects', [format: format ?: 'unknown'])
                response.sendRedirect(imageInfo.redirectUri.toString())
                return
            }

            response.contentLengthLong = imageInfo.length
            response.contentType = result.mime
            // Grails will provide a dummy output stream for HEAD requests but
            // explicitly bail on HEAD methods so we don't transfer bytes out of storage
            // unnecessarily
            if (request.method == 'HEAD') {
                incrementCounter('iiif.render.head', 'IIIF render HEAD requests', [format: format ?: 'unknown'])
                return
            }

            imageInfo.inputStreamSupplier.call(null).withStream { stream ->
                IOUtils.copy(stream, response.outputStream)
//                    response.flushBuffer()
            }

            incrementCounter('iiif.render.success', 'Successful IIIF render responses', [format: format ?: 'unknown'])
        }
    }

    // Helpers

    private boolean isIiifEnabled() {
        return grailsApplication.config.getProperty('iiif.enabled', Boolean, true)
    }

    private List<Integer> computeScaleFactors(Image image) {
        List<Integer> sf = []
        int levels = Math.max(1, image.zoomLevels ?: 1)
        int f = 1
        for (int i = 0; i < levels; i++) {
            sf << f
            f *= 2
        }
        return sf
    }

    private String getBaseUrl() {
        String configured = grailsApplication.config.getProperty('iiif.baseUrl', String, null)
        if (configured) return configured.trim().replaceAll('/+ ?$', '')
        String server = grailsApplication.config.getProperty('grails.serverURL', String, '')
        return server?.trim()?.replaceAll('/+ ?$', '') ?: ''
    }

    private void enableCors() {
        response.setHeader('Access-Control-Allow-Origin', '*')
        response.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS')
        response.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range')
    }

    private boolean checkForModified(String etag, Date lastModified) {
        String ifNoneMatch = request.getHeader(ImageController.HEADER_ETAG)
        long ifModifiedSince = request.getDateHeader(ImageController.HEADER_LAST_MODIFIED)
        if (ifNoneMatch && etag && ifNoneMatch == etag) return false
        if (ifModifiedSince > 0 && lastModified && lastModified.time <= ifModifiedSince) return false
        return true
    }

    private Map iiifError(int status, String message) {
        enableCors()
        response.status = status
        Map<String, Object> err = [
                '@context': CONTEXT,
                id       : request.requestURL?.toString(),
                type     : 'Error',
                status   : status,
                message  : message
        ]
        render(err as JSON)
        return err
    }

}
