package au.org.ala.images

import au.org.ala.images.metrics.MetricsSupport
import au.org.ala.web.AlaSecured
import au.org.ala.web.CASRoles
import grails.converters.JSON
import grails.converters.XML
import groovy.util.logging.Slf4j
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.headers.Header
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse

import org.apache.catalina.connector.ClientAbortException
import org.apache.commons.io.IOUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.Path
import javax.ws.rs.Produces
import java.util.concurrent.atomic.AtomicLong

import static io.swagger.v3.oas.annotations.enums.ParameterIn.HEADER
import static io.swagger.v3.oas.annotations.enums.ParameterIn.PATH
import static io.swagger.v3.oas.annotations.enums.ParameterIn.QUERY
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST
import static javax.servlet.http.HttpServletResponse.SC_FOUND
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND
import static javax.servlet.http.HttpServletResponse.SC_NOT_MODIFIED
import static javax.servlet.http.HttpServletResponse.SC_OK
import static javax.servlet.http.HttpServletResponse.SC_PARTIAL_CONTENT
import static javax.servlet.http.HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE

@Slf4j
class ImageController implements MetricsSupport {

    private final static NEW_LINE = '\r\n'
    public static final String HEADER_ETAG = 'ETag'
    public static final String HEADER_LAST_MODIFIED = 'Last-Modified'
    public static final String HEADER_CONTENT_RANGE = 'Content-Range'
    public static final String HEADER_RANGE = 'Range'

    def imageService
    def imageStoreService
    def imageStagingService
    def batchService
    def collectoryService
    def authService
    def analyticsService

    @Value('${placeholder.sound.thumbnail}')
    Resource audioThumbnail

    @Value('${placeholder.sound.large}')
    Resource audioLargeThumbnail

    @Value('${placeholder.document.thumbnail}')
    Resource documentThumbnail

    @Value('${placeholder.document.large}')
    Resource documentLargeThumbnail

    @Value('${placeholder.missing.thumbnail}')
    Resource missingThumbnail

    @Value('${placeholder.image.original}')
    Resource missingOriginal

    @Value('${placeholder.image.thumbnail}')
    Resource missingImageThumbnail

    @Value('classpath:images/images-placeholder-650x499.png')
    Resource missingImageThumbnailLarge

    @Value('classpath:images/images-placeholder-650x650.png')
    Resource missingImageThumbnailSquareLarge

    @Value('classpath:images/images-placeholder-300x300.png')
    Resource missingImageThumbnailSquare

    @Value('classpath:images/images-placeholder-300x300.png')
    Resource missingTile

    @Value('${analytics.trackThumbnails:false}')
    boolean trackThumbnails = false

    @Value('${images.cache.headers:true}')
    boolean cacheHeaders = true

    @Value('${images.disableCache:false}')
    boolean disableCache = false

    static AtomicLong boundaryCounter = new AtomicLong(0)

    def index() { }

    def list(){
        redirect(controller: 'search', action:'list')
    }

    /**
     * @deprecated use getOriginalFile instead.
     */
    @Deprecated
    def proxyImage() {
        boolean noRedirect = params.containsKey('nr')
        serveImage(
                imageStoreService.originalImageInfo(imageService.getImageGUIDFromParams(params)),
                trackThumbnails,
                'original',
                noRedirect
        )
    }

    @Operation(
            method = "GET",
            summary = "Get original image, sound or video file.",
            description = "Get original image, sound or video file.",
            parameters = [
                    @Parameter(name="id", in = PATH, description = 'Image Id', required = true)
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType='image/jpeg')],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            tags = ['Access to image derivatives (e.g. thumbnails, tiles and originals)']
    )
    @Produces("image/jpeg")
    @Path("/image/{id}/original")
    def getOriginalFile() {
        boolean noRedirect = params.containsKey('nr')
        serveImage(
                imageStoreService.originalImageInfo(imageService.getImageGUIDFromParams(params)),
                trackThumbnails,
                'original',
                noRedirect
        )
    }

    /**
     * This method serves the image from the file system where possible for better performance.
     * proxyImageThumbnail is used heavily by applications rendering search results (biocache, BIE).
     */
    @Operation(
            method = "GET",
            summary = "Get image thumbnail.",
            description = "Get image thumbnail.",
            parameters = [
                    @Parameter(name="id", in = PATH, description = 'Image Id', required = true)
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType='image/jpeg')],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            tags = ['Access to image derivatives (e.g. thumbnails, tiles and originals)']
    )
    @Produces("image/jpeg")
    @Path("/image/{id}/thumbnail")
    def proxyImageThumbnail() {
        boolean invalidate = params.containsKey('i')
        boolean noRedirect = params.containsKey('nr')
        serveImage(
                imageStoreService.thumbnailImageInfo(imageService.getImageGUIDFromParams(params), '', invalidate),
                trackThumbnails,
                'thumbnail',
                noRedirect
        )
    }

    @Operation(
            method = "GET",
            summary = "Get an image thumbnail version.",
            description = "Get an image thumbnail version.",
            parameters = [
                    @Parameter(name="id", in = PATH, description = 'Image Id', required = true),
                    @Parameter(name="type", in = PATH, description = 'Thumbnail type (one of: large, xlarge, square, square_black, square_white, square_darkGrey, square_darkGray, centre_crop, centre_crop_large)', required = true)
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType='image/jpeg')],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            tags = ['Access to image derivatives (e.g. thumbnails, tiles and originals)']
    )
    @Produces("image/jpeg")
    @Path("/image/{id}/{type}")
    def proxyImageThumbnailType() {
        String type = params.thumbnailType ?: params.type ?:  'large' // for backwards compat thumbnailType URL param takes precedence
        boolean invalidate = params.containsKey('i')
        boolean noRedirect = params.containsKey('nr')
        if (!imageService.validateThumbnailType(type)) {
            render(text: "Invalid thumbnail type", status: SC_NOT_FOUND, contentType: 'text/plain')
            return
        }
        serveImage(
                imageStoreService.thumbnailImageInfo(imageService.getImageGUIDFromParams(params), type, invalidate),
                trackThumbnails,
                'thumbnail-'+type,
                noRedirect
        )
    }

    @Operation(
            method = "GET",
            summary = "Get image tile - for use with tile mapping service clients such as LeafletJS or Openlayers.",
            description = "Get image tile - for use with tile mapping service clients such as LeafletJS or Openlayers.",
            parameters = [
                    @Parameter(name="id", in = PATH, description = 'Image Id', required = true),
                    @Parameter(name="z", in = PATH, description = 'Tile mapping service Z value', required = true),
                    @Parameter(name="y", in = PATH, description = 'Tile mapping service Y value', required = true),
                    @Parameter(name="x", in = PATH, description = 'Tile mapping service X value', required = true),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType='image/png')],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            tags = ['Access to image derivatives (e.g. thumbnails, tiles and originals)']
    )
    @Produces("image/png")
    @Path("/image/{id}/tms/{z}/{x}/{y}.png")
    def proxyImageTile() {
        int x = params.int('x')
        int y = params.int('y')
        int z = params.int('z')
        boolean invalidate = params.containsKey('i')
        boolean noRedirect = params.containsKey('nr')
        serveImage(
                imageStoreService.tileImageInfo(imageService.getImageGUIDFromParams(params), x, y, z, invalidate),
                false,
                'tile',
                noRedirect
        )
    }

    private void serveImage(
            ImageInfo imageInfo,
            boolean sendAnalytics,
            String requestType,
            boolean noRedirect = false) {
        recordTime('image.serve', 'Time to serve image request', [type: requestType]) {
            def imageIdentifier = imageInfo.imageIdentifier
            if (!imageIdentifier || !imageInfo.exists) {
                incrementCounter('image.serve.notfound', 'Image not found requests', [type: requestType])
                if (imageInfo.shouldExist && imageInfo.contentType.startsWith('image')) {
                    sendMissingImage(requestType)
                } else {
                    render(text: "Image not found", status: SC_NOT_FOUND, contentType: 'text/plain')
                }
                return
            }

            incrementCounter('image.serve.request', 'Image serve requests', [type: requestType])

            boolean contentDisposition = params.boolean("contentDisposition", false)

        if (sendAnalytics) {
            if (imageInfo.dataResourceUid != null) {
                analyticsService.sendAnalytics(imageInfo.exists, imageInfo.dataResourceUid, 'imageview', request.getHeader("User-Agent"))
            } else {
                analyticsService.sendAnalyticsFromImageId(imageInfo.exists, imageInfo.imageIdentifier, 'imageview', request.getHeader("User-Agent"))
            }
        }

        if (!noRedirect && imageInfo.redirectUri) {
            if (disableCache) {
                // Replace any cache headers with no-cache headers
                response.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate')
                response.setHeader('Pragma', 'no-cache')
                response.setDateHeader('Expires', 0)
            }
            URI uri = imageInfo.redirectUri
            if (uri) {
                response.status = SC_FOUND
                response.setHeader('Location', uri.toString())
                return
            }
        }

        long length = -1

        try {
            // could use withCacheHeaders here but they add Etag/LastModified even if we throw an exception during
            // the generate closure
            def etag = imageInfo.etag
            def lastMod = imageInfo.lastModified
            if (!disableCache) {
                def changed = checkForNotModified(etag, lastMod)
                if (changed) {
                    if (etag) {
                        response.setHeader(HEADER_ETAG, etag)
                    }
                    if (lastMod) {
                        response.setDateHeader(HEADER_LAST_MODIFIED, lastMod.time)
                    }
                    if (cacheHeaders) {
                        cache(shared: true, neverExpires: true)
                    }
                    response.sendError(SC_NOT_MODIFIED)
                    return
                }
            }

            length = imageInfo.length
            def ranges = decodeRangeHeader(length)
            def contentType = imageInfo.contentType
            def extension = imageInfo.extension

            if (ranges.size() > 1) {
                def boundary = startMultipartResponse(ranges, contentType)
                applyCacheHeaders(disableCache, cacheHeaders, etag, lastMod)

                // Grails will provide a dummy output stream for HEAD requests but
                // explicitly bail on HEAD methods so we don't transfer bytes out of storage
                // unnecessarily
                if (request.method == 'HEAD') {
                    return
                }

                def out = response.outputStream
                def pw = out.newPrintWriter()

                for (def range: ranges) {
                    writeRangePart(range, imageInfo, boundary, contentType, pw, out)
                }
                finaliseMultipartResponse(boundary, pw)
//                response.flushBuffer()
            } else {
                Range range = ranges[0]
                long rangeLength = range.length()
                String contentRange = range.contentRangeHeader()
                if (contentRange) {
                    response.setHeader(HEADER_CONTENT_RANGE, contentRange)
                    response.setStatus(SC_PARTIAL_CONTENT)
                } else {
                    response.setHeader("Accept-Ranges", "bytes")
                }
                applyCacheHeaders(disableCache, cacheHeaders, etag, lastMod)
                response.contentLengthLong = rangeLength
                response.contentType = contentType
                if (contentDisposition) {
                    response.setHeader("Content-disposition", "attachment;filename=${imageIdentifier}.${extension ?: "jpg"}")
                }
                // Grails will provide a dummy output stream for HEAD requests but
                // explicitly bail on HEAD methods so we don't transfer bytes out of storage
                // unnecessarily
                if (request.method == 'HEAD') {
                    return
                }

                imageInfo.inputStreamSupplier.call(range).withStream { stream ->
                    IOUtils.copy(stream, response.outputStream)
//                    response.flushBuffer()
                }
            }
        } catch (Range.InvalidRangeHeaderException e) {
            response.setHeader(HEADER_CONTENT_RANGE, "bytes */${length != -1 ? length : '*'}")
            render(text: "Invalid range header", status: SC_REQUESTED_RANGE_NOT_SATISFIABLE, contentType: 'text/plain')
        } catch (FileNotFoundException e) {
            log.debug('Image not found in storage', e)
            if (imageInfo.shouldExist && imageInfo.contentType.startsWith('image')) {
                sendMissingImage(requestType)
            } else {
                render(text: "Image not found", status: SC_NOT_FOUND, contentType: 'text/plain')
            }
        } catch (ClientAbortException e) {
            // User hung up, just ignore this exception since we can't recover into a nice error response.
            incrementCounter('image.serve.client_abort', 'Client aborted requests', [type: requestType])
        } catch (Exception e) {
            log.error("Exception serving image", e)
            recordError('serveImage', [type: requestType, error: e.class.simpleName])
            cache(false)
            if (response.containsHeader(HEADER_LAST_MODIFIED)) {
                response.setHeader(HEADER_LAST_MODIFIED, '')
            }
            if (response.containsHeader(HEADER_ETAG)) {
                response.setHeader(HEADER_ETAG, '')
            }
            throw e
        }
        }
    }

    private void applyCacheHeaders(boolean disableCache, boolean cacheHeadersEnabled, String etag, Date lastMod) {
        if (disableCache) {
            // Replace any cache headers with no-cache headers
            response.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate')
            response.setHeader('Pragma', 'no-cache')
            response.setDateHeader('Expires', 0)
            // Do not emit ETag/Last-Modified when caching is disabled
            return
        }
        if (etag) {
            response.setHeader(HEADER_ETAG, etag)
        }
        if (lastMod) {
            response.setDateHeader(HEADER_LAST_MODIFIED, lastMod.time)
        }
        if (cacheHeadersEnabled) {
            cache(shared: true, neverExpires: true)
        }
    }

    /**
     * Serve a placeholder image with a 404 status code.
     * @param requestType The type of request, one of 'tile', 'original', 'thumbnail-large', 'thumbnail-square', 'thumbnail'
     */
    private void sendMissingImage(String requestType) {
        Resource ph
        switch (requestType) {
            case 'tile':
                ph = missingTile
                break
            case 'original':
                ph = missingOriginal
                break
            case 'thumbnail-centre_crop_large':
                ph = missingImageThumbnailSquareLarge
                break
            case 'thumbnail-large':
                ph = missingImageThumbnailLarge
                break
            case 'thumbnail-square':
            case 'thumbnail-square_white':
            case 'thumbnail-square_black':
            case 'thumbnail-square_darkGrey':
            case 'thumbnail-square_darkGray':
            case 'thumbnail-centre_crop':
                ph = missingImageThumbnailSquare
                break
            case 'thumbnail':
            default:
                ph = missingImageThumbnail
                break
        }
        try {
            def len = ph.contentLength()
            response.status = SC_NOT_FOUND
            response.contentType = 'image/png'
            response.contentLengthLong = len
            if (request.method != 'HEAD') {
                ph.inputStream.withStream { stream ->
                    IOUtils.copy(stream, response.outputStream)
                }
            }
        } catch (Exception e) {
            log.warn("Failed to stream placeholder image for requestType=${requestType}", e)
            render(text: "Image not found", status: SC_NOT_FOUND, contentType: 'text/plain')
        }
    }

    /**
     * Check whether a 304 response should be sent
     * @param etag The etag of the current URL
     * @param lastMod the last modified date of the current URL
     * @return true if a 304 response should be sent
     */
    private boolean checkForNotModified(String etag, Date lastMod) {
        def possibleTags = request.getHeader('If-None-Match')
        long modifiedDate = -1
        try {
            modifiedDate = request.getDateHeader('If-Modified-Since')
        }
        catch (IllegalArgumentException iae) {
            // nom nom nom
            log.error "Couldn't parse If-Modified-Since header", iae
        }

        if (possibleTags || (modifiedDate != -1)) {
            def etagChanged = false
            def lastModChanged = false

            // First let's check for ETags, they are 1st class
            if (possibleTags && etag) {
                def tagList = possibleTags.split(',')*.trim()
                log.debug "There was a list of ETag candidates supplied [{}], new ETag... {}", tagList, etag
                if (!tagList.contains(etag)) {
                    etagChanged = true
                }
            }

            if ((modifiedDate != -1) && lastMod) {
                // Or... 2nd class... check lastmod
                def compareDate = new Date(modifiedDate)

                if (compareDate < lastMod) {
                    lastModChanged = true
                }
            }
            // If neither has changed, we 304. But if either one has changed, we don't
            return !etagChanged && !lastModChanged
        }
        // no headers in request, no 304
        return false
    }

    private String startMultipartResponse(List<Range> ranges, String contentType) {

        def boundary = boundaryCounter.incrementAndGet().toString().padLeft(20, '0')

        def rangesSize = ranges.size()

        def contentTypeHeader = 'Content-Type: ' + contentType

        def contentRanges = ranges*.contentRangeHeader()

        response.status = SC_PARTIAL_CONTENT
        response.contentType = "multipart/byteranges; boundary=$boundary"
        // calculate the content-length for the response
        // each range will contain:
        // \r\n
        // --$boundary\r\n
        // Content-Type: $contentType\r\n
        // Content-Range: $range.contentRangeHeader\r\n
        // \r\n
        // range bytes\r\n
        //
        // then the footer:
        // \r\n
        // --$boundary--\r\n
        response.contentLengthLong = (2 + 2 + boundary.size() + 2 + contentTypeHeader.size() + 2 + HEADER_CONTENT_RANGE.size() + ': '.size() + 2 + 2) * rangesSize + contentRanges.sum { it.size() } + ranges*.length().sum() + 2 + 2 + boundary.size() + 2 + 2

        return boundary
    }

    private void writeRangePart(Range range, ImageInfo imageInfo, String boundary, String contentType, PrintWriter pw, OutputStream out) {
        pw.write(NEW_LINE)
        pw.write('--')
        pw.write(boundary)
        pw.write(NEW_LINE)
        pw.write('Content-Type: ')
        pw.write(contentType)
        pw.write(NEW_LINE)
        pw.write(HEADER_CONTENT_RANGE)
        pw.write(': ')
        pw.write(range.contentRangeHeader())
        pw.write(NEW_LINE)
        pw.write(NEW_LINE)
        pw.flush()

        imageInfo.inputStreamSupplier.call(range).withStream { stream ->
            IOUtils.copy(stream, out)
        }
        out.flush()
    }

    private void finaliseMultipartResponse(String boundary, PrintWriter pw) {
        pw.write(NEW_LINE)
        pw.write('--')
        pw.write(boundary)
        pw.write('--')
        pw.write(NEW_LINE)
        pw.flush()
    }

    /**
     * This service is used directly in front end in an AJAX fashion.
     * method to authenticate client applications.
     *
     * @return
     */
    def deleteImage() {

        def success = false

        def message

        def image = Image.findByImageIdentifier(params.id as String, [ cache: true])

        if (image) {
            def userId = getUserIdForRequest(request)

            if (userId){
                //is user in ROLE_ADMIN or the original owner of the image
                def isAdmin = request.isUserInRole(CASRoles.ROLE_ADMIN)
                def isImageOwner = image.uploader == userId
                if (isAdmin || isImageOwner){
                    success = imageService.scheduleImageDeletion(image.id, userId)
                    message = "Image scheduled for deletion."
                } else {
                    message = "Logged in user is not authorised."
                }
            } else {
                message = "Unable to obtain user details."
            }
        } else {
            message = "Invalid image identifier."
        }
        renderResults(["success": success, message: message])
    }

    @AlaSecured(value = [CASRoles.ROLE_ADMIN], anyRole = true, redirectUri = "/")
    def scheduleArtifactGeneration() {

        def imageInstance = imageService.getImageFromParams(params)
        def userId = authService.getUserId()
        def results = [success: true]

        if (imageInstance) {
            imageService.scheduleArtifactGeneration(imageInstance.id, userId)
            results.message = "Image artifact generation scheduled for image ${imageInstance.id}"
        } else {
            // Removing this because loading the whole db is probably not a good idea now
//            def imageList = Image.findAll()
//            long count = 0
//            imageList.each { image ->
//                imageService.scheduleArtifactGeneration(image.id, userId)
//                count++
//            }
//            results.message = "Image artifact generation scheduled for ${count} images."
            render(status: SC_BAD_REQUEST, contentType: 'text/plain', text: 'Image not found')
        }

        renderResults(results)
    }

    @Operation(
            method = "GET",
            summary = "Get original image.",
            description = "Get original image.",
            parameters = [
                    @Parameter(name="id", in = QUERY, description = 'Image Id', required = true),
                    @Parameter(name="Accept", in = HEADER, description = "Content type requested", required = true)
            ],
            responses = [
                    @ApiResponse(content = [
                            @Content(mediaType='application/json', schema = @Schema(implementation=Map)),
                            @Content(mediaType='image/*', schema = @Schema(type="string", format="binary")),
                            @Content(mediaType='text/html', schema = @Schema(implementation=Map)),
                    ],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            tags = ['Access to image derivatives (e.g. thumbnails, tiles and originals)']
    )
    @Produces("application/json")
    @Path("/image/details")
    def details() {
        withFormat {
            html {
                def imageInstance = imageService.getImageFromParams(params)
                if (imageInstance) {
                    getImageModel(imageInstance)
                } else {
                    flash.errorMessage = "Could not find image with id ${params.int("id") ?: params.imageId }!"
                    redirect(action:'list', controller: 'search')
                }
            }
            image {
                getOriginalFile()
            }
            json {
                def imageInstance = imageService.getImageFromParams(params)
                if (imageInstance) {
                    def payload = [:]
                    imageService.addImageInfoToMap(imageInstance, payload, false, false)
                    def jsonStr = payload as JSON
                    if (params.callback) {
                        response.setContentType("text/javascript")
                        render("${params.callback}(${jsonStr})")
                    } else {
                        response.addHeader("Access-Control-Allow-Origin", "*")
                        response.setContentType("application/json")
                        render(jsonStr)
                    }
                } else {
                    response.status = SC_NOT_FOUND
                    render([success:false] as JSON, status: SC_NOT_FOUND)
                }
            }
            xml {
                response.addHeader("Access-Control-Allow-Origin", "*")
                def imageInstance = imageService.getImageFromParams(params)
                if(imageInstance) {
                    render(imageInstance as XML)
                } else {
                    response.status = SC_NOT_FOUND
                    render([success:false] as XML, status: SC_NOT_FOUND)
                }
            }
        }
    }

    private def getImageModel(Image image){
        def subimages = Subimage.findAllByParentImage(image)*.subimage
        long sizeOnDisk
        try {
            // don't run the calculation on total size here, it could be expensive.
            // if required, the admin view will still have it.
//            sizeOnDisk = imageStoreService.consumedSpace(image)
            sizeOnDisk = image.fileSize
        } catch (e) {
            sizeOnDisk = -1
        }

        def userId = authService.userId

        def isAdmin = request.isUserInRole(CASRoles.ROLE_ADMIN)

        def thumbUrls = imageService.getAllThumbnailUrls(image.imageIdentifier)

        boolean isImage = imageService.isImageType(image)

        //add additional metadata
        def resourceLevel = collectoryService.getResourceLevelMetadata(image.dataResourceUid)

        if (grailsApplication.config.getProperty('analytics.trackDetailedView', Boolean, false)) {
            analyticsService.sendAnalytics(image != null, image?.dataResourceUid, 'imagedetailedview', request.getHeader("User-Agent"))
        }

        [imageInstance: image, subimages: subimages,
         parentImage: image.parent,
         sizeOnDisk: sizeOnDisk,
         squareThumbs: thumbUrls, isImage: isImage, resourceLevel: resourceLevel, isAdmin:isAdmin, userId:userId]
    }

    def view() {
        def image = imageService.getImageFromParams(params)
        if (!image) {
            flash.errorMessage = "Could not find image with id ${params.int("id")}!"
        }
        def subimages = Subimage.findAllByParentImage(image)*.subimage

        if (grailsApplication.config.getProperty('analytics.trackLargeViewer', Boolean, false)) {
            analyticsService.sendAnalytics(image != null, image?.dataResourceUid, 'imagelargeviewer', request.getHeader("User-Agent"))
        }

        render (view: 'viewer', model: [imageInstance: image, subimages: subimages])
    }

    def tagsFragment() {
        def imageInstance = imageService.getImageFromParams(params)
        def imageTags = ImageTag.findAllByImage(imageInstance)
        def tags = imageTags?.collect { it.tag }
        def leafTags = TagUtils.getLeafTags(tags)

        [imageInstance: imageInstance, tags: leafTags]
    }

    @AlaSecured(value = [CASRoles.ROLE_ADMIN])
    def imageAuditTrailFragment() {
        def imageInstance = Image.get(params.int("id"))
        def messages = []
        if (imageInstance) {
            messages = AuditMessage.findAllByImageIdentifier(imageInstance.imageIdentifier, [order:'asc', sort:'dateCreated'])
        }
        [messages: messages]
    }

    def imageMetadataTableFragment() {

        def imageInstance = imageService.getImageFromParams(params)
        def metaData = []
        def source = null
        if (imageInstance) {
            // Use helper method that reads from JSONB with fallback to EAV table
            def allMetadata = imageService.getImageMetadata(imageInstance)

            if (params.source) {
                source = MetaDataSourceType.valueOf(params.source)
                if (source){
                    metaData = allMetadata?.findAll { it.source == source }
                } else {
                    metaData = allMetadata
                }
            } else {
                metaData = allMetadata
            }
        }

        [imageInstance: imageInstance, metaData: metaData?.sort { it.key }, source: source]
    }

    def coreImageMetadataTableFragment() {
        def imageInstance = imageService.getImageFromParams(params)

        render(view: '_coreImageMetadataFragment', model: getImageModel(imageInstance))
    }

    def imageTooltipFragment() {
        def imageInstance = imageService.getImageFromParams(params)
        [imageInstance: imageInstance]
    }

    def imageTagsTooltipFragment() {
        def imageInstance = imageService.getImageFromParams(params)
        def imageTags = ImageTag.findAllByImage(imageInstance)
        def tags = imageTags?.collect { it.tag }
        def leafTags = TagUtils.getLeafTags(tags)
        [imageInstance: imageInstance, tags: leafTags]
    }

    def createSubimageFragment() {
        def imageInstance = imageService.getImageFromParams(params)
        def metadata = imageService.getImageMetadata(imageInstance)
        [imageInstance: imageInstance, x: params.x, y: params.y, width: params.width, height: params.height, metadata: metadata]
    }

    def viewer() {
        def imageInstance = imageService.getImageFromParams(params)
        if (!imageInstance) {
            response.sendError(404)
            return
        }
        if (grailsApplication.config.getProperty('analytics.trackLargeViewer', Boolean)) {
            analyticsService.sendAnalytics(imageInstance != null, imageInstance?.dataResourceUid, 'imagelargeviewer', request.getHeader("User-Agent"))
        }
        [imageInstance: imageInstance, auxDataUrl: params.infoUrl]
    }

    private renderResults(Object results, int responseCode = SC_OK) {

        withFormat {
            json {
                def jsonStr = results as JSON
                if (params.callback) {
                    response.setContentType("text/javascript")
                    render("${params.callback}(${jsonStr})")
                } else {
                    response.setContentType("application/json")
                    render(jsonStr)
                }
            }
            xml {
                render(results as XML)
            }
        }
        response.status = responseCode
    }

    @AlaSecured(value = [CASRoles.ROLE_USER, CASRoles.ROLE_ADMIN], anyRole = true, redirectUri = "/")
    def resetImageCalibration() {
        def image = Image.findByImageIdentifier(params.imageId, [ cache: true])
        if (image) {
            imageService.resetImageLinearScale(image)
            renderResults([success: true, message:"Image linear scale has been reset"])
            return
        }
        renderResults([success:false, message:'Missing one or more required parameters: imageId, pixelLength, actualLength, units'])
    }

    private getUserIdForRequest(HttpServletRequest request) {
        if (grailsApplication.config.getProperty('security.cas.disableCAS', Boolean, false)){
            return "-1"
        }
        authService.getUserId()
    }

    private List<Range> decodeRangeHeader(long totalLength) {
        def range = request.getHeader(HEADER_RANGE)
        if (range) {
            return Range.decodeRange(range, totalLength)
        } else {
            return [Range.emptyRange(totalLength)]
        }
    }
}

