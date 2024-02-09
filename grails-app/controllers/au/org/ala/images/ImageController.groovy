package au.org.ala.images

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
import static javax.servlet.http.HttpServletResponse.SC_FOUND
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND
import static javax.servlet.http.HttpServletResponse.SC_NOT_MODIFIED
import static javax.servlet.http.HttpServletResponse.SC_OK
import static javax.servlet.http.HttpServletResponse.SC_PARTIAL_CONTENT
import static javax.servlet.http.HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE

@Slf4j
class ImageController {

    private final static NEW_LINE = '\r\n'
    public static final String HEADER_ETAG = 'ETag'
    public static final String HEADER_LAST_MODIFIED = 'Last-Modified'
    public static final String HEADER_CONTENT_RANGE = 'Content-Range'
    public static final String HEADER_RANGE = 'Range'

    def imageService
    def imageStoreService
    def logService
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
        serveImage(
                { Image image -> image.fileSize },
                { Image image -> image.storageLocation.originalRedirectLocation(image.imageIdentifier) },
                { Image image, Range range -> imageStoreService.originalInputStream(image, range) },
                { Image image -> image.mimeType },
                { Image image -> image.extension },
                grailsApplication.config.getProperty('analytics.trackThumbnails', Boolean, false)
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
        serveImage(
                { Image image -> image.fileSize },
                { Image image -> image.storageLocation.originalRedirectLocation(image.imageIdentifier) },
                { Image image, Range range -> imageStoreService.originalInputStream(image, range) },
                { Image image -> image.mimeType },
                { Image image -> image.extension },
                grailsApplication.config.getProperty('analytics.trackThumbnails', Boolean, false)
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
        serveImage(
                { Image image ->
                    if (image.mimeType.startsWith('image')) {
                        imageStoreService.thumbnailStoredLength(image)
                    } else if (image.mimeType.startsWith('audio')) {
                        audioThumbnail.contentLength()
                    } else {
                        documentThumbnail.contentLength()
                    }
                },
                { Image image ->
                    if (image.mimeType.startsWith('image')) {
                        image.storageLocation.thumbnailRedirectLocation(image.imageIdentifier)
                    } else {
                        null
                    }
                },
                { Image image, Range range ->
                    if (image.mimeType.startsWith('image')) {
                        imageStoreService.thumbnailInputStream(image, range)
                    } else if (image.mimeType.startsWith('audio')) {
                        range.wrapInputStream(audioThumbnail.inputStream)
                    } else {
                        range.wrapInputStream(documentThumbnail.inputStream)
                    }
                },
                { Image image -> image.mimeType.startsWith('image') ? 'image/jpeg' : 'image/png' },
                { Image image -> image.mimeType.startsWith('image') ? 'jpg' : 'png' },
                grailsApplication.config.getProperty('analytics.trackThumbnails', Boolean, false)
        )
    }

    @Operation(
            method = "GET",
            summary = "Get image thumbnail large version.",
            description = "Get image thumbnail large version.",
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
    @Path("/image/{id}/large")
    def proxyImageThumbnailType() {
        String type = params.thumbnailType ?: 'large'
        serveImage(
                { Image image ->
                    if (image.mimeType.startsWith('image')) {
                        imageStoreService.thumbnailTypeStoredLength(image, type)
                    } else if (image.mimeType.startsWith('audio')) {
                        audioLargeThumbnail.contentLength()
                    } else {
                        documentLargeThumbnail.contentLength()
                    }
                },
                { Image image ->
                    if (image.mimeType.startsWith('image')) {
                        image.storageLocation.thumbnailTypeRedirectLocation(image.imageIdentifier, type)
                    } else {
                        null
                    }
                },
                { Image image, Range range ->
                    if (image.mimeType.startsWith('image')) {
                        imageStoreService.thumbnailTypeInputStream(image, type, range)
                    } else if (image.mimeType.startsWith('audio')) {
                        range.wrapInputStream(audioLargeThumbnail.inputStream)
                    } else {
                        range.wrapInputStream(documentLargeThumbnail.inputStream)
                    }
                },
                { Image image -> image.mimeType.startsWith('image') ? type == 'square' ? 'image/png' : 'image/jpeg' : 'image/png' },
                { Image image -> image.mimeType.startsWith('image') ? type == 'square' ? 'png' : 'jpg' : 'png' },
                grailsApplication.config.getProperty('analytics.trackThumbnails', Boolean, false)
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
        serveImage(
                { Image image -> imageStoreService.tileStoredLength(image, x, y, z) },
                { Image image -> image.storageLocation.tileRedirectLocation(image.imageIdentifier, x, y, z) },
                { Image image, Range range -> imageStoreService.tileInputStream(image, range, x, y, z) },
                { Image image -> 'image/jpeg' },
                { Image image -> 'jpg' },
                false
        )
    }

    private void serveImage(
            Closure<Long> getLength,
            Closure<URI> getRedirectUri,
            Closure<InputStream> getInputStream,
            Closure<String> getContentType,
            Closure<String> getExtension,
            boolean sendAnalytics) {
        def imageIdentifier = imageService.getImageGUIDFromParams(params)
        if (!imageIdentifier) {
            render(message: "Image not found", status: SC_NOT_FOUND, contentType: 'text/plain')
            return
        }

        def imageInstance = Image.findByImageIdentifier(imageIdentifier, [ cache: true])
        if (!imageInstance) {
            render(message: "Image not found", status: SC_NOT_FOUND, contentType: 'text/plain')
            return
        }

        boolean contentDisposition = params.boolean("contentDisposition", false)
        boolean cacheHeaders = grailsApplication.config.getProperty('images.cache.headers', Boolean, true)

        if (sendAnalytics) {
            analyticsService.sendAnalytics(imageInstance, 'imageview', request.getHeader("User-Agent"))
        }

        if (imageInstance.storageLocation.supportsRedirect) {
            URI uri = getRedirectUri(imageInstance)
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
            def etag = imageInstance.contentSHA1Hash
            def lastMod = imageInstance.dateUploaded
            def changed = checkForNotModified(etag, lastMod)
            if (changed) {
                response.setHeader(HEADER_ETAG, etag)
                response.setDateHeader(HEADER_LAST_MODIFIED, lastMod.time)
                if (cacheHeaders) {
                    cache(shared: true, neverExpires: true)
                }
                response.sendError(SC_NOT_MODIFIED)
                return
            }

            length = getLength(imageInstance)
            def ranges = decodeRangeHeader(length)
            def contentType = getContentType(imageInstance)
            def extension = getExtension(imageInstance)

            if (ranges.size() > 1) {
                def boundary = startMultipartResponse(ranges, contentType)
                response.setHeader(HEADER_ETAG, etag)
                response.setDateHeader(HEADER_LAST_MODIFIED, lastMod.time)
                if (cacheHeaders) {
                    cache(shared: true, neverExpires: true)
                }

                // Grails will provide a dummy output stream for HEAD requests but
                // explicitly bail on HEAD methods so we don't transfer bytes out of storage
                // unnecessarily
                if (request.method == 'HEAD') {
                    return
                }

                def out = response.outputStream
                def pw = out.newPrintWriter()

                for (def range: ranges) {
                    writeRangePart(range, imageInstance, boundary, contentType, pw, out, getInputStream)
                }
                finaliseMultipartResponse(boundary, pw)
                response.flushBuffer()
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
                response.setHeader(HEADER_ETAG, etag)
                response.setDateHeader(HEADER_LAST_MODIFIED, lastMod.time)
                if (cacheHeaders) {
                    cache(shared: true, neverExpires: true)
                }
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

                getInputStream(imageInstance, range).withStream { stream ->
                    IOUtils.copy(stream, response.outputStream)
                    response.flushBuffer()
                }
            }
        } catch (Range.InvalidRangeHeaderException e) {
            response.setHeader(HEADER_CONTENT_RANGE, "bytes */${length != -1 ? length : '*'}")
            render(text: "Invalid range header", status: SC_REQUESTED_RANGE_NOT_SATISFIABLE, contentType: 'text/plain')
        } catch (FileNotFoundException e) {
            log.debug('Image not found in storage', e)
            render(text: "Image not found in storage", status: SC_NOT_FOUND, contentType: 'text/plain')
        } catch (ClientAbortException e) {
            // User hung up, just ignore this exception since we can't recover into a nice error response.
            throw e
        } catch (Exception e) {
            log.error("Exception serving image", e)
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

    /**
     * Check whether a 304 response should be sent
     * @param etag The etag of the current URL
     * @param lastMod the last modified date of the current URL
     * @return true if a 304 response should be sent
     */
    private boolean checkForNotModified(String etag, Date lastMod) {
        def possibleTags = request.getHeader('If-None-Match')
        def modifiedDate = -1
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

    private void writeRangePart(Range range, Image imageInstance, String boundary, String contentType, PrintWriter pw, OutputStream out, Closure<InputStream> getInputStream) {
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

        getInputStream(imageInstance, range).withStream { stream ->
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
            def imageList = Image.findAll()
            long count = 0
            imageList.each { image ->
                imageService.scheduleArtifactGeneration(image.id, userId)
                count++
            }
            results.message = "Image artifact generation scheduled for ${count} images."
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
                    render([success:false] as JSON)
                }
            }
            xml {
                response.addHeader("Access-Control-Allow-Origin", "*")
                def imageInstance = imageService.getImageFromParams(params)
                if(imageInstance) {
                    render(imageInstance as XML)
                } else {
                    response.status = SC_NOT_FOUND
                    render([success:false] as XML)
                }
            }
        }
    }

    private def getImageModel(Image image){
        def subimages = Subimage.findAllByParentImage(image)*.subimage
        def sizeOnDisk = imageStoreService.consumedSpace(image)

        def userId = authService.userId

        def isAdmin = request.isUserInRole(CASRoles.ROLE_ADMIN)

        def thumbUrls = imageService.getAllThumbnailUrls(image.imageIdentifier)

        boolean isImage = imageService.isImageType(image)

        //add additional metadata
        def resourceLevel = collectoryService.getResourceLevelMetadata(image.dataResourceUid)

        if (grailsApplication.config.getProperty('analytics.trackDetailedView', Boolean, false)) {
            analyticsService.sendAnalytics(image, 'imagedetailedview', request.getHeader("User-Agent"))
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
            analyticsService.sendAnalytics(image, 'imagelargeviewer', request.getHeader("User-Agent"))
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
            if (params.source) {
                source = MetaDataSourceType.valueOf(params.source)
                if (source){
                    metaData = imageInstance.metadata?.findAll { it.source == source }
                } else {
                    metaData = imageInstance.metadata
                }
            } else {
                metaData = imageInstance.metadata
            }
        }

        [imageInstance: imageInstance, metaData: metaData?.sort { it.name }, source: source]
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
        def metadata = ImageMetaDataItem.findAllByImage(imageInstance)
        [imageInstance: imageInstance, x: params.x, y: params.y, width: params.width, height: params.height, metadata: metadata]
    }

    def viewer() {
        def imageInstance = imageService.getImageFromParams(params)
        if (!imageInstance) {
            response.sendError(404)
            return
        }
        if (grailsApplication.config.getProperty('analytics.trackLargeViewer', Boolean)) {
            analyticsService.sendAnalytics(imageInstance, 'imagelargeviewer', request.getHeader("User-Agent"))
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

