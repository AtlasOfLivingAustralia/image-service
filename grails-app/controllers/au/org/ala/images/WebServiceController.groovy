package au.org.ala.images

import au.ala.org.ws.security.RequireApiKey
import au.org.ala.plugins.openapi.Path
import au.org.ala.web.SSO
//import au.org.ala.ws.security.ApiKeyInterceptor
import com.google.common.base.Suppliers
import grails.converters.JSON
import grails.converters.XML
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.headers.Header
import io.swagger.v3.oas.annotations.media.ArraySchema
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import org.apache.http.HttpStatus
import grails.plugins.csv.CSVWriter
import org.springframework.web.multipart.MultipartFile

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.Consumes
import javax.ws.rs.Produces
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream

import static io.swagger.v3.oas.annotations.enums.ParameterIn.HEADER
import static io.swagger.v3.oas.annotations.enums.ParameterIn.PATH
import static io.swagger.v3.oas.annotations.enums.ParameterIn.QUERY

class WebServiceController {

    static allowedMethods = [findImagesByMetadata: 'POST', getImageInfoForIdList: 'POST']

    def imageService
    def imageStoreService
    def tagService
    def searchService
    def logService
    def batchService
    def elasticSearchService
    def collectoryService
    def authService
    def imageRecognitionService

    @Operation(
            method = "DELETE",
            summary = "Delete image",
            description = "Delete image. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "imageID", in = PATH, required = true, description = "Image Id", schema = @Schema(implementation = String)),
                    @Parameter(name = "apiKey", in = HEADER, required = true, description = "User Id as api key", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "405",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "404",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
               @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/image/{imageID}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = "image-service/write")
    def deleteImageService() {

        def success = false
        def userId = request.getRemoteUser() // TODO is this populated?

        if(!userId) {
            response.sendError(HttpStatus.SC_BAD_REQUEST, "Must include API key")
        } else {
            def message = ""
            def image = Image.findByImageIdentifier(params.imageID as String, [ cache: true])
            if (image) {
                success = imageService.scheduleImageDeletion(image.id, userId)
                message = "Image scheduled for deletion."
            } else {
                message = "Invalid image identifier."
            }
            renderResults(["success": success, message: message])
        }
    }

    private long forEachImageId(closure) {
        def c = Image.createCriteria()
        def imageIdList = c {
            projections {
                property("id")
            }
        }

        long count = 0
        imageIdList.each { imageId ->
            if (closure) {
                closure(imageId)
            }
            count++
        }
        return count
    }

    @Operation(
            method = "POST",
            summary = "Schedule thumbnail generation",
            description = "Schedule thumbnail generation. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Image Id", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "405", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
            ]),
                    @ApiResponse(responseCode = "404", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/scheduleThumbnailGenerationWS/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def scheduleThumbnailGenerationWS() {
        scheduleThumbnailGeneration()
    }

    @SSO
    def scheduleThumbnailGeneration() {
        def imageInstance = Image.findByImageIdentifier(params.id as String, [ cache: true])
        def userId = getUserIdForRequest(request)
        def results = [success: true]

        if (params.id && !imageInstance) {
            results.success = false
            results.message = "Could not find image ${params.id}"
            renderResults(results, HttpStatus.SC_BAD_REQUEST)
        } else {
            if (imageInstance) {
                imageService.scheduleThumbnailGeneration(imageInstance.id, userId)
                results.message = "Image thumbnail generation scheduled for image ${imageInstance.id}"
            } else {
                def count = forEachImageId { imageId ->
                    imageService.scheduleThumbnailGeneration(imageId, userId)
                }
                results.message = "Image thumbnail generation scheduled for ${count} images."
            }
            renderResults(results)
        }
    }

    @Operation(
            method = "POST",
            summary = "Schedule artifact generation",
            description = "Schedule artifact generation. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Image Id", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "405", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "404", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/scheduleArtifactGenerationWS/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def scheduleArtifactGenerationWS() {
        scheduleArtifactGeneration()
    }

    @SSO
    def scheduleArtifactGeneration() {

        def imageInstance = Image.findByImageIdentifier(params.id as String, [ cache: true])
//        def userId = request.getHeader(ApiKeyInterceptor.API_KEY_HEADER_NAME)
        def userId = authService.userId
        def results = [success: true]

        if (params.id && !imageInstance) {
            results.success = false
            results.message = "Could not find image ${params.id}"
            flash.message  = results.message
            renderResults(results, HttpStatus.SC_BAD_REQUEST)
        } else {
            if (imageInstance) {
                imageService.scheduleArtifactGeneration(imageInstance.id, userId)
                results.message = "Image artifact generation scheduled for image ${imageInstance.id}"
            } else {
                def count = forEachImageId { imageId ->
                    imageService.scheduleArtifactGeneration(imageId, userId)
                }
                results.message = "Image artifact generation scheduled for ${count} images."
            }
            flash.message  = results.message
            renderResults(results)
        }
    }

    @Operation(
            method = "GET",
            summary = "Schedule keyword generation",
            description = "Schedule keyword generation. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Image Id", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "405", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/scheduleKeywordRegenerationWS/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def scheduleKeywordRegenerationWS() {
        scheduleKeywordRegeneration()
    }

    @SSO
    def scheduleKeywordRegeneration() {
        def imageInstance = Image.findByImageIdentifier(params.id as String, [ cache: true])
//        def userId = request.getHeader(ApiKeyInterceptor.API_KEY_HEADER_NAME)
        def userId = request.getRemoteUser()
        def results = [success:true]
        if (params.id && !imageInstance) {
            results.success = false
            results.message = "Could not find image ${params.id}"
            flash.message  = results.message
            renderResults(results, HttpStatus.SC_BAD_REQUEST)
        } else {
            if (imageInstance) {
                imageService.scheduleKeywordRebuild(imageInstance.id, userId)
                results.message = "Image keyword rebuild scheduled for image ${imageInstance.id}"
            } else {
                def imageList = Image.findAll()
                long count = 0
                imageList.each { image ->
                    imageService.scheduleKeywordRebuild(image.id, userId)
                    count++
                }
                results.message = "Image keyword rebuild scheduled for ${count} images."
            }
            flash.message  = results.message
            renderResults(results)
        }
    }

    @RequireApiKey(scopes = ['image-service/write'])
    def scheduleInboxPollWS() {
        scheduleInboxPoll()
    }

    @SSO
    def scheduleInboxPoll() {
        def results = [success:true]
        def userId =  authService.getUserId() ?: params.userId
        results.importBatchId = imageService.schedulePollInbox(userId)
        renderResults(results)
    }

    @Operation(
            method = "GET",
            summary = "Get tag model",
            description = "Get tag model. Required scopes: 'image-service/write'.",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "405", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/tagsWS")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes=["image-service/write"])
    def getTagModelWS() {
        getTagModel()
    }

    @SSO
    def getTagModel() {

        def newNode = { Tag tag, String label, boolean disabled = false ->
            [name: label, text: "${label}", children:[], 'icon': false, tagId: tag?.id, state:[disabled: disabled]]
        }

        def rootNode = newNode(null, "root")

        def tags
        if (params.q) {
            def query = params.q.toString().toLowerCase()
            def c = Tag.createCriteria()
            tags = c.list([order: 'asc', sort: 'path']) {
                like("path", "%${query}%")
            }

        } else {
            tags = Tag.list([order: 'asc', sort: 'path'])
        }

        tags.each { tag ->
            def path = tag.path
            if (path.startsWith(TagConstants.TAG_PATH_SEPARATOR)) {
                path = path.substring(TagConstants.TAG_PATH_SEPARATOR.length())
            }
            def bits = path.split(TagConstants.TAG_PATH_SEPARATOR)
            if (bits) {
                def parent = rootNode
                bits.eachWithIndex { pathElement, elementIndex ->
                    def child
                    child = parent.children?.find({ it.name == pathElement})
                    if (!child) {
                        boolean disabled = false
                        if (elementIndex < bits.size() - 1) {
                            disabled = true
                        }
                        child = newNode(tag, pathElement, disabled)
                        parent.children << child
                    }
                    parent = child
                }
            }
        }

        renderResults(rootNode.children)
    }

    @Operation(
            method = "PUT",
            summary = "Create tag by path",
            description = "Create tag by path. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "tagPath", in = QUERY, required = true, description = "Tag path. Paths separated by '/'. e.g. 'Birds/Colour/Red'", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "405", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/tagWS")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def createTagByPathWS() {
        createTagByPath()
    }

    @SSO
    def createTagByPath() {
        def success = false
        def tagPath = params.tagPath as String
        def tagId = 0
        if (tagPath) {
            def parent = Tag.get(params.int("parentTagId"))
            def tag = tagService.createTagByPath(tagPath, parent)
            success = tag != null
            tagId = tag.id
        }
        renderResults([success: success, tagId: tagId])
    }

    @Operation(
            method = "PUT",
            summary = "Move tag",
            description = "Move tag. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "targetTagId", in = QUERY, required = true, description = "Target Tag Id to move", schema = @Schema(implementation = String)),
                    @Parameter(name = "newParentTagId", in = QUERY, required = true, description = "New target parent tag Id", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "405", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/tag/moveWS")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def moveTagWS() {
        moveTag()
    }

    @SSO
    def moveTag() {

        def target = Tag.get(params.int("targetTagId"))
        def newParent = Tag.get(params.int("newParentTagId"))

        if (target) {
            tagService.moveTag(target, newParent)
            renderResults([success: true])
        } else {
            renderResults([success: false, message: "Tag not recognised"], HttpStatus.SC_NOT_FOUND)
        }
    }

    @Operation(
            method = "PUT",
            summary = "Rename tag",
            description = "Rename tag. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "tagID", in = PATH, required = true, description = "Tag ID", schema = @Schema(implementation = String)),
                    @Parameter(name = "name", in = QUERY, required = true, description = "New name", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/tag/{tagID}/renameWS")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def renameTagWS() {
        renameTag()
    }

    @SSO
    def renameTag() {
        def tag = Tag.get(params.int("tagID"))
        if (tag && params.name) {
            tagService.renameTag(tag, params.name)
            renderResults([success: true])
        } else {
            renderResults([success: false, message: "Tag not recognised"], HttpStatus.SC_NOT_FOUND)
        }
    }

    @Operation(
            method = "DELETE",
            summary = "Delete tag",
            description = "Delete tag. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "tagId", in = PATH, required = true, description = "Tag Id", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/tag/delete/{tagId}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def deleteTagWS() {
        deleteTag()
    }

    @SSO
    def deleteTag() {
        def tag = Tag.get(params.int("tagId"))
        if (tag) {
            tagService.deleteTag(tag)
            renderResults([success: true])
        } else {
            renderResults([success: false, message: "Tag not recognised"], HttpStatus.SC_NOT_FOUND)
        }
    }

    @Operation(
            method = "PUT",
            summary = "Tag an image",
            description = "Tag an image. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "tagId", in = PATH, required = true, description = "Tag Id", schema = @Schema(implementation = String)),
                    @Parameter(name = "imageId", in = PATH, required = true, description = "Image Id", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/tag/{tagId}/imageWS/{imageId}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def attachTagToImageWS() {
        attachTagToImage()
    }

    @SSO
    def attachTagToImage() {
        def success = false
        def message = ""
        def status = HttpStatus.SC_OK
        def image = Image.findByImageIdentifier(params.imageId as String, [ cache: true])
        def tag = Tag.get(params.int("tagId"))
        if (!image){
            message =  "Unrecognised image ID"
            status = HttpStatus.SC_NOT_FOUND
        } else if(!tag) {
            message = "Unrecognised tag ID"
            status = HttpStatus.SC_NOT_FOUND
        } else if (image && tag) {
            success = tagService.attachTagToImage(image, tag, authService.getUserId())
            status = HttpStatus.SC_OK
        }
        renderResults([success: success, message: message], status)
    }

    @Operation(
            method = "GET",
            summary = "Find images by keyword (a keyword is just the raw string of a tag)",
            description = "Find images by keyword (a keyword is just the raw string of a tag).",
            parameters = [
                    @Parameter(name = "keyword", in = PATH, required = true, description = "Keyword", schema = @Schema(implementation = String)),
                    @Parameter(name = "max", in = QUERY, required = false, description = "max results to return", schema = @Schema(implementation = Integer, defaultValue = "100")),
                    @Parameter(name = "offset", in = QUERY, required = false, description = "offset for paging", schema = @Schema(implementation = Integer, defaultValue = "0"))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/images/keyword/{keyword}")
    @Consumes("application/json")
    @Produces("application/json")
    def getImagesForKeyword(){
        QueryResults<Image> results = searchService.findImagesByKeyword(params.keyword, params)
        renderResults([
            keyword: params.keyword,
            totalImageCount: results.totalCount,
            images: results.list,
        ], 200, true)
    }

    @Operation(
            method = "GET",
            summary = "Find images by keyword (a keyword is just the raw string of a tag)",
            description = "Find images by keyword (a keyword is just the raw string of a tag).",
            parameters = [
                    @Parameter(name = "tagID", in = PATH, required = true, description = "Tag ID", schema = @Schema(implementation = String)),
                    @Parameter(name = "max", in = QUERY, required = false, description = "max results to return", schema = @Schema(implementation = Integer, defaultValue = "100")),
                    @Parameter(name = "offset", in = QUERY, required = false, description = "offset for paging", schema = @Schema(implementation = Integer, defaultValue = "0"))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/images/tag/{tagID}")
    @Consumes("application/json")
    @Produces("application/json")
    def getImagesForTag(){
        QueryResults<Image> results = searchService.findImagesByTagID(params.tagID, params)
        renderResults([
                tagID: params.tagID,
                totalImageCount: results.totalCount,
                images: results.list,
        ], 200, true)
    }

    @Operation(
            method = "DELETE",
            summary = "Detach tag from image",
            description = "Detach tag from image. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "tagId", in = PATH, required = true, description = "Tag Id", schema = @Schema(implementation = String)),
                    @Parameter(name = "imageId", in = PATH, required = true, description = "Image Id", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/tag/{tagId}/imageWS/{imageId}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def detachTagFromImageWS() {
        detachTagFromImage()
    }

    @SSO
    def detachTagFromImage() {
        def success = false
        def image = Image.findByImageIdentifier(params.imageId as String, [ cache: true])
        def tag = Tag.get(params.int("tagId"))
        if (image && tag) {
            success = tagService.detachTagFromImage(image, tag)
            renderResults([success: success])
        } else {
            renderResults([success: success, message:'Image or tag not found'], HttpStatus.SC_NOT_FOUND)
        }
    }

    @Operation(
            method = "GET",
            summary = "Get Image Details - optionally include tags and other metadata (e.g. EXIF)",
            description = "Get Image Details - optionally include tags and other metadata (e.g. EXIF).",
            parameters = [
                    @Parameter(name = "imageID", in = PATH, required = true, description = "Image ID", schema = @Schema(implementation = String)),
                    @Parameter(name = "includeTags", in = QUERY, required = false, description = "Include tags", schema = @Schema(implementation = Boolean)),
                    @Parameter(name = "includeMetadata", in = QUERY, required = false, description = "Include metadata", schema = @Schema(implementation = Boolean))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/image/{imageID}")
    @Consumes("application/json")
    @Produces("application/json")
    def getImageInfo() {
        def results = [success:false]
        def imageId = params.id ? params.id : params.imageID
        def image = Image.findByImageIdentifier(imageId as String, [ cache: true])
        if (image) {
            results.success = true
            imageService.addImageInfoToMap(image, results, params.boolean("includeTags"), params.boolean("includeMetadata"))
            renderResults(results, 200, true)
        } else {
            results["message"] = "image id not found"
            renderResults(results, HttpStatus.SC_NOT_FOUND, true)
        }
    }

    @Operation(
            method = "GET",
            summary = "Get Image PopUp details",
            description = "Get Image PopUp details.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Image ID", schema = @Schema(implementation = String)),
                    @Parameter(name = "includeTags", in = QUERY, required = false, description = "Include tags", schema = @Schema(implementation = Boolean)),
                    @Parameter(name = "includeMetadata", in = QUERY, required = false, description = "Include metadata", schema = @Schema(implementation = Boolean))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/imagePopupInfo/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    def imagePopupInfo() {
        def results = [success:false]
        def image = Image.findByImageIdentifier(params.id as String, [ cache: true])
        if (image) {
            results.success = true
            results.data = [:]
            imageService.addImageInfoToMap(image, results.data, false, false)
            results.link = createLink(controller: "image", action:'details', id: image.id)
            results.linkText = "Image details..."
            results.title = "Image properties"
            renderResults(results, 200, true)
        } else {
            results["message"] = "image id not found"
            renderResults(results, HttpStatus.SC_NOT_FOUND, true)
        }
    }

    private renderResults(Object results, int responseCode = 200, boolean sendAccessControlAllowOriginHeader = false) {
        response.status = responseCode
        withFormat {
            json {
                def jsonStr = results as JSON
                if (params.callback) {
                    response.setContentType("text/javascript")
                    render("${params.callback}(${jsonStr})")
                } else {
                    if (sendAccessControlAllowOriginHeader) {
                        response.addHeader("Access-Control-Allow-Origin", "*")
                    }
                    response.setContentType("application/json")
                    render(jsonStr)
                }
            }
            xml {
                if (sendAccessControlAllowOriginHeader) {
                    response.addHeader("Access-Control-Allow-Origin", "*")
                }
                render(results as XML)
            }
        }
    }

    @Operation(
            method = "GET",
            summary = "Repository statistics",
            description = "Repository statistics.",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/repositoryStatistics")
    @Consumes("application/json")
    @Produces("application/json")
    def getRepositoryStatistics() {
        def results = statsCache.get()
        renderResults(results, 200, true)
    }

    private def statsCache = Suppliers.memoizeWithExpiration(this.&getRepositoryStatisticsResultsInternal, 1, TimeUnit.MINUTES)

    private Map<String, Integer> getRepositoryStatisticsResultsInternal() {
        def results = [:]
        results.imageCount = Image.count()
        results.deletedImageCount = Image.countByDateDeletedIsNotNull()
        results.licenceCount = License.count()
        results.licenceMappingCount = LicenseMapping.count()
        return results
    }

    @Operation(
            method = "GET",
            summary = "Repository size statistics",
            description = "Repository size statistics.",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/repositoryStatistics")
    @Consumes("application/json")
    @Produces("application/json")
    def getRepositorySizeOnDisk() {
        def results = [ repoSizeOnDisk : ImageUtils.formatFileSize(imageStoreService.getRepositorySizeOnDisk()) ]
        renderResults(results, 200, true)
    }

    @Operation(
            method = "GET",
            summary = "Background queue statistics",
            description = "Background queue statistics.",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/backgroundQueueStats")
    @Consumes("application/json")
    @Produces("application/json")
    def getBackgroundQueueStats() {
        def results = [:]
        results.queueLength = imageService.getImageTaskQueueLength()
        results.tilingQueueLength = imageService.getTilingTaskQueueLength()
        results.batchUploads = batchService.getActiveBatchUploadCount()
        renderResults(results, 200, true)
    }

    def createSubimage() {
        def image = Image.findByImageIdentifier(params.id as String, [ cache: true])
        if (!image) {
            renderResults([success:false, message:"Image not found: ${params.id}"])
            return
        }

        if (!params.x || !params.y || !params.height || !params.width) {
            renderResults([success:false, message:"Rectangle not correctly specified. Use x, y, height and width params"])
            return
        }

        def x = params.int('x')
        def y = params.int('y')
        def height = params.int('height')
        def width = params.int('width')
        def title = params.title
        def description = params.description

        if (height == 0 || width == 0) {
            renderResults([success:false, message:"Rectangle not correctly specified. Height and width cannot be zero"])
            return
        }

        def userId = getUserIdForRequest(request)
        if (!userId) {
            renderResults([success:false, message:"User needs to be logged in to create sub image"])
            return
        }

        def subimage = imageService.createSubimage(image, x, y, width, height, userId, [title:title, description:description] )
        renderResults([success: subimage != null, subImageId: subimage?.imageIdentifier])
    }

    @Operation(
            method = "PUT",
            summary = "Create Subimage",
            description = "Create Subimage. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Image ID", schema = @Schema(implementation = String)),
                    @Parameter(name = "x", in = QUERY, required = true, description = "x co-ordinate", schema = @Schema(implementation = Integer)),
                    @Parameter(name = "y", in = QUERY, required = true, description = "y co-ordinate", schema = @Schema(implementation = Integer)),
                    @Parameter(name = "height", in = QUERY, required = true, description = "sub image height", schema = @Schema(implementation = Integer)),
                    @Parameter(name = "width", in = QUERY, required = true, description = "sub image width", schema = @Schema(implementation = Integer)),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tag services"]
    )
    @Path("/ws/subimage/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def subimage() {
        def image = Image.findByImageIdentifier(params.id as String, [ cache: true])
        if (!image) {
            renderResults([success:false, message:"Image not found: ${params.id}"], HttpStatus.SC_NOT_FOUND)
            return
        }

        if (!params.x || !params.y || !params.height || !params.width) {
            renderResults([success:false, message:"Rectangle not correctly specified. Use x, y, height and width params"], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def x = params.int('x')
        def y = params.int('y')
        def height = params.int('height')
        def width = params.int('width')
        def title = params.title
        def description = params.description

        if (height == 0 || width == 0) {
            renderResults([success:false, message:"Rectangle not correctly specified. Height and width cannot be zero"], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def userId = getUserIdForRequest(request)
        if(!userId){
            renderResults([success:false, message:"User needs to be logged in to create sub image"], HttpStatus.SC_FORBIDDEN)
            return
        }

        def subimage = imageService.createSubimage(image, x, y, width, height, userId, [title:title, description:description] )

        if (subimage && subimage.imageIdentifier){
            renderResults([success: true, subImageId: subimage.imageIdentifier], HttpStatus.SC_OK)
        } else {
            renderResults([success: false], HttpStatus.SC_INTERNAL_SERVER_ERROR)
        }

    }


    def getSubimageRectangles() {
        // TODO Where does this get used?
        def image = Image.findByImageIdentifier(params.id as String, [ cache: true])
        if (!image) {
            renderResults([success:false, message:"Image not found: ${params.id}"])
            return
        }

        def subimages = Subimage.findAllByParentImage(image)
        def results = [success: true, subimages: []]
        subimages.each { subImageRect ->
            def sub = subImageRect.subimage
            results.subimages << [imageId: sub.imageIdentifier, x: subImageRect.x, y: subImageRect.y, height: subImageRect.height, width: subImageRect.width]
        }
        renderResults(results, 200, true)
    }

    def addUserMetadataToImage() {
        // TODO Where does this get used?
        def image = Image.findByImageIdentifier(params.id as String, [ cache: true])
        if (!image) {
            renderResults([success:false, message:"Image not found: ${params.id}"], HttpStatus.SC_NOT_FOUND)
            return
        }

        def key = params.key
        if (!key) {
            renderResults([success:false, message:"Metadata key/name not supplied!"], HttpStatus.SC_BAD_REQUEST)
            return
        }
        def value = params.value
        if (!value) {
            renderResults([success:false, message:"Metadata value not supplied!"], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def userId = getUserIdForRequest(request)

        def success = imageService.setMetaDataItem(image, MetaDataSourceType.UserDefined, key, value, userId)
        imageService.scheduleImageIndex(image.id)

        renderResults([success:success])
    }

    @Operation(
            method = "GET",
            summary = "Search for images",
            description = "Search for images.",
            parameters = [
                    @Parameter(name = "q", in = QUERY, required = true, description = "Query", schema = @Schema(implementation = String)),
                    @Parameter(name = "fq", in = QUERY, required = true, description = "Filter query", schema = @Schema(implementation = String)),
                    @Parameter(name = "max", in = QUERY, required = false, description = "Max results to return", schema = @Schema(implementation = Integer)),
                    @Parameter(name = "offset", in = QUERY, required = false, description = "Search Offset", schema = @Schema(implementation = Integer)),
                    @Parameter(name = "sort", in = QUERY, required = false, description = "Sort field", schema = @Schema(implementation = String)),
                    @Parameter(name = "order", in = QUERY, required = false, description = "Sort order", schema = @Schema(implementation = String, allowableValues = ['asc', 'desc'])),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
            ],
            tags = ["Image search"]
    )
    @Path("/ws/search")
    @Consumes("application/json")
    @Produces("application/json")
    def search(){
        def ct = new CodeTimer("Image list")

        params.offset = params.offset ?: 0
        params.max = params.max ?: 50
        params.sort = params.sort ?: 'dateUploaded'
        params.order = params.order ?: 'desc'

        def query = params.q as String

        QueryResults<Image> results = searchService.search(params)

        def filterQueries = params.findAll { it.key == 'fq' && it.value }

        ct.stop(true)
        renderResults([
          q: query,
          totalImageCount: results.totalCount,
          filters: filterQueries,
          searchCriteria: searchService.getSearchCriteriaList(),
          facets: results.aggregations,
          images: results.list,
        ], 200, true)
    }

    @Operation(
            method = "GET",
            summary = "Facet search for images",
            description = "Facet search for images.",
            parameters = [
                    @Parameter(name = "facet", in = QUERY, required = true, description = "Facet", schema = @Schema(implementation = String)),
                    @Parameter(name = "q", in = QUERY, required = true, description = "Query", schema = @Schema(implementation = String)),
                    @Parameter(name = "fq", in = QUERY, required = true, description = "Filter query", schema = @Schema(implementation = String)),
                    @Parameter(name = "max", in = QUERY, required = false, description = "Max results to return", schema = @Schema(implementation = Integer)),
                    @Parameter(name = "offset", in = QUERY, required = false, description = "Search Offset", schema = @Schema(implementation = Integer)),
                    @Parameter(name = "sort", in = QUERY, required = false, description = "Sort field", schema = @Schema(implementation = String)),
                    @Parameter(name = "order", in = QUERY, required = false, description = "Sort order", schema = @Schema(implementation = String, allowableValues = ['asc', 'desc'])),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
            ],
            tags = ["Image search"]
    )
    @Path("/ws/facet")
    @Consumes("application/json")
    @Produces("application/json")
    def facet(){

        if(!params.facet){
            response.sendError(HttpStatus.SC_BAD_REQUEST, "Facet parameter is required")
            return
        }

        params.offset = params.offset ?: 0
        params.max = params.max ?: 50
        params.sort = params.sort ?: 'dateUploaded'
        params.order = params.order ?: 'desc'
        def results = searchService.facet(params)
        renderResults(results.aggregations, 200, true)
    }

    private getUserIdForRequest(HttpServletRequest request) {

        //check for API access
        if (grailsApplication.config.getProperty('security.cas.disableCAS', Boolean, false)){
            return "-1"
        }

        def userId = request.remoteUser
        // First check the CAS filter cookie thing
//        def userId = authService.getUserId()

        userId
    }

    @Operation(
            method = "POST",
            summary = "Bulk Add User Metadata To Image",
            description = "Bulk Add User Metadata To Image. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Image ID", schema = @Schema(implementation = String)),
            ],
            requestBody = @RequestBody(
                    required = true,
                    description = "JSON Document of fields names to field values",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = Map))
            ),
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Image metadata"]
    )
    @Path("/ws/bulkAddUserMetadataToImage/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def bulkAddUserMetadataToImage(String id) {
        def image = Image.findByImageIdentifier(id, [ cache: true])
        def userId = getUserIdForRequest(request)
        if (!image) {
            renderResults([success:false, message:"Image not found: ${params.id}"])
            return
        }

        def metadata = request.getJSON() as Map<String, String>
        def results = imageService.setMetadataItems(image, metadata, MetaDataSourceType.UserDefined, userId)
        imageService.scheduleImageIndex(image.id)

        renderResults([success:results != null])
    }

    @Operation(
            method = "POST",
            summary = "Remove User Metadata from Image",
            description = "Remove User Metadata from Image. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Image ID", schema = @Schema(implementation = String)),
                    @Parameter(name = "key", in = QUERY, required = true, description = "Metadata key/name", schema = @Schema(implementation = String)),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Image metadata"]
    )
    @Path("/ws/removeUserMetadataFromImageWS/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes = ['image-service/write'])
    def removeUserMetadataFromImageWS() {
        removeUserMetadataFromImage()
    }

    @SSO
    def removeUserMetadataFromImage() {
        def image = Image.findByImageIdentifier(params.id as String, [ cache: true])
        if (!image) {
            renderResults([success:false, message:"Image not found: ${params.id}"], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def key = params.key
        if (!key) {
            renderResults([success:false, message:"Metadata key/name not supplied!"], HttpStatus.SC_BAD_REQUEST)
            return
        }
        def userId = getUserIdForRequest(request)
        def success = imageService.removeMetaDataItem(image, key, MetaDataSourceType.UserDefined, userId)

        renderResults([success: success])
    }

    @Operation(
            method = "GET",
            summary = "Get Metadata",
            description = "Get Metadata.",
            parameters = [
                    @Parameter(name = "source", in = QUERY, required = false, description = "Only return metadata items with this source", schema = @Schema(implementation = MetaDataSourceType)),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])],
            tags = ["Image metadata"]
    )
    @Path("/ws/getMetadataKeys")
    @Consumes("application/json")
    @Produces("application/json")
    def getMetadataKeys() {

        def source = params.source as MetaDataSourceType
        def results
        def c = ImageMetaDataItem.createCriteria()

        if (source) {
            results = c.list {
                eq("source", source)
                projections {
                    distinct("name")
                }
            }

        } else {
            results = c.list {
                projections {
                    distinct("name")
                }
            }
        }

        renderResults(results?.sort { it?.toLowerCase() }, 200, true)
    }

    @Operation(
            method = "GET",
            summary = "Get ImageLinks For MetaData Values",
            description = "Get ImageLinks For MetaData Values.",
            parameters = [
                    @Parameter(name = "key", in = QUERY, required = true, description = "Metadata key", schema = @Schema(implementation = String)),
                    @Parameter(name = "q", in = QUERY, required = true, description = "Query", schema = @Schema(implementation = MetaDataSourceType)),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
            ],
            tags = ["Image metadata"]
    )
    @Path("/ws/getImageLinksForMetaDataValues")
    @Consumes("application/json")
    @Produces("application/json")
    def getImageLinksForMetaDataValues() {

        def key = params.key as String
        if (!key) {
            render([success:false, message:'No key parameter supplied'], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def query = (params.q ?: params.value) as String

        if (!query) {
            render([success:false, message:'No q or value parameter supplied'], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def images = searchService.findImagesByMetadata(key, [query], params)
        def results = [images:[], success: true, count: images.totalCount]

        if(images.get(key) != null) {
            def keyValues = imageService.getMetadataItemValuesForImages(images, key)

            images.each { image ->
                def info = imageService.getImageInfoMap(image)
                info[key] = keyValues[image.id]
                results.images << info
            }
        }

        renderResults(results, 200, true)
    }

    @Operation(
            method = "POST",
            summary = "Get images for a list of image IDs",
            description = "Get images for a list of image IDs.",
            requestBody = @RequestBody(
                    required = true,
                    description = "JSON Document of with a single field, imageIds, which contains a list of image ids.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = Map))
            ),
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
            ],
            tags = ["Image metadata"]
    )
    @Path("/ws/getImageInfoForIdList")
    @Consumes("application/json")
    @Produces("application/json")
    def getImageInfoForIdList() {

        def query = request.JSON

        if (query) {

            List<String> imageIds = (query.imageIds as List)?.collect { it as String }

            if (!imageIds) {
                renderResults([success:false, message:'You must supply a list of image IDs (imageIds) to search for!'], HttpStatus.SC_BAD_REQUEST, true)
                return
            }

            def results =  [:]
            def errors = []

            imageIds.each { imageId ->

                def image = Image.findByImageIdentifier(imageId, [ cache: true])

                if (image) {
                    def map = imageService.getImageInfoMap(image)
                    results[imageId] = map
                } else {
                    errors << imageId
                }
            }

            renderResults([success: true, results: results, invalidImageIds: errors], 200, true)
            return
        }
        renderResults([success:false, message:'POST with content type "application/JSON" required.'], HttpStatus.SC_BAD_REQUEST, true)
    }

    @Operation(
            method = "GET",
            summary = "Retrieve a list of recognised Licences",
            description = "Retrieve a list of recognised Licences.",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])],
            tags = ["Licences"]
    )
    @Path("/ws/licence")
    @Consumes("application/json")
    @Produces("application/json")
    def licence(){
        def licenses = License.findAll()
        renderResults (licenses, 200, true)
    }

    @Operation(
            method = "GET",
            summary = "Retrieve a list of string to licence mappings in use by the image service",
            description = "Retrieve a list of string to licence mappings in use by the image service.",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])],
            tags = ["Licences"]
    )
    @Path("/ws/licenceMapping")
    def licenceMapping(){
        def licenses = LicenseMapping.findAll()
        renderResults (licenses, 200, true)
    }

    @Operation(
            method = "POST",
            summary = "Find image by original filename (URL)",
            description = "Find image by original filename (URL).",
            requestBody = @RequestBody(
                    required = true,
                    description = "JSON Document of with a single field, filenames, which contains a list of file names.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = Map))
            ),
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/findImagesByOriginalFilename")
    @Consumes("application/json")
    @Produces("application/json")
    def findImagesByOriginalFilename() {

        CodeTimer ct = new CodeTimer("Original file lookup")

        def query = request.JSON

        if (query) {

            def filenames = query.filenames as List<String>

            if (!filenames) {
                renderResults([success:false, message:'You must supply a list of filenames to search for!'])
                return
            }

            def results =  [:]

            filenames.each { filename ->

                def images = searchService.findImagesByOriginalFilename(filename, params)
                def list = []
                images?.each { image ->
                    def map = imageService.getImageInfoMap(image)
                    list << map
                }
                results[filename] = [count: list.size(), images: list]
            }

            renderResults([success: true, results: results])

        } else {
            renderResults([success:false, message:'POST with content type "application/JSON" required.'])
        }
        ct.stop(true)
    }

    @Operation(
            method = "POST",
            summary = "Find images by image metadata - deprecated. Use /search instead",
            description = "Find images by image metadata - deprecated. Use /search instead.",
            deprecated = true,
            requestBody = @RequestBody(
                    required = true,
                    description = "JSON doc with a string \"key\" and a string list \"values\" fields.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = Map))
            ),
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/findImagesByMetadata")
    @Consumes("application/json")
    @Produces("application/json")
    @Deprecated
    def findImagesByMetadata() {

        def query = request.JSON

        if (query) {

            def key = query.key as String
            def values = query.values as List<String>

            if (!key) {
                renderResults([success:false, message:'You must supply a metadata key!'])
                return
            }

            if (!values) {
                renderResults([success:false, message:'You must supply a values list!'])
                return
            }

            def results = elasticSearchService.searchByMetadata(key, values, params)
            def totalCount = 0
            results.values().each {
                totalCount = totalCount + it.size()
            }

            //add additional fields for backwards compatibility
            results.each { id, metadataItems ->
                metadataItems.each { metadata ->
                    metadata["imageId"] = metadata.imageIdentifier
                    metadata["tileZoomLevels"] = metadata.zoomLevels
                    metadata["filesize"] = metadata.fileSize
                    metadata["imageUrl"] = imageService.getImageUrl(metadata.imageIdentifier)
                    metadata["largeThumbUrl"] = imageService.getImageThumbLargeUrl(metadata.imageIdentifier)
                    metadata["squareThumbUrl"]  = imageService.getImageSquareThumbUrl(metadata.imageIdentifier)
                    metadata["thumbUrl"]  = imageService.getImageThumbUrl(metadata.imageIdentifier)
                    metadata["tilesUrlPattern"]  = imageService.getImageTilesUrlPattern(metadata.imageIdentifier)
                    metadata.remove("fileSize")
                    metadata.remove("zoomLevels")
                    metadata.remove("storageLocationId")
                    metadata.remove("storageLocation")
                }
            }

            renderResults([success: true, images: results, count: totalCount])
            return
        }

        renderResults([success:false, message:'POST with content type "application/JSON" required.'])
    }

    def getNextTileJob() {
        def results = imageService.createNextTileJob()
        renderResults(results)
    }

    @Operation(
            method = "POST",
            summary = "Cancel Tile Job",
            description = "Cancel Tile Job. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "jobTicket", in = QUERY, required = true, description = "Job Ticket", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Tile jobs"]
    )
    @Path("/ws/cancelTileJob")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes=["image-service/write"])
    def cancelTileJob() {

        def userId = getUserIdForRequest(request)
        def ticket = params.jobTicket ?: params.ticket
        if (!ticket) {
            renderResults([success:false, message:'No job ticket specified'])
            return
        }

        def job = OutsourcedJob.findByTicket(ticket)
        if (!job) {
            renderResults([success:false, message:'No such ticket or ticket expired.'])
            return
        }

        logService.log("Cancelling job (Ticket: ${job.ticket} for image ${job.image.imageIdentifier}")

        // Push the job back on the queue
        if (job.taskType == ImageTaskType.TMSTile) {
            imageService.scheduleTileGeneration(job.image.id, userId)
        }

        job.delete()
    }

    def postJobResults() {
        def ticket = params.jobTicket ?: params.ticket
        if (!ticket) {
            renderResults([success:false, message:'No job ticket specified'], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def job = OutsourcedJob.findByTicket(ticket)
        if (!job) {
            renderResults([success:false, message:'No such ticket or ticket expired.'], HttpStatus.SC_BAD_REQUEST)
            return
        }

        def zoomLevels = params.int("zoomLevels")
        if (!zoomLevels) {
            renderResults([success:false, message:'No zoomLevels supplied.'], HttpStatus.SC_BAD_REQUEST)
            return
        }

        if (job.taskType == ImageTaskType.TMSTile) {
            // Expect a multipart file request
            MultipartFile file = request.getFile('tilesArchive')

            if (!file || file.size == 0) {
                renderResults([success:false, message:'tilesArchive param not present. Expected multipart file.'], HttpStatus.SC_BAD_REQUEST)
                return
            }

            if (imageStoreService.storeTilesArchiveForImage(job.image, file)) {
                job.image.zoomLevels = zoomLevels
                job.delete()
                renderResults([success: true])
                return
            } else {
                renderResults([success:false, message: "Error storing tiles for image!"], HttpStatus.SC_INTERNAL_SERVER_ERROR)
                return
            }
        }
        renderResults([success: false, message:'Unhandled task type'], HttpStatus.SC_BAD_REQUEST)
    }

    /**
     * Update an image's metadata.
     *
     * @return
     */
    @Operation(
            method = "POST",
            summary = "Update image metadata using a JSON payload.",
            description = "Update image metadata using a JSON payload. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name = "imageIdentifier", in = QUERY, required = true, description = "Image Identifier", schema = @Schema(implementation = String)),
                    @Parameter(name = "metadata", in = QUERY, required = false, description = "Metadata as a JSON document, encoded as a POST param", schema = @Schema(implementation = String)),
                    @Parameter(name = "tags", in = QUERY, required = false, description = "List of tags", array = @ArraySchema(schema = @Schema(implementation = String))),
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/updateMetadata")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes=["image-service/write"])
    def updateMetadata(){

        CodeTimer ct = new CodeTimer("Update Image metadata ${params.imageIdentifier}")

        Image image = Image.findByImageIdentifier(params.imageIdentifier, [ cache: true])
        if (image){
            def userId = getUserIdForRequest(request)
            def metadata = {
                if(params.metadata){
                    JSON.parse(params.metadata as String) as Map
                } else {
                    [:]
                }
            }.call()

            imageService.updateImageMetadata(image, metadata)

            List tags = params.tags instanceof String ? [params.tags] : params.tags as List
            tagService.updateTags(image, tags as List, userId)

            renderResults([success: true])
        } else {
            renderResults([success: false, message: 'Image not found'], HttpStatus.SC_NOT_FOUND)
        }

        ct.stop(true)
    }

    /**
     * Main web service for image upload.
     *
     * @return
     */
    @Operation(
            method = "POST",
            summary = "Upload a single image, with by URL or multipart HTTP file upload.\nFor multipart the image must be posted in a 'image' property.",
            description = """Required scopes: 'image-service/write'. The following metadata properties can be set/updated:    
                
                audience      - http://purl.org/dc/terms/audience
                contributor   - http://purl.org/dc/terms/contributor
                creator       - http://purl.org/dc/terms/creator
                created       - http://purl.org/dc/terms/created  
                description   - http://purl.org/dc/terms/description 
                format        - http://purl.org/dc/terms/format (see https://www.iana.org/assignments/media-types/media-types.xhtml) 
                license       - http://purl.org/dc/terms/license
                publisher     - http://purl.org/dc/terms/publisher
                references    - http://purl.org/dc/terms/references   
                rights        - http://purl.org/dc/terms/rights 
                rightsHolder  - http://purl.org/dc/terms/rightsHolder
                source        - http://purl.org/dc/terms/source
                title         - http://purl.org/dc/terms/title
                type          - http://purl.org/dc/terms/type""",
            parameters = [
                    @Parameter(name = "metadata", in = QUERY, required = false, description = "Metadata as a JSON document, encoded as a POST param", schema = @Schema(implementation = String)),
                    @Parameter(name = "tags", in = QUERY, required = false, description = "List of tags", array = @ArraySchema(schema = @Schema(implementation = String))),
                    @Parameter(name = "imageUrl", in = QUERY, required = false, description = "An URL to be used to load the image from, use in lieu of sending a multipart upload", schema = @Schema(implementation = String)),
            ],
            requestBody = @RequestBody(
                    required = false,
                    description = "Multipart file upload, use image for the part name - TODO This needs properties for the multipart",
                    content = @Content(mediaType = "multipart/form-data", schema = @Schema(type = 'object', implementation = String, format = 'binary'))
            ),
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "500", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Upload"]
    )
    @Path("/ws/uploadImage")
    @Consumes("multipart/form-data")
    @Produces("application/json")
    @RequireApiKey(scopes=["image-service/write"])
    def uploadImage() {
        // Expect a multipart file request
        try {
            ImageStoreResult storeResult = null

            CodeTimer storeTimer = new CodeTimer("Store image")

            def userId = getUserIdForRequest(request)
            def url = params.imageUrl ?: params.url
            def metadata = {
                if (params.metadata) {
                    JSON.parse(params.metadata as String) as Map
                } else {
                    [:]
                }
            }.call()

            if (url) {
                // Image is located at an endpoint, and we need to download it first.
                storeResult = imageService.storeImageFromUrl(url, userId, metadata)
                if (!storeResult || !storeResult.image) {
                    renderResults([success: false, message: "Unable to retrieve image from ${url}"], HttpStatus.SC_BAD_REQUEST)
                }
            } else {
                // it should contain a file parameter
                if (request.metaClass.respondsTo(request, 'getFile', String)) {
                    MultipartFile file = request.getFile('image')
                    if (!file) {
                        renderResults([success: false, message: 'image parameter not found. Please supply an image file.'], HttpStatus.SC_BAD_REQUEST)
                        return
                    }

                    if (file.size == 0) {
                        renderResults([success: false, message: 'the supplied image was empty. Please supply an image file.'], HttpStatus.SC_BAD_REQUEST)
                        return
                    }

                    storeResult = imageService.storeImage(file, userId, metadata)
                } else {
                    renderResults([success: false, message: "No url parameter, therefore expected multipart request!"], HttpStatus.SC_BAD_REQUEST)
                }
            }

            storeTimer.stop()

            if (storeResult && storeResult.image) {

                CodeTimer ct = new CodeTimer("Setting Image metadata ${storeResult.image.imageIdentifier}")

                List tags = params.tags instanceof String ? [params.tags] : params.tags as List
                tagService.updateTags(storeResult.image, tags, userId)

                if (storeResult.alreadyStored) {
                    //reindex if already stored
                    imageService.scheduleImageIndex(storeResult.image.id)
                } else {
                    if (storeResult.image) {
                        imageService.schedulePostIngestTasks(storeResult.image.id, storeResult.image.imageIdentifier, storeResult.image.originalFilename, userId)
                    } else {
                        imageService.scheduleNonImagePostIngestTasks(storeResult.image.id)
                    }
                }

                ct.stop(true)

                renderResults([success: true, imageId: storeResult.image?.imageIdentifier, alreadyStored: storeResult.alreadyStored])
            } else {
                renderResults([success: false, message: "Failed to store image!"], 500)
            }
        } catch (Exception e){
            log.error("Problem storing image " + e.getMessage(), e)
            renderResults([success: false, message: "Failed to store image!"], 500)
        }
    }

    @RequireApiKey(scopes=["image-service/write"])
    def uploadImageAI() {

        MultipartFile file
        def url = params.imageUrl ?: params.url
        if (!url) {
            if (request.metaClass.respondsTo(request, 'getFile', String)) {
                file = request.getFile('image')
            }
        }
        if(!url && !file) {
            renderResults([success: false, message: "No url parameter, therefore expected multipart request!"], HttpStatus.SC_BAD_REQUEST)
            return
        }
        List inappropriateLabels = imageRecognitionService.checkImageContent(file, url)
        if(inappropriateLabels) {
            renderResults([success: false, message: "Detected inappropriate content: $inappropriateLabels"], HttpStatus.SC_BAD_REQUEST)
            return
        }
        else {
            uploadImage()
        }
    }

    @Operation(
            method = "POST",
            summary = "Asynchronous upload images by supplying a list of URLs in  a JSON  payload",
            description = """Asynchronous upload images by supplying a list of URLs in  a JSON  payload. Required scopes: 'image-service/write'. JSON document with a list of images to upload.  Provide a list of objects under the images key with the follow properties:
                
                audience      - http://purl.org/dc/terms/audience
                contributor   - http://purl.org/dc/terms/contributor
                creator       - http://purl.org/dc/terms/creator
                created       - http://purl.org/dc/terms/created  
                description   - http://purl.org/dc/terms/description 
                format        - http://purl.org/dc/terms/format (see https://www.iana.org/assignments/media-types/media-types.xhtml) 
                license       - http://purl.org/dc/terms/license
                publisher     - http://purl.org/dc/terms/publisher
                references    - http://purl.org/dc/terms/references   
                rights        - http://purl.org/dc/terms/rights 
                rightsHolder  - http://purl.org/dc/terms/rightsHolder
                source        - http://purl.org/dc/terms/source
                title         - http://purl.org/dc/terms/title
                type          - http://purl.org/dc/terms/type""",
            requestBody = @RequestBody(
                    required = true,
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = Map))
            ),
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "404", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Upload"]
    )
    @Path("/ws/uploadImagesFromUrls")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes=["image-service/write"])
    def uploadImagesFromUrls() {

        def userId = getUserIdForRequest(request)
        def body = request.JSON

        if (body) {

            List<Map<String, String>> imageList = body.images

            if (!imageList) {
                renderResults(
                        [success:false,
                         message:'You must supply a list of objects called "images", each of which' +
                                 ' must contain a "sourceUrl" key, along with optional meta data items!'],
                        HttpStatus.SC_BAD_REQUEST
                )
                return
            }

            //validate post
            def invalidCount = 0
            imageList.each { srcImage ->
                if(!srcImage.sourceUrl &&  !srcImage.imageUrl){
                    invalidCount += 1
                }
            }

            if (invalidCount) {
                renderResults(
                    [success:false,
                     message: 'You must supply a list of objects called "images", each of which' +
                             ' must contain a "sourceUrl" key, along with optional meta data items. Invalid submissions:' + invalidCount],
                    HttpStatus.SC_BAD_REQUEST
                )
                return
            }

            // first create the images
            def results = imageService.batchUpdate(imageList, userId)

            imageList.each { srcImage ->
                def newImage = results[srcImage.sourceUrl ?: srcImage.imageUrl]
                if (newImage && newImage.success) {
                    imageService.setMetadataItems(newImage.image, srcImage, MetaDataSourceType.SystemDefined, userId)
                    imageService.scheduleArtifactGeneration(newImage.image.id, userId)
                    imageService.scheduleImageIndex(newImage.image.id)
                    newImage.image = null
                }
            }

            renderResults([success: true, results: results])
        } else {
            renderResults([success:false, message:'POST with content type "application/JSON" required.'], HttpStatus.SC_BAD_REQUEST)
        }
    }

    def calibrateImageScale() {
        def userId = getUserIdForRequest(request)
        def image = Image.findByImageIdentifier(params.imageId, [ cache: true])
        def units = params.units ?: "mm"
        def pixelLength = params.double("pixelLength") ?: 0
        def actualLength = params.double("actualLength") ?: 0
        if (image && units && pixelLength && actualLength) {
            def pixelsPerMM = imageService.calibrateImageScale(image, pixelLength, actualLength, units, userId)
            renderResults([success: true, pixelsPerMM:pixelsPerMM, message:"Image is scaled at ${pixelsPerMM} pixels per mm"])
        } else {
            renderResults([success: false, message: 'Missing one or more required parameters: imageId, pixelLength, actualLength, units'])
        }
    }

    def resetImageCalibration() {
        def image = Image.findByImageIdentifier(params.imageId, [ cache: true])
        if (image) {
            imageService.resetImageLinearScale(image)
            renderResults([success: true, message:"Image linear scale has been reset"])
            return
        }
        renderResults([success:false, message:'Missing one or more required parameters: imageId, pixelLength, actualLength, units'])
    }

    def setHarvestable() {
        def userId = getUserIdForRequest(request)
        def image = Image.findByImageIdentifier(params.imageId, [ cache: true])
        if (image) {
            imageService.setHarvestable(image, (params.value ?: params.harvest ?: "").toBoolean(), userId)
            renderResults([success: true, message:"Image harvestable now set to ${image.harvestable}", harvestable: image.harvestable])
        } else {
            renderResults([success:false, message:'Missing one or more required parameters: imageId, value'])
        }
    }

    @Operation(
            method = "POST",
            summary = "Schedule upload from URLs",
            description = """Schedule upload from URLs. Required scopes: 'image-service/write'. JSON document with a list of images to upload.  Provide a list of objects under the images key with the follow properties:
                
                imageUrl      - The image url
                audience      - http://purl.org/dc/terms/audience
                contributor   - http://purl.org/dc/terms/contributor
                creator       - http://purl.org/dc/terms/creator
                created       - http://purl.org/dc/terms/created
                description   - http://purl.org/dc/terms/description
                format        - http://purl.org/dc/terms/format (see https://www.iana.org/assignments/media-types/media-types.xhtml)
                license       - http://purl.org/dc/terms/license
                publisher     - http://purl.org/dc/terms/publisher
                references    - http://purl.org/dc/terms/references
                rights        - http://purl.org/dc/terms/rights
                rightsHolder  - http://purl.org/dc/terms/rightsHolder
                source        - http://purl.org/dc/terms/source
                title         - http://purl.org/dc/terms/title
                type          - http://purl.org/dc/terms/type""",
            requestBody = @RequestBody(
                    required = true,
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = Map))
            ),
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/write"])
            ],
            tags = ["Upload"]
    )
    @Path("/ws/scheduleUploadFromUrls")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes=["image-service/write"])
    def scheduleUploadFromUrls() {

        def userId = getUserIdForRequest(request)
        def body = request.JSON

        if (body) {
            List<Map<String, String>> imageList = body.images
            if (!imageList) {
                renderResults([success:false, message:'You must supply a list of objects called "images", each of which must contain a "sourceUrl" key, along with optional meta data items!'], HttpStatus.SC_BAD_REQUEST)
                return
            }

            def batchId = batchService.createNewBatch()
            int imageCount = 0
            imageList.each { srcImage ->
                if (!srcImage.containsKey("importBatchId")) {
                    srcImage["importBatchId"] = batchId
                }
                batchService.addTaskToBatch(batchId, new UploadFromUrlTask(srcImage, imageService, userId))
                imageCount++
            }

            renderResults([success: true, message: "${imageCount} urls scheduled for upload (batch id ${batchId}).", batchId: batchId])
            return
        }

        renderResults([success:false, message:'POST with content type "application/JSON" required.'], HttpStatus.SC_BAD_REQUEST)
    }

    @Operation(
            method = "GET",
            summary = "Get batch status for a batch upload of images",
            description = "Get batch status for a batch upload of images. Required scopes: 'image-service/read'.",
            parameters = [
                    @Parameter(name = "batchId", in = QUERY, required = true, description = "The batch id", schema = @Schema(implementation = String))],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
                    @ApiResponse(responseCode = "403",headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            security = [
                    @SecurityRequirement(name="openIdConnect", scopes=["image-service/read"])
            ],
            tags = ["Upload"]
    )
    @Path("/ws/getBatchStatus")
    @Consumes("application/json")
    @Produces("application/json")
    @RequireApiKey(scopes=["image-service/read"])
    def getBatchStatus() {
        def status = batchService.getBatchStatus(params.batchId)
        if (status) {
            renderResults([success:true, taskCount: status.taskCount, tasksCompleted: status.tasksCompleted, batchId: status.batchId, timeStarted: status.timeStarted.getTime(), timeFinished: status.timeFinished?.getTime() ?: 0])
        } else {
            renderResults([success: false, message: 'Missing or invalid batchId'], HttpStatus.SC_BAD_REQUEST)
        }
    }

    @Operation(
            method = "GET",
            summary = "List the recognised darwin core terms",
            description = "List the recognised darwin core terms",
            parameters = [
                    @Parameter(name = "q", in = QUERY, required = false, description = "Optional Darwin Core field name to query for", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])],
            tags = ["JSON services for accessing and updating metadata"]
    )
    @Path("/ws/darwinCoreTerms")
    @Consumes("application/json")
    @Produces("application/json")
    def darwinCoreTerms() {
        def terms = []

        def filter = params.q ? { DarwinCoreField dwc -> dwc.name().toLowerCase().contains(params.q.toLowerCase()) } : { DarwinCoreField dwc -> true}

        DarwinCoreField.values().each {
            if (filter(it)) {
                terms.add([name: it.name(), label: it.label])
            }
        }
        renderResults(terms, 200, true)
    }

    def harvest() {

        def harvestResults = imageService.getHarvestTabularData()

        if (harvestResults) {
            response.setHeader("Content-disposition", "attachment;filename=images-harvest.csv")
            response.contentType = "text/csv"

            def bos = new OutputStreamWriter(response.outputStream)

            def writer = new CSVWriter(bos, {
                for (int i = 0; i < harvestResults.columnHeaders.size(); ++i) {
                    def col = harvestResults.columnHeaders[i]
                    "${col}" {
                        it[col] ?: ""
                    }
                }
            })

            harvestResults.data.each {
                writer << it
            }

            bos.flush()
            bos.close()
        } else {
            renderResults([success:"false", message:'No harvestable images found'], HttpStatus.SC_BAD_REQUEST)
        }
    }

    @Operation(
            method = "GET",
            summary = "Export CSV of entire image catalogue",
            description = "Export CSV of entire image catalogue",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/gzip", schema = @Schema(implementation = Map, format= 'binary'))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])
            ],
            tags = ["Export"]
    )
    @Path("/ws/exportCSV")
    @Consumes("application/json")
    @Produces("application/gzip")
    def exportCSV(){
        response.setHeader("Content-disposition", "attachment;filename=images-export.csv.gz")
        response.contentType = "application/gzip"
        def bos = new GZIPOutputStream(response.outputStream)
        imageService.exportCSV(bos)
        bos.flush()
        bos.close()
    }

    @Operation(
            method = "GET",
            summary = "Export CSV of URL to imageIdentifier mappings",
            description = "Export CSV of URL to imageIdentifier mappings.",
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/gzip", schema = @Schema(implementation = Map, format= 'binary'))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])],
            tags = ["Export"]
    )
    @Path("/ws/exportMapping")
    @Consumes("application/json")
    @Produces("application/gzip")
    def exportMapping(){
        response.setHeader("Content-disposition", "attachment;filename=image-mapping.csv.gz")
        response.contentType = "application/gzip"
        def bos = new GZIPOutputStream(response.outputStream)
        imageService.exportMappingCSV(bos)
        bos.flush()
        bos.close()
    }

    @Operation(
            method = "GET",
            summary = "Export dataset mappings",
            description = "Export dataset mappings.",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Data Resource UID", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/gzip", schema = @Schema(implementation = Map, format= 'binary'))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
            ],
            tags = ["Export"]
    )
    @Path("/ws/exportMapping/{id}")
    @Consumes("application/json")
    @Produces("application/gzip")
    def exportDatasetMapping(){
        if (!params.id){
            renderResults([success: false, message: "Failed to store image!"], 400)
        } else {
            response.setHeader("Content-disposition", "attachment;filename=image-mapping-${params.id}.csv.gz")
            response.contentType = "application/gzip"
            def bos = new GZIPOutputStream(response.outputStream)
            imageService.exportDatasetMappingCSV(params.id, bos)
            bos.flush()
            bos.close()
        }
    }

    @Operation(
            method = "GET",
            summary = "Export CSV of URL to imageIdentifier mappings",
            description = """Exports the following fields in CSV:
                image_identifier as "imageID"
                identifier
                audience
                contributor
                created
                creator
                description
                format
                license
                publisher
                references
                rightsHolder
                source
                title
                type""",
            parameters = [
                    @Parameter(name = "id", in = PATH, required = true, description = "Data Resource UID", schema = @Schema(implementation = String))
            ],
            responses = [
                    @ApiResponse(content = [@Content(mediaType = "application/gzip", schema = @Schema(implementation = Map, format= 'binary'))],responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ]),
                    @ApiResponse(responseCode = "400", content = [@Content(mediaType = "application/json", schema = @Schema(implementation = Map))], headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ]),
            ],
            tags = ["Export"]
    )
    @Path("/ws/exportDataset/{id}")
    @Consumes("application/json")
    @Produces("application/gzip")
    def exportDataset(){
        if (!params.id){
            renderResults([success: false, message: "Failed to store image!"], 400)
        } else {
            response.setHeader("Content-disposition", "attachment;filename=image-export-${params.id}.csv.gz")
            response.contentType = "application/gzip"
            def bos = new GZIPOutputStream(response.outputStream)
            imageService.exportDatasetCSV(params.id, bos)
            bos.flush()
            bos.close()
        }
    }
}