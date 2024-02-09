package au.org.ala.images

import au.ala.org.ws.security.RequireApiKey
import grails.converters.JSON
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.headers.Header
import io.swagger.v3.oas.annotations.media.ArraySchema
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import org.apache.commons.io.FileUtils

import javax.ws.rs.Consumes
import javax.ws.rs.Path
import javax.ws.rs.Produces

import static io.swagger.v3.oas.annotations.enums.ParameterIn.PATH
import static io.swagger.v3.oas.annotations.enums.ParameterIn.QUERY

class BatchController {

    def batchService

    def index() { }

    @Operation(
            method = "POST",
            summary = "Upload zipped AVRO files for loading",
            description = "Upload zipped AVRO files for loading. Required scopes: 'image-service/write'.",
            parameters = [
                    @Parameter(name="dataResourceUid", in = QUERY, description = 'Data Resource UId', required = true)
            ],
            requestBody = @RequestBody(
                    description = "The gzipped upload file",
                    required = true,
                    content = [
                            @Content(mediaType = 'application/gzip', schema = @Schema(name='archive', title='The file to upload', type='string', format='binary'))
                    ]
            ),
            responses = [
                    @ApiResponse(content = [
                            @Content(mediaType='application/json', schema = @Schema(implementation=Map))
                            ],
                            responseCode = "200",
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
            tags = ['BatchUpdate']
    )
    @Consumes('multipart/form-data')
    @Produces("application/json")
    @Path("/ws/batch/upload")
    @RequireApiKey(scopes = ["image-service/write"])
    def upload(){

        //multi part upload
        def zipFile = request.getFile('archive')
        def dataResourceUid = params.dataResourceUid

        if (!zipFile){
            response.sendError(400, "Missing zip file")
            return
        }

        if (!dataResourceUid){
            response.sendError(400, "Missing dataResourceUid parameter")
            return
        }

        // move zip file from tmp working directory to uploads directory
        File uploadDir = new File(
                grailsApplication.config.getProperty('imageservice.batchUpload') +
                "/tmp-" + System.currentTimeMillis() + "/")
        FileUtils.forceMkdir(uploadDir)
        File tmpFile = new File(uploadDir, zipFile.originalFilename)
        zipFile.transferTo(tmpFile)

        // unpack zip
        def upload = batchService.createBatchFileUploadsFromZip(dataResourceUid, tmpFile)

        //return an async response
        def response = [
                batchID : upload.getId(),
                dataResourceUid: upload.dataResourceUid,
                file: upload.filePath,
                dateCreated: upload.getDateCreated(),
                status: upload.status,
                files: []
        ]

        // add file details to response for validation by client
        upload.batchFiles.each {
            response.files << [
                    status: it.status,
                    file: it.filePath,
                    recordCount: it.recordCount
            ]
        }
        render (response as JSON)
    }

    @Operation(
            method = "GET",
            summary = "Get batch update status",
            description = "Get batch update status.",
            parameters = [
                    @Parameter(name="id", in = PATH, description = 'The batch id', required = true)
            ],
            responses = [
                    @ApiResponse(content = [
                            @Content(mediaType='application/json', schema = @Schema(implementation=Map))
                            ],
                            responseCode = "200",
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
            tags = ['BatchUpdate']
    )
    @Produces("application/json")
    @Path("/ws/batch/status/{id}")
    def status(){

        //write zip file to filesystem
        def upload = batchService.getBatchFileUpload(params.id)
        if (upload){
            //return an async response
            def response = createResponse(upload)
            render (response as JSON)
        } else {
            response.sendError(404)
        }
    }

    @Operation(
            method = "GET",
            summary = "Get batch update status",
            description = "Get batch update status.",
            parameters = [
                    @Parameter(name="dataResourceUid", in = PATH, description = 'Data Resource UId', required = true)
            ],
            responses = [
                    @ApiResponse(content = [
                            @Content(mediaType='application/json', array = @ArraySchema(schema = @Schema(implementation=Map)))
                            ],
                            responseCode = "200",
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
            tags = ['BatchUpdate']
    )
    @Produces("application/json")
    @Path("/ws/batch/dataresource/{dataResourceUid}")
    def statusForDataResource(){

        //write zip file to filesystem
        def uploads = batchService.getBatchFileUploadsFor(params.dataResourceUid) ?: []
        //return an async response
        def responses = uploads.collect { upload ->
            createResponse(upload)
        }
        render (responses as JSON)
    }

    Map createResponse(BatchFileUpload upload){
        //return an async response
        def response = [
                batchID : upload.getId(),
                dataResourceUid: upload.dataResourceUid,
                file: upload.filePath,
                dateCreated: upload.getDateCreated(),
                status: upload.status,
                files: []
        ]
        upload.batchFiles.each {
            response.files << [
                    status: it.status,
                    file: it.filePath,
                    recordCount: it.recordCount
            ]
        }
        response
    }
}
