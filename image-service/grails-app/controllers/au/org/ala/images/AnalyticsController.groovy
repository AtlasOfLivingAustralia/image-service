package au.org.ala.images

import grails.converters.JSON
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.headers.Header
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse

import javax.ws.rs.Path
import javax.ws.rs.Produces

import static io.swagger.v3.oas.annotations.enums.ParameterIn.PATH

/**
 * Instructions for obtaining required JSON...
 * http://notes.webutvikling.org/access-google-analytics-api-with-oauth-token/
 *
 * Note:
 * https://stackoverflow.com/questions/12837748/analytics-google-api-error-403-user-does-not-have-any-google-analytics-account
 *
 * Referenced JSON should look something like this:
 * {
 *   "type": "service_account",
 *   "project_id": "XXXXXXXXXX",
 *   "private_key_id": "XXXXXXXXXX",
 *   "private_key": "XXXXXXXXXX",
 *   "client_email": "XXXXXXXXXX",
 *   "client_id": "XXXXXXXXXX",
 *   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
 *   "token_uri": "https://oauth2.googleapis.com/token",
 *   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
 *   "client_x509_cert_url": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
 * }
 */
class AnalyticsController {

    def analyticsService

    @Operation(
            method = "GET",
            summary = "Get image usage for data resource. e.g dataResourceUID=dr123",
            description = "Get image usage for data resource.",
            parameters = [
                    @Parameter(name="dataResourceUID", in = PATH, description = 'Data Resource UID', required = true)
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
                    @ApiResponse(responseCode = "400", headers = [
                            @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                            @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                    ])
            ],
            tags = ['Analytics services - image usage tracking']
    )
    @Produces("application/json")
    @Path("/ws/analytics/{dataResourceUID}")
    def byDataResource() {

        def dataResourceUID = params.dataResourceUID
        if (!dataResourceUID){
            response.sendError(400, "Please supply a data resource UID")
            return null
        } else {
            def results = analyticsService.byDataResource(dataResourceUID)
            render (results as JSON)
        }
    }

    @Operation(
            method = "GET",
            summary = "Get overall image usage for the system",
            description = "Get overall image usage for the system",
            responses = [
                    @ApiResponse(content = [
                            @Content(mediaType='application/json', schema = @Schema(implementation=Map))
                            ],
                            responseCode = "200",
                            headers = [
                                    @Header(name = 'Access-Control-Allow-Headers', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Methods', description = "CORS header", schema = @Schema(type = "String")),
                                    @Header(name = 'Access-Control-Allow-Origin', description = "CORS header", schema = @Schema(type = "String"))
                            ])
            ],
            tags = ['Analytics services - image usage tracking']
    )
    @Produces("application/json")
    @Path("/ws/analytics")
    def byAll() {
        def results = analyticsService.byAll()
        render (results as JSON)
    }
}
