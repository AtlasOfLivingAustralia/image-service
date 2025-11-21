package au.org.ala.images

import grails.converters.JSON
import org.grails.web.converters.configuration.ConvertersConfigurationHolder
import org.grails.web.converters.marshaller.json.DomainClassMarshaller

class BootStrap {

    def messageSource

    def elasticSearchService

    def batchService

    def init = { servletContext ->

        messageSource.setBasenames(
                "file:///var/opt/atlas/i18n/image-service/messages",
                "file:///opt/atlas/i18n/image-service/messages",
                "WEB-INF/grails-app/i18n/messages",
                "classpath:messages"
        )

        elasticSearchService.initialize()
        batchService.initialize()


        JSON.registerObjectMarshaller(S3StorageLocation, { S3StorageLocation it ->
            // Register a custom marshaller for S3StorageLocation
            def json = [
                id: it.id,
                region: it.region,
                bucket: it.bucket,
                prefix: it.prefix ?: '',
                accessKey: it.accessKey ?: '',
                secretKey: it.secretKey ?: '',
                containerCredentials: it.containerCredentials ?: false,
                publicRead: it.publicRead ?: false,
                redirect: it.redirect ?: false,
                cloudfrontDomain: it.cloudfrontDomain ?: '',
            ]

            return json
        })

        JSON.registerObjectMarshaller(FileSystemStorageLocation, { FileSystemStorageLocation it ->
            // Register a custom marshaller for FileSystemStorageLocation
            def json = [
                id: it.id,
                basePath: it.basePath ?: ''
            ]

            return json
        })

        JSON.registerObjectMarshaller(SwiftStorageLocation, { SwiftStorageLocation it ->
            // Register a custom marshaller for SwiftStorageLocation
            def json = [
                id: it.id,
                authUrl: it.authUrl ?: '',
                containerName: it.containerName ?: '',
                username: it.username ?: '',
                password: it.password ?: '',
                tenantId: it.tenantId ?: '',
                tenantName: it.tenantName ?: '',
                authenticationMethod: it.authenticationMethod?.name() ?: 'KEYSTONE',
                publicContainer: it.publicContainer ?: false,
                redirect: it.redirect ?: false
            ]

            return json
        })
    }
    def destroy = {
    }
}
