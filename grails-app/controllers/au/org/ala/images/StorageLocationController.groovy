package au.org.ala.images

import au.org.ala.images.StorageLocationService.AlreadyExistsException
import au.org.ala.web.AlaSecured
import au.org.ala.web.CASRoles
import grails.gorm.transactions.Transactional
import grails.web.http.HttpHeaders

import static org.springframework.http.HttpStatus.OK

@AlaSecured(value = [CASRoles.ROLE_ADMIN, "ROLE_IMAGE_ADMIN"], anyRole = true, statusCode = 403)
class StorageLocationController {

    def storageLocationService
    def settingService


    def create() {
        def json = request.getJSON()
        try {
            def sl = storageLocationService.createStorageLocation(json)
            if (!sl) {
                render(status: 400)
                return
            }
        } catch (AlreadyExistsException e) {
            render(status: 409, text: e.message)
            return
        } catch (e) {
            log.error("Error saving storage location", e)
            render(status: 400)
            return
        }
        render(status: 204)
    }

    def listFragment() {
        def storageLocationList = StorageLocation.list([sort: 'id'])
        def verifieds = storageLocationList.collectEntries {
            [ (it.id): it.verifySettings() ]
        }
        [storageLocationList:  storageLocationList, defaultId: settingService.getStorageLocationDefault(), verifieds: verifieds]
    }

    def editFragment() {
        StorageLocation instance = StorageLocation.get(params.id)
        respond instance
    }

    @Transactional
    def update() {
        StorageLocation instance = StorageLocation.get(params.id)

        if (!instance) {
            transactionStatus.setRollbackOnly()
            return render(status: 404)
        }
        def result = instance.setProperties(request)

        instance.validate()
        if (instance.hasErrors()) {
            render(status: 422)
            return
        }

        boolean updateAcls = false

        if (instance instanceof S3StorageLocation) {
            // Trigger ACL refresh if either publicRead or privateAcl changed
            updateAcls = instance.isDirty('publicRead') || instance.isDirty('privateAcl')
        }

        instance.save()

        if (updateAcls) {
            storageLocationService.updateAcl(instance)
        }

        respond instance, [status: OK]
    }

    def setDefault() {
        def id = params.long('id')
        StorageLocation sl = StorageLocation.get(id)
        if (sl) {
            settingService.setStorageLocationDefault(id)
            render(status: 204)
        } else {
            render(status: 404)
        }
    }

    def migrate() {
        Long source = params.long('src')
        Long destination = params.long('dst')
        boolean deleteSource = params.boolean('deleteSrc', false)

        if (source == destination) {
            render(status: 400, text: "Source and destination are the same")
        }

        log.error("Migrate from source {} to destination {}", source, destination)
        if (source && destination) {
            storageLocationService.migrate(source, destination, request.remoteUser, deleteSource)
            render(status: 202)
        } else {
            render(status: 404)
        }
    }

}
