package au.org.ala.images

import au.org.ala.images.helper.FlybernateSpec

class ImageServiceFlybernateSpec extends FlybernateSpec {

    private final ImageService service = new ImageService()

    @Override
    List<Class> getDomainClasses() {
        [
                StorageLocation,
                FileSystemStorageLocation,
                Image,
                License,
                LicenseMapping
        ]
    }

    def "updateMetadata persists recognisedLicense through the real executeUpdate path"() {
        given:
        def originalLicense = persistLicense('OLD', 'Original License')
        def updatedLicense = persistLicense('CC-BY', 'Creative Commons Attribution')
        persistImage('img-1', 'image-1.jpg', 'dr-1', originalLicense.acronym, originalLicense)

        when:
        service.updateMetadata('img-1', [license: updatedLicense.acronym])
        hibernateSession.flush()
        hibernateSession.clear()

        then:
        with(Image.findByImageIdentifier('img-1')) {
            license == updatedLicense.acronym
            recognisedLicense != null
            recognisedLicense.id == updatedLicense.id
            recognisedLicense.acronym == updatedLicense.acronym
        }
    }

    def "ImageMetadataUpdateBackgroundTask persists recognisedLicense sync through updateMetadata"() {
        given:
        def originalLicense = persistLicense('OLD', 'Original License')
        def updatedLicense = persistLicense('CC-BY', 'Creative Commons Attribution')
        def mappedLicenseValue = 'https://creativecommons.org/licenses/by/4.0/'
        new LicenseMapping(license: updatedLicense, value: mappedLicenseValue).save(failOnError: true, flush: true)
        persistImage('img-2', 'image-2.jpg', 'dr-2', originalLicense.acronym, originalLicense)

        when:
        new ImageMetadataUpdateBackgroundTask('img-2', [license: mappedLicenseValue], service).execute()
        hibernateSession.flush()
        hibernateSession.clear()

        then:
        with(Image.findByImageIdentifier('img-2')) {
            license == mappedLicenseValue
            recognisedLicense != null
            recognisedLicense.id == updatedLicense.id
            recognisedLicense.acronym == updatedLicense.acronym
        }
    }

    private static License persistLicense(String acronym, String name) {
        new License(
                acronym: acronym,
                name: name,
                url: "https://example.org/licenses/${acronym.toLowerCase()}",
                imageUrl: "https://example.org/licenses/${acronym.toLowerCase()}.png"
        ).save(failOnError: true, flush: true)
    }

    private static Image persistImage(String imageIdentifier, String originalFilename, String dataResourceUid, String license, License recognisedLicense) {
        new Image(
                imageIdentifier: imageIdentifier,
                originalFilename: originalFilename,
                dataResourceUid: dataResourceUid,
                license: license,
                recognisedLicense: recognisedLicense,
                storageLocationName: 'test-storage'
        ).save(failOnError: true, flush: true)
    }
}
