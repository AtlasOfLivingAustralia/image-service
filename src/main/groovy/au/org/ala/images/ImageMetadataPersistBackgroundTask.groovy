package au.org.ala.images

import groovy.util.logging.Slf4j
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringUtils
//import org.apache.log4j.Logger
import org.hibernate.Session

@Slf4j
class ImageMetadataPersistBackgroundTask extends BackgroundTask {

    Long _imageId
    ImageService _imageService
    ImageStoreService _imageStoreService
    MetaDataSourceType _metaDataSourceType
    String _uploaderId
    String _imageIdentifier
    String _originalFilename
//    private Logger log = Logger.getLogger(ImageMetadataPersistBackgroundTask.class)

    ImageMetadataPersistBackgroundTask(Long imageId,
                                       String imageIdentifier,
                                       String originalFilename,
                                       MetaDataSourceType metaDataSourceType,
                                       String uploaderId,
                                       ImageService imageService,
                                       ImageStoreService imageStoreService) {
        _imageId = imageId
        _imageService = imageService
        _imageStoreService = imageStoreService
        _metaDataSourceType = metaDataSourceType
        _uploaderId = uploaderId
        _imageIdentifier = imageIdentifier
        _originalFilename = originalFilename
    }

    @Override
    void execute() {

        Session session = null
        try {
            session = _imageService.sessionFactory.openSession()
            session.beginTransaction()
            byte[] imageBytes = _imageStoreService.retrieveImage(_imageIdentifier)
            Map _metadata = _imageService.getImageMetadataFromBytes(imageBytes, _originalFilename)
            _metadata.each { k, v ->
                def cleanedValue = sanitizeString(v)
                if (cleanedValue && cleanedValue.length() < 8000) {
                    final query = session.createSQLQuery("insert into image_meta_data_item (id, image_id, version, name, value, source) values (nextval('image_metadata_seq'), :image_id, :version, :name, :value, :source)")
                    query.setLong('image_id', _imageId)
                    query.setLong('version', 1l)
                    query.setString('name', k)
                    query.setString('value', cleanedValue)
                    query.setString('source', 'Embedded')
                    query.executeUpdate()
                }
            }
        } catch (Exception e){
            log.error(e.getMessage(), e)
        } finally {
            if (session){
                session.getTransaction().commit()
                session.close()
            }
        }
        log.debug("Metadata update for "+ _imageId)
    }

    private static String sanitizeString(Object value) {
        if (value) {
            value = value.toString()
        } else {
            return ""
        }

        def bytes = value?.getBytes("utf8")

        def hasZeros = bytes.contains(0)
        if (hasZeros) {
            return Base64.encodeBase64String(bytes)
        } else {
            return StringUtils.trimToEmpty(value)
        }
    }

    @Override
    String toString() {
        return "ImageMetadataPersistBackgroundTask," + _imageId
    }
}
