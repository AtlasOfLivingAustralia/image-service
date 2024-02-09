package au.org.ala.images

import groovy.json.JsonSlurper

/**
 * Services to retrieve resource level metadata.
 */
class CollectoryService {

    def grailsApplication

    static String NO_DATARESOURCE = 'no_dataresource'

    def serviceMethod() {}

    //a low rent cache of image metadata
    static _lookupCache = [:]

    static _uidLookupCache = [:]

    /**
     * Adds the image metadata (dublin core terms) to the image
     * for the image's associated data resource definition in the collectory.
     *
     * @param image
     */
    def addMetadataForResource(image){

        //if there no resource UID, move on
        if(!image["dataResourceUid"]){
            return
        }

        //if there no collectory configured, move on
        if (!grailsApplication.config.getProperty('collectory.baseURL')) {
            return
        }

        def metadata = getResourceLevelMetadata(image.dataResourceUid)

        if(metadata && metadata.imageMetadata) {
            //only add properties if they are blank on the source image
            metadata.imageMetadata.each { kvp ->
                if (kvp.value && !image[kvp.key]) {
                    image[kvp.key] = kvp.value
                }
            }
        }
    }

    def clearCache() {
        log.info("Clearing cache - current size: " + _lookupCache.size())
        _lookupCache.clear()
        _uidLookupCache.clear()
    }

    def getResourceLevelMetadata(dataResourceUid) {

        def metadata = [:]

        if (!dataResourceUid || dataResourceUid == NO_DATARESOURCE){
            return metadata
        }

        //lookup the resource UID
        if(!_lookupCache.containsKey(dataResourceUid)){
            def url = grailsApplication.config.getProperty('collectory.baseURL') + "/ws/dataResource/" + dataResourceUid
            try {
                def js = new JsonSlurper()
                def json = js.parseText(new URL(url).text)
                if (json) {
                    metadata = json
                }
                _lookupCache.put(dataResourceUid, json)
            } catch (Exception e){
                log.warn("Unable to load metadata from ${url}")
            }
        } else {
            metadata = _lookupCache.get(dataResourceUid)
        }

        metadata
    }

    def getNameForUID(uid){

        if(!uid){
            return null
        }

        //lookup the resource UID
        if(!_uidLookupCache.containsKey(uid)){
            def url = grailsApplication.config.getProperty('collectory.baseURL') + "/ws/lookup/name/" + uid
            try {
                def js = new JsonSlurper()
                def json = js.parseText(new URL(url).text)
                _uidLookupCache.put(uid, json.name)
            } catch (Exception e){
                log.warn("Unable to load metadata from ${url}")
            }
        } else {
            _uidLookupCache.get(uid)
        }
    }
}