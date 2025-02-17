package au.org.ala.images

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import groovy.json.JsonSlurper
import org.springframework.beans.factory.annotation.Value

import java.time.Duration

/**
 * Services to retrieve resource level metadata.
 */
class CollectoryService {

    def grailsApplication

    static String NO_DATARESOURCE = 'no_dataresource'

    @Value('${collectory.cache.dataResourceSpec:maximumSize=10000,expireAfterWrite=1d,refreshAfterWrite=1d}')
    String dataResourceLookupCacheSpec = "maximumSize=10000,expireAfterWrite=1d,refreshAfterWrite=1d"

    @Value('${collectory.cache.nameSpec:maximumSize=10000,expireAfterWrite=1d,refreshAfterWrite=1d}')
    String nameLookupCacheSpec = "maximumSize=10000,expireAfterWrite=1d,refreshAfterWrite=1d"

    LoadingCache<String, Optional<Object>> dataResourceLookupCache = Caffeine.from(dataResourceLookupCacheSpec)
            .build { key -> getResourceLevelMetadataInternal(key) }

    LoadingCache<String, Optional<Object>> nameLookupCache = Caffeine.from(nameLookupCacheSpec)
            .build { key -> getNameForUIDInternal(key) }


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
        log.info("Clearing cache - current size: " + dataResourceLookupCache.estimatedSize())
        dataResourceLookupCache.invalidateAll()
        nameLookupCache.invalidateAll()
    }

    def getResourceLevelMetadata(dataResourceUid) {

        def results = dataResourceLookupCache.get(dataResourceUid)
        return results.orElse([:])
    }

    private Optional<Object> getResourceLevelMetadataInternal(String dataResourceUid) {
        if (!dataResourceUid || dataResourceUid == NO_DATARESOURCE){
            return Optional.empty()
        }

        def url = grailsApplication.config.getProperty('collectory.baseURL') + "/ws/dataResource/" + dataResourceUid
        try {
            def js = new JsonSlurper()
            def json = js.parseText(new URL(url).text)
            if (json) {
                return Optional.of(json)
            }
            return Optional.empty()
        } catch (FileNotFoundException e) {
            log.trace("Data Resource UID not found: ${dataResourceUid} at ${url}")
            return Optional.empty()
        } catch (Exception e) {
            log.warn("Unable to load metadata from ${url} because ${e.message}")
            return null // always retry errors that aren't 404s
        }
    }

    def getNameForUID(uid){
            if (!uid) {
                return null
            }

            //lookup the resource UID
            def results = nameLookupCache.get(uid)
            return results.orElse(null)
    }

    Optional<Object> getNameForUIDInternal(String uid){
        //lookup the resource UID
        def url = grailsApplication.config.getProperty('collectory.baseURL') + "/ws/lookup/name/" + uid
        try {
            def js = new JsonSlurper()
            def json = js.parseText(new URL(url).text)
            return Optional.of(json)
        } catch (FileNotFoundException e) {
            log.trace("Name not found: ${uid} at ${url}")
            return Optional.empty()
        } catch (Exception e) {
            log.warn("Unable to load metadata from ${url} because ${e.message}")
            return null
        }
    }
}