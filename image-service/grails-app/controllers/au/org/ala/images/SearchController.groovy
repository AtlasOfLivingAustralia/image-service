package au.org.ala.images

import au.org.ala.web.AlaSecured
import au.org.ala.web.CASRoles
import grails.converters.JSON
import org.apache.commons.lang3.StringUtils

import java.util.regex.Pattern

class SearchController {

    def searchService
    def searchCriteriaService
    def selectionService
    def elasticSearchService
    def collectoryService
    def authService

    def index() {
        boolean hasCriteria = searchService.getSearchCriteriaList()?.size() > 0
        def criteriaDefinitions = searchCriteriaService.getCriteriaDefinitionList()
        [criteriaDefinitions: criteriaDefinitions]
        render(view: 'advancedSearch', model:[hasCriteria: hasCriteria, criteriaDefinitions: criteriaDefinitions])
    }

    def list() {

        def ct = new CodeTimer("Image list").tap { debug() }

        params.offset = params.offset ?: 0
        params.max = params.max ?: 50
        params.sort = params.sort ?: 'dateUploaded'
        params.order = params.order ?: 'desc'

        def query = params.q as String

        QueryResults<Image> results = searchService.search(params)

        def userId = authService.getUserId()

        def isLoggedIn = StringUtils.isNotEmpty(userId)
        def selectedImageMap = [:] // selectionService.getSelectedImageIdsAsMap(userId)

        def isAdmin = request.isUserInRole('ROLE_ADMIN')

        def filters = [:]

        def filterQueries = params.findAll { it.key == 'fq' && it.value}
        filterQueries.each {
            if(it.value instanceof String[]){
                it.value.each { filter ->
                    if(filter) {
                        def kv = filter.split(":")
                        if (kv[0] == "dataResourceUid") {
                            def name = collectoryService.getNameForUID(kv[1])
                            if(!name){
                                name = message(code:"no_dataresource")
                            }
                            filters["Data resource: ${name}"] = filter
                        } else {
                            filters["""${message(code:kv[0])}: ${message(code:kv[1], default:kv[1])}"""] = filter
                        }
                    }
                }
            } else {
                if(it.value) {
                    def kv = it.value.split(":")
                    if (kv[0] == "dataResourceUid") {
                        def name = collectoryService.getNameForUID(kv[1])
                        if(!name){
                            name = message(code:"no_dataresource")
                        }
                        filters["Data resource: ${name}"] =  it.value
                    } else {
                        filters["""${message(code:kv[0])}: ${message(code:kv[1], default:kv[1])}"""] = it.value
                    }
                }
            }
        }

        ct.stop(true)
        [images: results.list,
         facets: results.aggregations,
         criteria: [],
         q: query,
         totalImageCount: results.totalCount,
         isLoggedIn: isLoggedIn,
         selectedImageMap: selectedImageMap,
         filters: filters,
         searchCriteria: searchService.getSearchCriteriaList(),
         criteriaDefinitions: searchCriteriaService.getCriteriaDefinitionList(),
         isAdmin: isAdmin
        ]
    }

    def facet(){
        if(!params.facet){
            response.sendError(400, "Missing facet param")
            return
        }
        params.offset = params.offset ?: 0
        params.max = params.max ?: 50
        params.sort = params.sort ?: 'dateUploaded'
        params.order = params.order ?: 'desc'
        def results = searchService.facet(params)

        def fq = params.findAll { it.key == 'fq' && it.value}
        def filterQueries = ""
        if(fq){
            fq.each {
                if(it)
                    filterQueries += ("&" + it)
            }
        }

        render(view: 'facetFragment', model:[facet:params.facet, filterQueries: filterQueries, facetValues:results.aggregations.get(params.facet)])
    }

    def removeCriterion(){
        searchService.removeSearchCriteria(params.criteriaId)
        redirect(action:'list', params:[q:params.q, fq:params.fq])
    }

    @AlaSecured
    def download(){
        response.setHeader("Content-Disposition", "attachment; filename=\"images.zip\"")
        response.setHeader("Content-Type", "application/zip")
        searchService.download(params, response.getOutputStream())
    }

    def addSearchCriteriaFragment() {
        def criteriaDefinitions = searchCriteriaService.getCriteriaDefinitionList()
        [criteriaDefinitions: criteriaDefinitions]
    }

    def criteriaDetailFragment() {
        try {
            def criteriaDefinition = SearchCriteriaDefinition.get(params.searchCriteriaDefinitionId)
            if (criteriaDefinition) {
                switch (criteriaDefinition.type) {
                    case CriteriaType.ImageProperty:
                        redirect(action: 'imageFieldCriteriaFragment', params: params)
                        break
                    case CriteriaType.ImageMetadata:
                        redirect(action: 'imageMetadataCriteriaFragment', params: params)
                        break
                    default:
                        throw new RuntimeException("Unhandled CriteriaType - " + criteriaDefinition.type)
                }
                return
            }
            throw new RuntimeException("No criteria specified!")
        } catch (Exception ex) {
            redirect(action: 'ajaxErrorFragment', params: [title: 'Error getting criteria details', errorMessage: ex.message])
        }
    }

    def imageFieldCriteriaFragment() {
        def criteriaDefinition = SearchCriteriaDefinition.get(params.int("searchCriteriaDefinitionId"))
        def criteria = searchService.getSearchCriteria(params.criteriaId)
        [criteriaDefinition: criteriaDefinition, criteria: criteria]
    }

    def imageMetadataCriteriaFragment() {
        def criteriaDefinition = SearchCriteriaDefinition.get(params.int("searchCriteriaDefinitionId"))
        def criteria = searchService.getSearchCriteria(params.criteriaId)
        def metadataNames = elasticSearchService.getMetadataKeys()
        def metadataItemName = ""
        def metadataItemValue = ""

        if (criteria) {
            def metaDataPattern = Pattern.compile("^(.*)[:](.*)\$")
            def matcher = metaDataPattern.matcher(criteria?.value)
            if (matcher.matches()) {
                metadataItemName = matcher.group(1)
                metadataItemValue = matcher.group(2)
            }
        }
        [criteriaDefinition: criteriaDefinition, criteria: criteria, metadataNames: metadataNames, metadataItemName: metadataItemName, metadataItemValue: metadataItemValue]
    }

    def ajaxErrorFragment() {
        def title = params.title ?: "Error!"
        def errorMessage = params.errorMessage ?: "Unspecified Error!"
        [title: title, errorMessage: errorMessage]
    }

    def ajaxAddSearchCriteria() {

        def results = [status: 'ok']
        try {

            def searchCriteria = searchService.getSearchCriteria(params.criteriaId)
            if (searchCriteria) {
                // we are saving a change to an existing criteria...
                searchService.saveSearchCriteria(searchCriteria.id, params)
            } else {
                // we create a new criteria
                searchCriteria = searchService.addSearchCriteria(params)
            }

            results["criteriaID"] = searchCriteria.id

        } catch (Exception ex) {
            results.status = "error"
            results.errorMessage = ex.message
        }

        render(results as JSON)
    }

    def editSearchCriteriaFragment() {
        def criteria = searchService.getSearchCriteria(params.criteriaId as String)
        [criteria: criteria]
    }

    def ajaxRemoveSearchCriteria() {
        def results = [status: 'ok']
        searchService.removeSearchCriteria(params.searchCriteriaId as String)
        render(results as JSON)
    }

    def ajaxClearSearchCriteria() {
        def results = [status: 'ok']
        searchService.removeAllSearchCriteria()
        render(results as JSON)
    }

    def criteriaListFragment() {
        def list = searchService.getSearchCriteriaList()
        [searchCriteria: list]
    }

    def searchResultsFragment() {
        params.max = params.max ?: 48
        def searchResults = searchService.search(params)
        def userId = authService.getUserId()
        def selectedImageMap = [:]
        if (userId) {
            selectedImageMap = selectionService.getSelectedImageIdsAsMap(userId)
        }

        [imageList: searchResults.list, totalCount: searchResults.totalCount, facets:searchResults.aggregations, selectedImageMap: selectedImageMap]
    }

    @AlaSecured(value=[CASRoles.ROLE_USER], anyRole = true)
    def ajaxSelectAllCurrentQuery() {

        def results = [success: true]
        def userId = authService.getUserId()
        if (userId) {
            searchService.withCriteriaImageIds(null, { idList ->
                selectionService.selectImages(userId, idList)
            })
        } else {
            results.success = false
            results.message = "Could not identify user!"
        }
        render(results as JSON)
    }
}
