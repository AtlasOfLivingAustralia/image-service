package au.org.ala.images

class FacetTagLib {
    static namespace = 'facet'

    def selectedFacetLink = { attrs, body ->

        def query = params.q

        def otherFilterQueries = params.findAll { it.key == 'fq' }

        def queryUrl = "?"
        if (query){
            queryUrl += "q=" + params.q
        }

        otherFilterQueries.each {

            if( it.value instanceof String[]){
                it.value.each { filter ->
                    if (attrs.filter != filter) {
                        queryUrl += "&fq=" + filter
                    }
                }
            } else {
                if (attrs.filter != it.value) {
                    queryUrl += "&fq=" + it.value
                }
            }
        }
        out << request.getRequestURL().toString() + queryUrl
    }

    def selectedCriterionLink = { attrs, body ->

        def query = params.q

        def otherFilterQueries = params.findAll { it.key == 'fq' }

        def queryUrl = ""
        if (query){
            queryUrl += "q=" + params.q
        }

        otherFilterQueries.each {

            if( it.value instanceof String[]){
                it.value.each { filter ->
                    queryUrl += "&fq=" + filter
                }
            } else {
                queryUrl += "&fq=" + it.value
            }
        }
        out << createLink(controller: 'search', action:'removeCriterion', params: [criteriaId: attrs.criteriaId]) + queryUrl
    }

}
