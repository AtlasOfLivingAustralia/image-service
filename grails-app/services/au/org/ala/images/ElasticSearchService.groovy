package au.org.ala.images

import au.org.ala.images.metrics.MetricsSupport
import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.ElasticsearchException
import co.elastic.clients.elasticsearch._types.SearchType
import co.elastic.clients.elasticsearch._types.SortOptionsBuilders
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.Time
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate
import co.elastic.clients.elasticsearch._types.aggregations.Buckets
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.DeleteRequest
import co.elastic.clients.elasticsearch.core.IndexRequest
import co.elastic.clients.elasticsearch.core.IndexResponse
import co.elastic.clients.elasticsearch.core.ScrollRequest
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.core.bulk.BulkOperationBuilders
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse
import co.elastic.clients.elasticsearch.indices.GetMappingRequest
import co.elastic.clients.elasticsearch.indices.GetMappingResponse
import co.elastic.clients.elasticsearch.indices.PutMappingRequest
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import co.elastic.clients.util.NamedValue
import com.opencsv.CSVWriter
import grails.core.GrailsApplication
import org.apache.http.HttpResponseInterceptor
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import groovy.json.JsonOutput
import grails.web.servlet.mvc.GrailsParameterMap
import org.apache.http.HttpHost
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.RestClient

import javax.annotation.PreDestroy
import java.util.regex.Pattern
import javax.annotation.PostConstruct
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class ElasticSearchService implements MetricsSupport {

    GrailsApplication grailsApplication
    def imageStoreService

    static String UNRECOGNISED_LICENCE =  "unrecognised_licence"
    static String NOT_SUPPLIED = "not_supplied"

    private ElasticsearchClient client

    @PostConstruct
    def initialize() {
        def hosts = grailsApplication.config.getProperty('elasticsearch.hosts', List, []).collect { host ->
            new HttpHost(host.host, host.port as Integer, host.scheme)
        }

        CredentialsProvider credentialsProvider = null
        if (grailsApplication.config.getProperty('elasticsearch.username') && grailsApplication.config.getProperty('elasticsearch.password')) {
            credentialsProvider = new BasicCredentialsProvider()
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            grailsApplication.config.getProperty('elasticsearch.username'),
                            grailsApplication.config.getProperty('elasticsearch.password')
                    )
            )
        }

        def defaultHeaders = grailsApplication.config.getProperty('elasticsearch.default-headers', List, []).collect {
            new BasicHeader(it.name, it.value)
        }

        def restClient = RestClient.builder(*hosts)
                .setHttpClientConfigCallback {
                    if (credentialsProvider) {
                        it.setDefaultCredentialsProvider(credentialsProvider)
                    }
                    // Hacks for elasticsearch-java client compatibility with older/newer ES versions
                    it.setDefaultHeaders(defaultHeaders)
                      .addInterceptorLast((HttpResponseInterceptor) (response, context) -> {
                          if (!response.containsHeader("X-Elastic-Product")) response.addHeader("X-Elastic-Product", "Elasticsearch")
                      })
                }
                .build()

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);

        initialiseIndex()
    }

    @PreDestroy
    def destroy() {
        if (client){
            try {
                client.close()
            } catch (Exception e){
                log.error("Error thrown trying to close down client", e)
            }
        }
    }

    def reinitialiseIndex() {
        try {
            def ct = new CodeTimer("Index deletion")
            final indexName = grailsApplication.config.getProperty('elasticsearch.indexName') as String
            def response = client.indices().delete(b -> b.index(indexName))
            if (response.acknowledged()) {
                log.info "The index is removed"
            } else {
                log.error "The index could not be removed"
            }
            ct.stop(true)

        } catch (ElasticsearchException e) {
            e.error().type() == 'index_not_found_exception' ? log.info("Index not found - this is expected behaviour for a new empty system.") : log.warn("ElasticsearchException thrown.", e)
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex)
            // failed to delete index - maybe because it didn't exist?
        }
        initialiseIndex()
    }

    def indexImage(Image image) {
        return recordTime('elasticsearch.index.image', 'Time to index image in Elasticsearch') {
            if (!image){
                log.error("Supplied image was null")
                incrementCounter('elasticsearch.index.error', 'Elasticsearch indexing errors', [reason: 'null_image'])
                return
            }

            if (image.dateDeleted){
                log.debug("Supplied image is deleted")
                incrementCounter('elasticsearch.index.skipped', 'Elasticsearch indexing skipped', [reason: 'deleted'])
                return
            }

            try {
                log.debug("Indexing image {}", image.id)
                def ct = new CodeTimer("Index Image ${image.id}")
                // only add the fields that are searchable. They are marked with an annotation
                def fields = Image.class.declaredFields
                def data = [:]
                fields.each { field ->
                    if (field.isAnnotationPresent(SearchableProperty)) {
                        data[field.name] = image."${field.name}"
                    }
                }

                if (image.recognisedLicense) {
                    data.recognisedLicence = image.recognisedLicense.acronym
                } else {
                    data.recognisedLicence = UNRECOGNISED_LICENCE
                }

                indexImageInES(
                        data.imageIdentifier,
                        data.contentMD5Hash,
                        data.contentSHA1Hash,
                        data.mimeType,
                        data.originalFilename,
                        data.extension,
                        data.dateUploaded,
                        data.dateTaken,
                        data.fileSize,
                        data.height,
                        data.width,
                        data.zoomLevels,
                        data.dataResourceUid,
                        data.creator ,
                        data.title,
                        data.description,
                        data.rights,
                        data.rightsHolder,
                        data.license,
                        data.thumbHeight,
                        data.thumbWidth,
                        data.harvestable,
                        data.recognisedLicence,
                        data.occurrenceId
                )
                ct.stop(true)
                incrementCounter('elasticsearch.index.success', 'Successful image indexing operations')
            } catch (Exception e) {
                log.error("Error indexing image ${image?.id}: ${e.message}", e)
                incrementCounter('elasticsearch.index.error', 'Elasticsearch indexing errors', [reason: 'exception', error: e.class.simpleName])
                throw e
            }
        }
    }

    def indexImageInES(
            imageIdentifier,
            contentMD5Hash,
            contentSHA1Hash,
            mimeType,
            originalFilename,
            extension,
            dateUploaded,
            dateTaken,
            fileSize,
            height,
            width,
            zoomLevels,
            dataResourceUid,
            creator,
            title,
            description,
            rights,
            rightsHolder,
            license,
            thumbHeight,
            thumbWidth,
            harvestable,
            recognisedLicence,
            occurrenceId
    ){
        def data = [
                imageIdentifier: imageIdentifier,
                contentMD5Hash: contentMD5Hash,
                contentSHA1Hash: contentSHA1Hash,
                format: mimeType,
                originalFilename: originalFilename,
                extension: extension,
                dateUploaded: dateUploaded,
                dateTaken: dateTaken,
                fileSize: fileSize,
                height: height,
                width: width,
                zoomLevels: zoomLevels,
                dataResourceUid: dataResourceUid,
                creator: creator,
                title: title,
                description:description,
                rights:rights,
                rightsHolder:rightsHolder,
                license:license,
                thumbHeight:thumbHeight,
                thumbWidth:thumbWidth,
                harvestable:harvestable,
                recognisedLicence: recognisedLicence,
                occurrenceID: occurrenceId
        ]

        addAdditionalIndexFields(data)

        IndexRequest request = IndexRequest.of( b ->
                b
                .index(grailsApplication.config.getProperty('elasticsearch.indexName'))
                .id(imageIdentifier)
                .document(data)
        )

        try {
            IndexResponse indexResponse = client.index(request)
        } catch (ElasticsearchException e) {
            log.error("Error indexing image ${imageIdentifier} in index: ${e.message}", e)
        }
    }

    def bulkIndexImageInES(list){
        def bulkRequest = new BulkRequest.Builder()
            .index(grailsApplication.config.getProperty('elasticsearch.indexName'))
            .timeout(new Time.Builder().time("5m").build())
            .operations(
                list.collect { data ->
                    addAdditionalIndexFields(data)
                    BulkOperationBuilders.index(b ->
                        b
                        .id(data.imageIdentifier)
                        .document(data)
                    )
                }
            )
            .build()

        try {
            def bulkResponse = client.bulk(bulkRequest)
        } catch (ElasticsearchException e) {
            log.error("Error indexing images in bulk: ${e.message}", e)
        }
    }

    private def addAdditionalIndexFields(data){

        if (data.dateUploaded){
            if(!data.dateUploadedYearMonth && data.dateUploaded instanceof java.util.Date){
                data.dateUploadedYearMonth = data.dateUploaded.format("yyyy-MM")
            }
        }

        if (data.format){
            if (data.format.startsWith('image')){
                data.fileType = 'image'
            } else if (data.format.startsWith('audio')){
                data.fileType = 'sound'
            } else if (data.format.startsWith('video')){
                data.fileType = 'video'
            } else {
                data.fileType = 'document'
            }
        }

        data.recognisedLicence  = data.recognisedLicence ?: UNRECOGNISED_LICENCE
        data.creator = data.creator ? data.creator.replaceAll("[\"|'&]", "") : NOT_SUPPLIED
        data.dataResourceUid = data.dataResourceUid ?:  CollectoryService.NO_DATARESOURCE
        calculateImageSize(data)
        data
    }

    private def calculateImageSize(data) {
        def imageSize = (data.height?.toInteger() ?: 0) * (data.width?.toInteger() ?: 0)
        if (imageSize < 100){
            data.imageSize = "less than 100"
        } else if (imageSize < 1000){
            data.imageSize = "less than 1k"
        } else if (imageSize < 10000){
            data.imageSize = "less than 10k"
        } else if (imageSize < 100000){
            data.imageSize = "less than 100k"
        } else if (imageSize < 1000000){
            data.imageSize = "less than 1m"
        } else {
            data.imageSize = (imageSize / 1000000).intValue() + "m"
        }
    }

    def deleteImage(Image image) {
        if (image) {
            def deleteRequest = DeleteRequest.of(b ->
                b
                .index(grailsApplication.config.getProperty('elasticsearch.indexName'))
                .id(image.imageIdentifier)
            )
            try {
                def response = client.delete(deleteRequest)
            } catch (ElasticsearchException e) {
                log.error("Error deleting image ${image.imageIdentifier} from index: ${e.message}", e)
            }
        }
    }

    QueryResults<Map<String,Object>> simpleImageSearch(List<SearchCriteria> searchCriteria, GrailsParameterMap params) {
        log.debug "search params: ${params}"
        SearchRequest.Builder request = buildSearchRequest(params, searchCriteria, grailsApplication.config.getProperty('elasticsearch.indexName') as String)
        SearchResponse searchResponse = client.search(request.build(), Map<String,Object>)
        final imageList = searchResponse.hits()?.hits()?.collect { hit -> hit.source() } ?: []
        QueryResults<Image> qr = new QueryResults<Image>()
        qr.list = imageList
        qr.totalCount = searchResponse.hits().total().value()

        applyAggregationsToQueryResults(searchResponse, qr)

        qr
    }

    QueryResults<Image> simpleFacetSearch(List<SearchCriteria> searchCriteria, GrailsParameterMap params) {
        log.debug "search params: ${params}"
        SearchRequest.Builder request = buildFacetRequest(params, searchCriteria, params.facet, grailsApplication.config.getProperty('elasticsearch.indexName') as String)
        SearchResponse searchResponse = client.search(request.build(), Map<String,Object>)
        QueryResults<Image> qr = new QueryResults<Image>()

        applyAggregationsToQueryResults(searchResponse, qr)

        qr
    }

    private static applyAggregationsToQueryResults(SearchResponse searchResponse, QueryResults<Image> qr) {
        searchResponse.aggregations().each {
            def facet = [:]
            def key = it.key
            def aggregate = it.value
            Buckets buckets
            switch (aggregate._kind()) {
                case Aggregate.Kind.Sterms:
                    log.trace("Using STerms aggregation for facet: ${key}")
                    buckets = aggregate.sterms().buckets()
                    break
                case Aggregate.Kind.Dterms:
                    log.trace("Using DTerms aggregation for facet: ${key}")
                    buckets = aggregate.dterms().buckets()
                    break
                case Aggregate.Kind.Srareterms:
                    log.trace("Using Srareterms aggregation for facet: ${key}")
                    buckets = aggregate.srareterms().buckets()
                    break
                case Aggregate.Kind.Umterms:
                    log.trace("Using Umterms aggregation for facet: ${key}")
                    buckets = aggregate.umterms().buckets()
                    break
                default:
                    log.warn("Unknown aggregation type: ${aggregate._kind()}")
                    buckets = null
            }
            if (buckets) {
                if (buckets.isArray()) {
                    buckets.array().each {
                        facet[it.key().stringValue()] = it.docCount()
                    }
                } else if (buckets.isKeyed()) {
                    buckets.keyed().each {
                        facet[it.value.key().stringValue()] = it.value.docCount()
                    }
                }
            }

            qr.aggregations.put(key, facet)
        }
    }


    void simpleImageDownload(List<SearchCriteria> searchCriteria, GrailsParameterMap params, OutputStream outputStream) {

        ZipOutputStream out = new ZipOutputStream(outputStream)
        ZipEntry e = new ZipEntry("images.csv")
        out.putNextEntry(e)

        def csvWriter = new CSVWriter(new OutputStreamWriter(out))
        def PAGE_SIZE = 10000
        params.offset = 0
        params.max = 1000
        def totalWritten = 0
        def fields = null

        SearchRequest.Builder searchRequest = buildSearchRequest(params, searchCriteria, grailsApplication.config.getProperty('elasticsearch.indexName') as String)
        searchRequest.scroll(b -> b.time("1m"))

        def searchResponse = client.search(searchRequest.build(), Map<String, Object>)

        String scrollId = searchResponse.scrollId()
        def hits = searchResponse.hits()

        //Scroll until no hits are returned
        while (hits.hits().size() != 0) {
            for (def hit : hits.hits()) {
                def map = hit.source()
                if (fields == null){
                    fields = ["imageURL"]
                    fields.addAll(map.keySet().sort())
                    csvWriter.writeNext(fields as String[])
                }
                def values = []

                fields.each {
                    if(it == "imageURL"){
                        values.add(imageStoreService.getImageUrl(map.get("imageIdentifier")))
                    } else {
                        values.add(map.get(it) ?: "")
                    }
                }
                csvWriter.writeNext(values as String[])
                totalWritten += 1
            }

            ScrollRequest scrollRequest = new ScrollRequest.Builder()
                    .scrollId(scrollId)
                    .scroll(b -> b.time("30s"))
                    .build()

            searchResponse = client.scroll(scrollRequest, Map<String,Object>)
            scrollId = searchResponse.scrollId()
            hits = searchResponse.hits()
        }

        params.offset += PAGE_SIZE
        log.debug("Writing complete...." + totalWritten)
        csvWriter.flush()
        out.closeEntry()
        out.close()
    }

    /**
     * Execute the search using a map query.
     *
     * @param query
     * @param params
     * @return
     */
    QueryResults<Image> search(Map query, GrailsParameterMap params) {
        log.debug "search params: ${params}"
        SearchRequest.Builder request = buildSearchRequest(JsonOutput.toJson(query), params, grailsApplication.config.getProperty('elasticsearch.indexName') as String)
        SearchResponse searchResponse = client.search(request.build(), Map<String,Object>)
        final imageList = searchResponse.hits() ? Image.findAllByImageIdentifierInList(searchResponse.hits().hits()*.id()) ?: [] : []
        QueryResults<Image> qr = new QueryResults<Image>()
        qr.list = imageList
        qr.totalCount = searchResponse.hits().total().value()
        qr
    }

    /**
     * Build the search request object from query and params
     *
     * @param queryString
     * @param params
     * @param index index name
     * @param geoSearchCriteria geo search criteria.
     * @return SearchRequest
     */
    SearchRequest.Builder buildSearchRequest(Map params, List<SearchCriteria> criteriaList, String index) {

        def request = new SearchRequest.Builder()
        populateCommonSearchRequest(request, index, params, criteriaList)

        // request aggregations (facets)
        grailsApplication.config.getProperty('facets', List).each { facet ->
            request.aggregations(facet as String) { b -> b.terms(b2 -> b2.field(facet as String).size(10)) }
        }

        //ask for the total
        request.trackTotalHits(trackHits -> trackHits.enabled(true))

        if (params.highlight) {
            request.highlight(highlight -> highlight.preTags("<b>").postTags("</b>").fragmentSize(60).numberOfFragments(2).requireFieldMatch(false))
        }

        return request
    }


    /**
     * Build the search request object from query and params
     *
     * @param queryString
     * @param params
     * @param index index name
     * @param geoSearchCriteria geo search criteria.
     * @return SearchRequest
     */
    SearchRequest.Builder buildFacetRequest(Map params, List<SearchCriteria> criteriaList, String facet, String index) {

        SearchRequest.Builder request = new SearchRequest.Builder()
        populateCommonSearchRequest(request, index, params, criteriaList)

        // request aggregations (facets)
        final maxFacetSize = grailsApplication.config.getProperty('elasticsearch.maxFacetSize', Integer)
        request.aggregations(facet as String, b -> b.terms(b2 -> b2.field(facet as String).size(maxFacetSize).order(NamedValue.of("_key", SortOrder.Asc))))

        //ask for the total
        request.trackTotalHits(builder -> builder.enabled(false))

        request
    }

    private void populateCommonSearchRequest(SearchRequest.Builder request, String index, Map params, List<SearchCriteria> criteriaList) {
        request.searchType(SearchType.DfsQueryThenFetch)

        // set indices and types
        request.index(index)
        // TODO types was deprecated in the High Level REST Client and removed in the Java Client
        // Suggested alternative is to use filters in the query
//        def types = []
//        if (params.types && params.types instanceof Collection<String>) {
//            types = params.types
//        }
//        request.types(types as String[])

        //create query builder
        def boolQueryBuilder = QueryBuilders.bool()
        boolQueryBuilder.must(b -> b.queryString(b2 -> b2.query(params.q ?: "*:*")))

        // Add FQ query filters
        def filterQueries = params.findAll { it.key == 'fq' }
        boolean errorOccurred = false
        if (filterQueries) {
            filterQueries.each {

                if (it.value instanceof String[]) {
                    it.value.each { filter ->
                        if (filter) {
                            def kv = filter.toString().split(":", 2)
                            if (kv.length == 2) {
                                boolQueryBuilder.must(b -> b.term(b2 -> b2.field(kv[0]).value(kv[1])))
                            } else {
                                errorOccurred = true
                            }
                        }
                    }
                } else {
                    if (it.value) {
                        def kv = it.value.toString().split(":", 2)
                        if (kv.length == 2) {
                            boolQueryBuilder.must(b -> b.term(b2 -> b2.field(kv[0]).value(kv[1])))
                        } else {
                            errorOccurred = true
                        }
                    }
                }
            }
        }
        if (errorOccurred) {
            log.debug("Error occurred parsing filter queries - one or more filters were malformed.  fqs: ${filterQueries}")
        }

        //add search criteria
        boolQueryBuilder = createQueryFromCriteria(boolQueryBuilder, criteriaList)

        // set pagination stuff
        pagenateQuery(request, params)
        request.query(b -> b.bool(boolQueryBuilder.build()))
    }

    private void pagenateQuery(SearchRequest.Builder request, Map params) {

        int maxOffset = grailsApplication.config.getProperty('elasticsearch.maxOffset', Integer)
        int maxPageSize = grailsApplication.config.getProperty('elasticsearch.maxPageSize', Integer)
        int defaultPageSize = grailsApplication.config.getProperty('elasticsearch.defaultPageSize', Integer)

//        SearchSourceBuilder source = new SearchSourceBuilder()

        //set the page size
        def pageSize
        if (params.max){
            if ((params.max as int) > maxPageSize){
                pageSize = maxPageSize
            } else {
                pageSize = params.max as int
            }
        } else {
            pageSize = defaultPageSize
        }
        request.size(pageSize)

        //set the offset
        if (params.offset){
            if ((params.offset as int) > maxOffset){
                request.from(maxOffset - pageSize)
            } else {
                request.from((params.offset as int))
            }
        } else {
            request.from(0)
        }

        request.sort(b -> b.field(b2 -> b2.field("dateUploaded").order(SortOrder.Desc)))
    }

    private def initialiseIndex() {
        try {
            def indexName = grailsApplication.config.getProperty('elasticsearch.indexName') as String
            boolean indexExists  = client.indices().exists(b -> b.index(indexName)).value()
            if (!indexExists){
                CreateIndexRequest request = new CreateIndexRequest.Builder().index(indexName).build()
                CreateIndexResponse createIndexResponse = client.indices().create(request)
                if (createIndexResponse.acknowledged()) {
                    log.info "Successfully created index and mappings for images"
                } else {
                    log.info "UN-Successfully created index and mappings for images"
                }

                def putMappingRequest = new PutMappingRequest.Builder().index(indexName)
//                putMappingRequest.type(indexName)
                putMappingRequest.withJson(new StringReader(
                        """{
                                  "properties": {
                                    "dateUploaded": {
                                      "type": "date"
                                    },
                                    "dataResourceUid": {
                                      "type": "keyword"
                                    },               
                                    "license": {
                                      "type": "keyword"
                                    },  
                                    "recognisedLicence": {
                                      "type": "keyword"
                                    },    
                                    "imageSize":{
                                       "type": "keyword"
                                    },
                                    "dateUploadedYearMonth":{
                                       "type": "keyword"
                                    }, 
                                    "format":{
                                       "type": "keyword"
                                    },    
                                    "fileType":{
                                       "type": "keyword"
                                    },                                               
                                    "createdYear":{
                                       "type": "keyword"
                                    },                                                                     
                                    "creator": {
                                      "type": "text",
                                      "fielddata": true,
                                      "fields": {
                                        "keyword": { 
                                          "type": "keyword"
                                        }
                                      }                                      
                                    },          
                                    "title": {
                                      "type": "text",
                                      "fielddata": true,
                                      "fields": {
                                        "keyword": { 
                                          "type": "keyword"
                                        }
                                      }                                      
                                    }, 
                                    "description": {
                                      "type": "text",
                                      "fielddata": true,
                                      "fields": {
                                        "keyword": { 
                                          "type": "keyword"
                                        }
                                      }                                      
                                    },                                                                                                                                    
                                    "width": {
                                      "type": "integer"
                                    },                                                                     
                                    "height": {
                                      "type": "integer"
                                    },                                                                     
                                    "thumbHeight": {
                                      "type": "integer"
                                    },                                                                     
                                    "thumbWidth": {
                                      "type": "integer"
                                    },
                                    "contentMD5Hash": {
                                      "type": "keyword"
                                    },
                                    "contentSHA1Hash": {
                                      "type": "keyword"
                                    }
                                  }
                                }"""))
                def resp = client.indices().putMapping(putMappingRequest.build())
            } else {
                log.info "Index already exists"
            }

        } catch (Exception e) {
            log.error ("Error creating index for images: ${e.message}", e)
        }
    }

    /**
     * Create a boolean elastic search query builder from the supplied criteria.
     * @param criteriaList
     * @return
     */
    private BoolQuery.Builder createQueryFromCriteria(BoolQuery.Builder boolQueryBuilder, List<SearchCriteria> criteriaList) {

        def metaDataPattern = Pattern.compile("^(.*)[:](.*)\$")

        // split out by criteria type
        def criteriaMap = criteriaList.groupBy { it.criteriaDefinition.type }

        def list = criteriaMap[CriteriaType.ImageProperty]
        if (list) {
            ESSearchCriteriaUtils.buildCriteria(boolQueryBuilder, list)
        }

        list = criteriaMap[CriteriaType.ImageMetadata]
        if (list) {
            for (int i = 0; i < list.size(); ++i) {
                def criteria = list[i]
                // need to split the metadata name out of the value...
                def matcher = metaDataPattern.matcher(criteria.value)
                if (matcher.matches()) {
                    def term = matcher.group(2)?.replaceAll('\\*', '%')
                    term = term.replaceAll(":", "\\:")

                    boolQueryBuilder.must(QueryBuilders.bool().filter(QueryBuilders.queryString().query("${matcher.group(1)}:${term}").build()).build())
                }
            }
        }

        boolQueryBuilder
    }

    def filtered = ['class', 'active', 'metaClass', 'tags', 'keywords', 'metadata']

    Map asMap(Image image) {

        def props = image.properties.collect{it}.findAll { !filtered.contains(it.key) }
        def map =  [:]
        props.each {
            map[it.key] = it.value
        }
        map
    }

    Map searchByMetadata(String key, List<String> values, GrailsParameterMap params) {

        def properties = getMetadataKeys()
        def caseInsensitive = [:]
        properties.each { caseInsensitive.put(it.toLowerCase(), it)}
        String indexField = caseInsensitive.get(key.toLowerCase())

        def queryString = values.collect { "\"${it}\"" }.join(" OR ")
        def queryBuilder = QueryBuilders.queryString().query(queryString)

        //find indexed field......
        queryBuilder.defaultField(indexField)

        try {
            def ct = new CodeTimer("Index search")
            def searchRequest = new SearchRequest.Builder()
            searchRequest.index(grailsApplication.config.getProperty('elasticsearch.indexName') as String)
            searchRequest.query(b -> b.queryString(queryBuilder.build()))
            searchRequest.from(params.int("offset"))
            searchRequest.size(params.int("max") ?: grailsApplication.config.getProperty('elasticsearch.maxPageSize', Integer))
            if (params?.sort) {
                def order = params?.order == "asc" ? SortOrder.Asc : SortOrder.Desc
                searchRequest.sort(SortOptionsBuilders.field(it -> it.field(params.sort as String).order(order)))
            }
            SearchResponse searchResponse = client.search(searchRequest.build(), Map<String,Object>)

            ct.stop(true)

            ct = new CodeTimer("Object retrieval (${searchResponse.hits().hits().size()} of ${searchResponse.hits().total().value()} hits)")
            final hitsIdList = searchResponse.hits() ? searchResponse.hits().hits()*.id() : []
            final imageList = hitsIdList ? Image.findAllByImageIdentifierInList(hitsIdList)?.collect { image ->
                image.metadata = null
                image.tags = null
                asMap(image)
            } ?: [] : []
            ct.stop(true)

            def resultsKeyedByValue = [:]

            imageList.each {

                def caseInsensitiveMap = [:]
                it.each { k, v -> caseInsensitiveMap[k.toLowerCase()] = v }
                def keyValue = caseInsensitiveMap.get(indexField.toLowerCase())
                def list = resultsKeyedByValue.get(keyValue, [])
                list << it
                resultsKeyedByValue.put(keyValue, list)
            }

            return resultsKeyedByValue
        } catch (ElasticsearchException e) {
            log.warn(".ElasticsearchException thrown - this is expected behaviour for a new empty system: {}", e.message)
            return [:]
        } catch (Exception e) {
            e.printStackTrace()
            log.warn("Exception thrown - this is expected behaviour for a new empty system: {}", e.message)
            return [:]
        }
    }

    def getMetadataKeys() {
        def indexName = grailsApplication.config.getProperty('elasticsearch.indexName') as String
        GetMappingRequest request = new GetMappingRequest.Builder()
            .index(indexName)
            .build()
        GetMappingResponse getMappingResponse = client.indices().getMapping(request)
        def indexResponse = getMappingResponse.result().get(indexName)
        def propertyNames = indexResponse.mappings().properties().keySet()
        return propertyNames
    }
}
