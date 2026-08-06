package au.org.ala.images

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.Method

class ElasticsearchQuerySerializationSpec extends Specification {

    def "metadata criteria create serializable nested query_string queries"() {
        given:
        def criteria = new SearchCriteria(
                criteriaDefinition: new SearchCriteriaDefinition(type: CriteriaType.ImageMetadata),
                value: 'metadata.camera:Canon*'
        )
        BoolQuery.Builder queryBuilder = invokeCreateQueryFromCriteria(new ElasticSearchService(), [criteria])

        when:
        String json = serialize(queryBuilder.build()._toQuery())

        then:
        noExceptionThrown()
        json.contains('"query_string":{"query":"metadata.camera:Canon%"}')
    }

    def "multi-value string criteria create serializable query_string filters"() {
        given:
        def criteria = criteria(CriteriaValueType.StringMultiSelect, 'tags', 'bird~mammal')

        when:
        String json = serialize(ESSearchCriteriaUtils.factory(criteria).createQueryBuilder(criteria)._toQuery())

        then:
        noExceptionThrown()
        json.contains('"query_string":{"query":"tags:bird"}')
        json.contains('"query_string":{"query":"tags:mammal"}')
    }

    @Unroll
    def "double #operator criteria create serializable range queries"() {
        given:
        def criteria = criteria(CriteriaValueType.NumberRangeDouble, 'decimalField', value)

        when:
        String json = serialize(ESSearchCriteriaUtils.factory(criteria).createQueryBuilder(criteria)._toQuery())

        then:
        noExceptionThrown()
        json.contains(expectedContent)

        where:
        operator | value       || expectedContent
        'equality' | 'eq 1.5' || '"query_string":{"query":"decimalField:1.5"}'
        'less than' | 'lt 2.5' || '"range":{"decimalField":{"lte":2.5}}'
        'greater than' | 'gt 3.5' || '"range":{"decimalField":{"gte":3.5}}'
        'between' | 'bt 5:4' || '"range":{"decimalField":{"gte":4.0,"lte":5.0}}'
    }

    private static SearchCriteria criteria(CriteriaValueType valueType, String fieldName, String value) {
        new SearchCriteria(
                criteriaDefinition: new SearchCriteriaDefinition(type: CriteriaType.ImageProperty, valueType: valueType, fieldName: fieldName),
                value: value
        )
    }

    private static BoolQuery.Builder invokeCreateQueryFromCriteria(ElasticSearchService service, List<SearchCriteria> criteria) {
        Method method = ElasticSearchService.getDeclaredMethod('createQueryFromCriteria', BoolQuery.Builder, List)
        method.accessible = true
        method.invoke(service, QueryBuilders.bool(), criteria) as BoolQuery.Builder
    }

    private static String serialize(Query query) {
        def writer = new StringWriter()
        def mapper = new JacksonJsonpMapper()
        def generator = mapper.jsonProvider().createGenerator(writer)
        new SearchRequest.Builder().index('images').query(query).build().serialize(generator, mapper)
        generator.close()
        writer.toString()
    }
}
