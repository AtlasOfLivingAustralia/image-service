package au.org.ala.images

import groovy.util.logging.Slf4j

@Slf4j
class SearchCriteriaService {


    List<SearchCriteriaDefinition> criteriaDefinitionList = []

    List<SearchCriteriaDefinition> getCriteriaDefinitionList() {

        if (!criteriaDefinitionList) {

            try {
                def fields = Image.class.declaredFields

                fields.each { field ->
                    if (field.isAnnotationPresent(SearchableProperty)) {
                        def ann = field.getAnnotation(SearchableProperty)

                        def criteriaDefinition = SearchCriteriaDefinition.findByTypeAndFieldName(CriteriaType.ImageProperty, field.name)
                        if (!criteriaDefinition) {
                            log.debug("Creating new Image Property criteria definition for ${field.name}")
                            criteriaDefinition = new SearchCriteriaDefinition(name: "Image ${field.name}", type: CriteriaType.ImageProperty, valueType: ann.valueType(), fieldName: field.name, units: ann.units(), description: ann.description())
                            criteriaDefinition.save(flush: true, failOnError: false)
                        }
                    }
                }
                criteriaDefinitionList = SearchCriteriaDefinition.list().sort { it.name }
            } catch (Exception e){
                log.error(e.getMessage(), e)
            }
        }
        return criteriaDefinitionList
    }
}
