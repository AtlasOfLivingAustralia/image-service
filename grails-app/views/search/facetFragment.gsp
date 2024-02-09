
<g:if test="${facetValues.size() >= grailsApplication.config.getProperty('elasticsearch.maxFacetSize', Integer)}">
<p>Note: Only the first ${grailsApplication.config.getProperty('elasticsearch.maxFacetSize')} are displayed</p>
</g:if>

<table class="table table-bordered table-condensed table-striped scrollTable" id="fullFacets">
<thead>
<th><g:message code="facet.${facet}" default="${facet}"/></th>
<th><g:message code="facet.count" /></th>
</thead>

<tbody class="scrollContent">

<g:each in="${facetValues}" var="facetField">
<tr class="normalRow">
    <td class="multiple-facet-value">
        <a href="${g.createLink(controller:'search', action:'list')}?q=${params.q}${filterQueries}&fq=${facet}:${facetField.key}">
            <g:if test="${facet =='dataResourceUid'}">
                <img:facetDataResourceResult dataResourceUid="${facetField.key}"/>
            </g:if>
            <g:else>
                <g:message code="${facetField.key}" default="${facetField.key}" />
            </g:else>
        </a>
    </td>
    <td class="multiple-facet-count" style="border-right: none;">${facetField.value}</td>
</tr>
</g:each>
</tbody>
</table>
