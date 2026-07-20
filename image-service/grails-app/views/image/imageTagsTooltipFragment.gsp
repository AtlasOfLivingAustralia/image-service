<g:if test="${tags}">
    <g:each in="${tags}" var="tag">
        <span class="">
            <i class="fa fa-tag"></i>&nbsp;${tag.label}
        </span>
    </g:each>
</g:if>
<g:else>
    <div>
        <g:message code="details.no.tags" />
    </div>
</g:else>