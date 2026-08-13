<g:if test="${imageInstance}">
    <div>
        <g:if test="${imageInstance.description}">
            <strong><img:sanitise value="${imageInstance.description}" image="${imageInstance.imageIdentifier}" key="description" /></strong>
            <div> ${img.maskUrlCredentials(value: imageInstance.originalFilename)}</div>
        </g:if>
        <g:else>
            <strong>${img.maskUrlCredentials(value: imageInstance.originalFilename)}</strong>
        </g:else>
        <div><g:message code="image.tooltip.frag.width.height" args="[imageInstance.width, imageInstance.height]" /></div>
        <div>${imageInstance.mimeType}&nbsp;&nbsp;<img:sizeInBytes size="${imageInstance.fileSize}" /></div>
        <div><g:message code="image.tooltip.frag.uploaded.on" /> <img:formatDate date="${imageInstance.dateUploaded}" /></div>
        <g:if test="${imageInstance.dateTaken != imageInstance.dateUploaded}">
            <div><g:message code="image.tooltip.frag.taken.on" /> <img:formatDate date="${imageInstance.dateTaken}" /></div>
        </g:if>
        <g:if test="${imageInstance.creator}">
            <div><img:sanitise value="${imageInstance.creator}" image="${imageInstance.imageIdentifier}" key="creator" /></div>
        </g:if>
        <g:if test="${imageInstance.title}">
            <div><img:sanitise value="${imageInstance.title}" image="${imageInstance.imageIdentifier}" key="title" /></div>
        </g:if>
        <g:if test="${imageInstance.rights}">
            <div><img:sanitise value="${imageInstance.rights}" image="${imageInstance.imageIdentifier}" key="rights" /></div>
        </g:if>
        <g:if test="${imageInstance.rightsHolder}">
            <div>
                <img:sanitise value="${imageInstance.rightsHolder}" image="${imageInstance.imageIdentifier}" key="rightsHolder" />
            </div>
        </g:if>
        <g:if test="${imageInstance.parent}">
            <div><small>*<g:message code="image.tooltip.frag.subimage.of" args="[img.maskUrlCredentials(value: imageInstance.parent?.originalFilename)]" /></small></div>
        </g:if>

        <a href="${createLink(absolute: true, controller: 'image', action: 'details', params:[id:imageInstance.id])}"><g:message code="image.tooltip.frag.view.metadata" /></a>
    </div>
</g:if>
<g:else>
    <div><g:message code="image.tooltip.frag.could.not.retrieve.image.for.id" /> ${params.id}</div>
</g:else>