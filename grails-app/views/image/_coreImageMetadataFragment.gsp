<table class="table table-bordered table-condensed table-striped">
    <g:if test="${imageInstance.dateDeleted}">
        <h5 class="alert alert-danger"><g:message code="core.image.metadata.this.image.is.deleted" /></h5>
    </g:if>
    <g:if test="${imageInstance.dataResourceUid}">
        <tr>
            <td class="property-name"><g:message code="core.image.metadata.data.resource" /></td>
            <td class="property-value">
                <a href="${grailsApplication.config.getProperty('collectory.baseURL')}/public/show/${imageInstance.dataResourceUid}">
                    ${resourceLevel.name}
                </a>
            </td>
        </tr>
    </g:if>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.image.identifier" /></td>
        <td class="property-value">${imageInstance.imageIdentifier}</td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.occurrence.id" /></td>
        <td id="occurrences_container" class="property-value">
            <i id="occurrences_spinner" class="fa fa-gear fa-spin"></i>
        </td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.title" /></td>
        <td class="property-value"><img:sanitise value="${imageInstance.title}" image="${imageInstance.imageIdentifier}" key="title"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.creator" /></td>
        <td class="property-value"><img:imageMetadata image="${imageInstance}" resource="${resourceLevel}" field="creator"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.created" /></td>
        <td class="property-value"><img:sanitise value="${imageInstance.created}" image="${imageInstance.imageIdentifier}" key="created"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.description" /></td>
        <td class="property-value"><img:sanitise value="${imageInstance.description}" image="${imageInstance.imageIdentifier}" key="description"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.publisher" /></td>
        <td class="property-value"><img:sanitise value="${imageInstance.publisher}" image="${imageInstance.imageIdentifier}" key="publisher"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.references" /></td>
        <td class="property-value"><img:sanitise value="${imageInstance.references}" image="${imageInstance.imageIdentifier}" key="references"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.audience" /></td>
        <td class="property-value"><img:sanitise value="${imageInstance.audience}" image="${imageInstance.imageIdentifier}" key="audience"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.source" /></td>
        <td class="property-value"><img:sanitise value="${imageInstance.source}" image="${imageInstance.imageIdentifier}" key="source"/></td>
    </tr>

    <g:if test="${isAdminView}">
        <tr>
            <td class="property-name"><g:message code="core.image.metadata.zoom.levels" /></td>
            <td class="property-value">${imageInstance.zoomLevels}</td>
        </tr>
    </g:if>
    <g:if test="${isImage}">

        <tr>
            <td class="property-name"><g:message code="core.image.metadata.linear.scale" /></td>
            <td class="property-value">
                <g:if test="${imageInstance.mmPerPixel}">
                    <g:message code="core.image.metadata.mm.per.pixel" args="[imageInstance.mmPerPixel]" />
                    <button id="btnResetLinearScale" type="button" class="btn btn-sm btn-default pull-right" title="Reset calibration">
                        <i class="glyphicon glyphicon-remove"></i>
                    </button>
                </g:if>
                <g:else>
                    <g:message code="core.image.metadata.not.calibrated" />
                </g:else>
            </td>
        </tr>
    </g:if>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.dateUploadedYearMonth" /></td>
        <td class="property-value"><img:formatDateTime date="${imageInstance.dateUploaded}" /></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.uploaded.by" /></td>
        <td class="property-value"><img:userDisplayName userId="${imageInstance.uploader}" /></td>
    </tr>
    <g:if test="${imageInstance.dateTaken}">
        <tr>
            <td class="property-name"><g:message code="core.image.metadata.date.taken.created" /></td>
            <td class="property-value"><img:formatDateTime date="${imageInstance.dateTaken}" /></td>
        </tr>
    </g:if>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.rights" /></td>
        <td class="property-value"><img:imageMetadata image="${imageInstance}" resource="${resourceLevel}" field="rights"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.rights.holder" /></td>
        <td class="property-value"><img:imageMetadata image="${imageInstance}" resource="${resourceLevel}" field="rightsHolder"/></td>
    </tr>
    <tr>
        <td class="property-name"><g:message code="core.image.metadata.recognisedLicence" /></td>
        <td class="property-value">
            <g:if test="${imageInstance.recognisedLicense}">
                <a href="${imageInstance.recognisedLicense.url}" title="${imageInstance.recognisedLicense.name +  ' (' + imageInstance.recognisedLicense.acronym + ')'}">
                    <img src="${imageInstance.recognisedLicense.imageUrl}">
                </a>
            </g:if>
            <g:else>
                <img:imageMetadata image="${imageInstance}" resource="${resourceLevel}" field="license"/>
            </g:else>
        </td>
    </tr>
    <g:if test="${imageInstance.dateDeleted}">
        <tr>
            <td class="property-name"><g:message code="core.image.metadata.date.deleted" /></td>
            <td class="property-value">${imageInstance.dateDeleted}</td>
        </tr>
    </g:if>
    <g:if test="${parentImage}">
        <tr>
            <td colspan="2">
                <h5>Parent image</h5>
                <g:link controller="image" action="details" id="${parentImage.imageIdentifier}">
                <img class="subimages_thumbs thumbnail" src="${g.createLink(controller: 'image', action: 'proxyImageThumbnail', params: ['id':parentImage.imageIdentifier])}"
                     alt="${ parentImage.imageIdentifier}"
                />
                </g:link>
                <i class="icon-info-sign image-info-button"></i>
            </td>
        </tr>
    </g:if>
    <g:if test="${subimages}">
        <tr>
            <td colspan="2">
                <h5><g:message code="core.image.metadata.sub.images" /></h5>
                <ul class="list-unstyled list-inline">
                    <g:each in="${subimages}" var="subimage">
                        <li imageId="${subimage.id}">
                            <g:link controller="image" action="details" id="${subimage.imageIdentifier}">
                            <img class="subimages_thumbs thumbnail" src="${g.createLink(controller: 'image', action: 'proxyImageThumbnail', params: ['id':subimage.imageIdentifier])}"
                                alt="${ subimage.imageIdentifier}"
                            />
                            <i class="icon-info-sign image-info-button"></i>
                            </g:link>
                        </li>
                    </g:each>
                </ul>
            </td>
        </tr>
    </g:if>
    <tr>
        <td colspan="2">
            <g:link mapping="image_ws_url" params="[imageId:imageInstance.imageIdentifier + '.json',includeMetadata:true,includeTags:true]" title="View JSON metadata" class="btn btn-default">
                <i class="glyphicon glyphicon-wrench"> </i>
            </g:link>
            <g:if test="${isImage}">
                <button class="btn btn-default" id="btnViewImage" title="View zoomable image"><span class="glyphicon glyphicon-eye-open"> </span></button>
            </g:if>
            <a class="btn btn-default" href="${createLink(controller:'image', action:'getOriginalFile', id:imageInstance.id, params:[contentDisposition: 'true'])}" title="${g.message(code:'core.image.metadata.download', default: 'Download full image')}" target="imageWindow"><i class="glyphicon glyphicon-download-alt"></i></a>
            <g:if test="${isAdminView}">
                <button class="btn btn-default" id="btnRegen" title="Regenerate artifacts"><i class="glyphicon glyphicon-refresh"></i></button>
            </g:if>
            <g:if test="${isAdminView}">
                <button class="btn btn-danger" id="btnDeleteImage" title="Delete image (admin)">
                    <i class="glyphicon glyphicon-remove  glyphicon-white"></i>
                </button>
            </g:if>
            <g:if test="${!isAdminView && (userId && userId.toString() == imageInstance.uploader) }">
                <button class="btn btn-danger" id="btnDeleteImage" title="${g.message(code:'core.image.metadata.delete', default: 'Delete your image')}">
                    <i class="glyphicon glyphicon-remove  glyphicon-white"></i>
                </button>
            </g:if>
            <g:if test="${isAdmin && !isAdminView}">
                <g:link class="btn btn-danger" controller="admin" action="image" params="[imageId: imageInstance.imageIdentifier]"><g:message code="core.image.metadata.admin.view" /></g:link>
            </g:if>
        </td>
    </tr>
</table>
<g:set var="images_param_name" value="${grailsApplication.config.getProperty('biocache.imagesFieldName', 'all_image_url')}" />
<script>
    $(document).ready(function() {
        $("#btnResetLinearScale").on('click', function (e) {
            e.preventDefault();
            imgvwr.areYouSure({
                title: <g:message code="core.image.metadata.reset.calibration.for.this.image" />,
                message: <g:message code="core.image.metadata.are.you.sure.scale" />,
                affirmativeAction: function () {
                    var url = "${createLink(absolute: true, controller:'webService', action:'resetImageCalibration')}?imageId=${imageInstance.imageIdentifier}";
                    $.ajax(url).done(function (result) {
                        window.location.reload(true);
                    });
                }
            });
        });
        findOccurrencesForImage();
    });

    function findOccurrencesForImage() {
        $.get("${u.createLink(baseProperty: 'biocache.service.baseURL', pathsProperty: 'biocache.service.searchPath')}", {'fl': 'id', 'q': "${images_param_name}:\"${imageInstance.imageIdentifier}\""})
        .done(function (data) {
            var ul = $('<ul></ul>');
            var occurrences = data.occurrences;
            var pageSize = data.pageSize;
            var startIndex = data.startIndex;
            var totalRecords = data.totalRecords;

            for (var i = 0; i < occurrences.length; ++i) {
                var occurrenceId = occurrences[i].uuid
                ul.append($('<li></li>').append($('<a></a>').attr('href', '${grailsApplication.config.getProperty('biocache.baseURL')}/occurrences/' + encodeURIComponent(occurrenceId)).text(occurrenceId)));
            }
            if (startIndex + pageSize < totalRecords) {
                ul.append($('<li></li>').append($('<a href="${grailsApplication.config.getProperty('biocache.baseURL')}/occurrences/search?q=${images_param_name}:${imageInstance.imageIdentifier}"><g:message code="core.image.metadata.find.all.records"/>...</a>')));
            }
            $('#occurrences_container').append(ul);
        }).always(function() {
            $('#occurrences_spinner').hide();
        });
    }

</script>