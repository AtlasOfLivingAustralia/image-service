<%@ page import="au.org.ala.web.CASRoles; au.org.ala.images.MetaDataSourceType" %>
<auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN},${CASRoles.ROLE_USER}">
<g:if test="${source == MetaDataSourceType.UserDefined}">
    <btn class="btn btn-success" id="btnAddUserMetaData" style="margin-bottom: 5px"><i class="icon-plus icon-white"></i>&nbsp;Add item</btn>
</g:if>
</auth:ifAnyGranted>

<g:if test="${metaData}">
    <table class="table table-bordered table-condensed table-striped">
        <g:each in="${metaData}" var="md">
            <tr metaDataKey="${md.key}">
                <td class="property-name">${md.key}</td>
                <td class="property-value"><img:renderMetaDataValue metaDataItem="${md}" />
                    <auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN}">
                        <g:if test="${source == MetaDataSourceType.UserDefined}">
                            <button class="btn btn-small btn-danger btnDeleteMetadataItem pull-right"><i class="icon-remove icon-white"></i></button>
                        </g:if>
                    </auth:ifAnyGranted>
                </td>
            </tr>
        </g:each>
    </table>
</g:if>
<g:else>
    <div class="muted">
        No items
    </div>
</g:else>

<script>

    <auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN}">

    $("#btnAddUserMetaData").on('click', function(e) {
        e.preventDefault();
        imgvwr.promptForMetadata(function(key, value) {
            $.ajax("${createLink(absolute: true, controller:'webService', action:'addUserMetadataToImage', id: imageInstance.imageIdentifier)}?key=" + key + "&value=" + value).done(function() {
                if (refreshMetadata) {
                    refreshMetadata($("#tabUserDefined"));
                }
            });
        });
    });

    $(".btnDeleteMetadataItem").on('click', function(e) {
        var metaDataKey = $(this).closest("[metaDataKey]").attr("metaDataKey");
        if (metaDataKey) {
            $.ajax("${createLink(absolute: true, controller:"webService", action: 'removeUserMetadataFromImage', id:imageInstance.imageIdentifier)}?key=" + metaDataKey).done(function(results) {
                if (refreshMetadata) {
                    refreshMetadata($("#tabUserDefined"));
                }
            });
        }
    });

    </auth:ifAnyGranted>

</script>
