<%@ page import="au.org.ala.web.CASRoles" %>
<style>

.image-tag button.close {
    line-height: 16px;
    font-size: 14px;
}

</style>
<g:if test="${tags}">
<h4>Tags</h4>

<g:each in="${tags}" var="tag">
    <g:set var="deleteButtonContent" value="" />
%{--    <auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN}">--}%
%{--        <g:set var="deleteButtonContent"><button type="button" class="close btnDetachTag" tagId="${tag.id}">&times;</button></g:set>--}%
%{--    </auth:ifAnyGranted>--}%
    <div class="badge image-tag" style="white-space: pre-wrap">${tag.label}&nbsp;&nbsp;${raw(deleteButtonContent)}</div>
</g:each>
</g:if>
<script>

    $(document).ready(function() {
        $("#btnAttachTag").on('click', function(e) {
            e.preventDefault();

            imgvwr.selectTag(function(tagId) {
                if (tagId) {
                    $.ajax("${createLink(controller:'webService', action:'attachTagToImage', id: imageInstance.imageIdentifier)}?tagID=" + tagId).done(function() {
                        if (loadTags) {
                            loadTags();
                        }
                    });
                }
            });
        });

        $(".btnDetachTag").on('click', function(e) {
            e.preventDefault();
            var tagId = $(this).attr("tagId");
            $.ajax("${createLink(controller:'webService', action:'detachTagFromImage', id: imageInstance.imageIdentifier)}?tagID=" + tagId).done(function() {
                if (loadTags) {
                    loadTags();
                }
            });
        });

    });
</script>
