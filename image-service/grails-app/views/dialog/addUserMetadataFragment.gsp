<%@ page import="au.org.ala.images.MetaDataSourceType" %>
<div>

    <form>
        <div class="form-group">
            <label class="control-label" for="metaDataKey">Name:</label>
            <input type="text" class="form-control input-lg" id="metaDataKey" placeholder="Metadata key">
        </div>

        <div class="form-group">
            <label class="control-label" for="metaDataValue"><g:message code="add.user.metadata.value" /></label>
            <input type="text" class="form-control input-lg" id="metaDataValue" placeholder="Value">
        </div>

        <div class="form-group">
            <button class="btn btn-primary" id="btnAddNewUserMetadata"><g:message code="add.user.metadata.add" /></button>
            <button class="btn btn-default" id="btnCancelAddUserMetaData"><g:message code="add.user.metadata.cancel" /></button>
        </div>
    </form>

    <script>

        $("#btnCancelAddUserMetaData").on('click', function(e) {
            e.preventDefault();
            imgvwr.hideModal();
        });

        $("#btnAddNewUserMetadata").on('click', function(e) {
            e.preventDefault();
            var key = $("#metaDataKey").val();
            var value = $("#metaDataValue").val();
            if (key && value) {
                key = encodeURIComponent(key);
                value = encodeURIComponent(value);
                if (imgvwr.onAddMetadata) {
                    imgvwr.onAddMetadata(key, value);
                }
            }
        });
    </script>
</div>
