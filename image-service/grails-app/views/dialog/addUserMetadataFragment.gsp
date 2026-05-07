<%@ page import="au.org.ala.images.MetaDataSourceType" %>
<div>

    <form>
        <div class="mb-3">
            <label class="col-form-label" for="metaDataKey">Name:</label>
            <input type="text" class="form-control form-control-lg" id="metaDataKey" placeholder="Metadata key">
        </div>

        <div class="mb-3">
            <label class="col-form-label" for="metaDataValue"><g:message code="add.user.metadata.value" /></label>
            <input type="text" class="form-control form-control-lg" id="metaDataValue" placeholder="Value">
        </div>

        <div class="mb-3">
            <button class="btn btn-primary" id="btnAddNewUserMetadata"><g:message code="add.user.metadata.add" /></button>
            <button class="btn btn-outline-dark" id="btnCancelAddUserMetaData"><g:message code="add.user.metadata.cancel" /></button>
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
