<%@ page import="au.org.ala.images.ImportFieldType" %>
<div>

    <div class="mb-3">
        <label for="fieldType">Field type</label>
        <g:select  class="form-select" name="fieldType" id="fieldType" from="${ImportFieldType.values()}" value="${fieldDefinition?.fieldType}" />
    </div>

    <div class="mb-3">
        <label for="fieldName">Field name</label>
        <g:textField  class="form-control" name="fieldName" id="fieldName" value="${fieldDefinition?.fieldName}"/>
    </div>

    <div class="mb-3">
        <label for="value">Value</label>
        <g:textField  class="form-control" name="value"  id="value" value="${fieldDefinition?.value}" />
    </div>

    <p class="card card-body">
        Note: Filename regex - can be used to derive fields from parts of the image file name e.g. title
    </p>

    <div class="mb-3">
        <button class="btn btn-outline-dark" id="btnCancel">Cancel</button>
        <button class="btn btn-primary" id="btnCreateNewField">${fieldDefinition ? "Save" : "Add"} Field</button>
    </div>

    <script>

        $("#btnCancel").on('click', function(e) {
            e.preventDefault();
            $('#ingestModal').modal('hide');
        });

        $("#btnCreateNewField").on('click', function(e) {
            e.preventDefault();
            var name = encodeURIComponent($("#fieldName").val());
            var type = encodeURIComponent($("#fieldType").val());
            var value = encodeURIComponent($("#value").val());
            if (name && type && value) {
                $.ajax("${createLink(action:'saveFieldDefinition')}?name=" + name + "&type=" + type + "&value=" + value).done(function(results) {
                    if (!results.success) {
                        alert(results.message);
                    } else {
                        $('#ingestModal').modal('hide');
                    }
                });
            }
        });
    </script>

</div>