<%@ page import="au.org.ala.images.ImportFieldType" %>
<div>

    <div class="form-group">
        <label for="fieldType">Field type</label>
        <g:select  class="form-control" name="fieldType" id="fieldType" from="${ImportFieldType.values()}" value="${fieldDefinition?.fieldType}" />
    </div>

    <div class="form-group">
        <label for="fieldName">Field name</label>
        <g:textField  class="form-control" name="fieldName" id="fieldName" value="${fieldDefinition?.fieldName}"/>
    </div>

    <div class="form-group">
        <label for="value">Value</label>
        <g:textField  class="form-control" name="value"  id="value" value="${fieldDefinition?.value}" />
    </div>

    <p class="well">
        Note: Filename regex - can be used to derive fields from parts of the image file name e.g. title
    </p>

    <div class="form-group">
        <button class="btn btn-default" id="btnCancel">Cancel</button>
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