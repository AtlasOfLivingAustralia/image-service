<div>

    <div class="form-group">
        <label class="control-label" for="tag">Current name</label>
        <input type="text" class="form-control input-lg" readonly="true" id="existing" value="${tagInstance.label}">
    </div>

    <div class="form-group">
        <label class="control-label" for="tag">New name</label>
        <input type="text" class="form-control input-lg" id="tag" placeholder="${tagInstance.label}" value="${tagInstance.label}">
    </div>

    <div class="form-group">
        <button class="btn btn-primary" id="btnRenameTag">Rename Tag</button>
        <button class="btn btn-default" id="btnCancelRenameTag">Cancel</button>
    </div>
</div>
<script>

    $("input:text").focus(function() {
        $(this).select();
    });

    $("#btnCancelRenameTag").on('click', function(e) {
        e.preventDefault();
        $('#tagModal').modal('hide');
    });

    $("#btnRenameTag").on('click', function(e) {
        e.preventDefault();
        var newSuffix = $("#tag").val();
        if (newSuffix) {
            $.ajax("${createLink(controller:'webService', action:'renameTag')}?tagID=${tagInstance.id}&name=" + newSuffix).done(function() {
                $('#tagModal').modal('hide');
            });
        }
    });

</script>