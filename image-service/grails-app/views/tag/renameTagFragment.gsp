<div>

    <div class="mb-3">
        <label class="col-form-label" for="tag">Current name</label>
        <input type="text" class="form-control form-control-lg" readonly="true" id="existing" value="${tagInstance.label}">
    </div>

    <div class="mb-3">
        <label class="col-form-label" for="tag">New name</label>
        <input type="text" class="form-control form-control-lg" id="tag" placeholder="${tagInstance.label}" value="${tagInstance.label}">
    </div>

    <div class="mb-3">
        <button class="btn btn-primary" id="btnRenameTag">Rename Tag</button>
        <button class="btn btn-outline-dark" id="btnCancelRenameTag">Cancel</button>
    </div>
</div>
<script>

    $("input:text").focus(function() {
        $(this).select();
    });

    $("#btnCancelRenameTag").on('click', function(e) {
        e.preventDefault();
        bootstrap.Modal.getInstance(document.getElementById('tagModal')).hide();
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