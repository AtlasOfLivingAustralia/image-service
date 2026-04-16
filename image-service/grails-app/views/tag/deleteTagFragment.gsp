<div class="form-horizontal">

    <div class="alert alert-danger">
        <h4>Warning</h4>
        Are you sure you wish to permanently delete tag <strong>'${tagInstance.label}'</strong> and all of its descendants? This tag, and any of it's children, will be removed from any images to which they are currently attached.'
    </div>

    <div class="control-group">
        <div class="controls">
            <button class="btn btn-danger" id="btnDeleteTag">Delete Tag</button>
            <button class="btn btn-default" id="btnCancelDeleteTag">Cancel</button>
        </div>
    </div>
</div>
<script>

    $("#btnCancelDeleteTag").on('click', function(e) {
        e.preventDefault();
        $('#tagModal').modal('hide');
    });

    $("#btnDeleteTag").on('click', function(e) {
        e.preventDefault();
        $.ajax("${createLink(controller:'webService', action:'deleteTag')}?tagId=${tagInstance.id}").done(function() {
            $('#tagModal').modal('hide');
        });
    });

</script>