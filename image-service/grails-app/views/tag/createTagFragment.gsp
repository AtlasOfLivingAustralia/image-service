<div>
    <div class="well well-small">
        <g:if test="${parentTag}">
                Enter a name for your new tag. It will be created under <strong>${parentTag.path}</strong>.
        </g:if>
        <g:else>
                Enter a new tag name. Tag hierarchy elements can be delimited with '/'.
        </g:else>
    </div>
    <form>
        <div class="form-group">
            <label for="tag">Tag name</label>
            <input type="text" id="tag" class="form-control input-lg" placeholder="<new tag>">
        </div>

        <div class="control-group">
            <div class="controls">
                <button class="btn btn-primary" id="btnAddTag">Create Tag</button>
                <button class="btn btn-default" id="btnCancelAddTag">Cancel</button>
            </div>
        </div>
    </form>
</div>
<script>

    $("#btnCancelAddTag").on('click', function(e) {
        e.preventDefault();
        $('#tagModal').modal('hide');
    });

    $("#btnAddTag").on('click', function(e) {
        e.preventDefault();
        var tagPath = $("#tag").val();
        if (tagPath) {
            $.ajax("${createLink(controller:'webService', action:'createTagByPath')}?tagPath=" + tagPath + "&parentTagId=${parentTag?.id}").done(function(results) {
                $('#tagModal').modal('hide');
            });
        }
    });

</script>