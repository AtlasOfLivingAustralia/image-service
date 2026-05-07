<div>
    <div class="card p-3">
        <g:if test="${parentTag}">
                Enter a name for your new tag. It will be created under <strong>${parentTag.path}</strong>.
        </g:if>
        <g:else>
                Enter a new tag name. Tag hierarchy elements can be delimited with '/'.
        </g:else>
    </div>
    <form>
        <div class="mb-3">
            <label for="tag">Tag name</label>
            <input type="text" id="tag" class="form-control form-control-lg" placeholder="<new tag>">
        </div>

        <div class="mb-3">
            <div class="controls">
                <button class="btn btn-primary" id="btnAddTag">Create Tag</button>
                <button class="btn btn-outline-dark" id="btnCancelAddTag">Cancel</button>
            </div>
        </div>
    </form>
</div>
<script>

    $("#btnCancelAddTag").on('click', function(e) {
        e.preventDefault();
        bootstrap.Modal.getInstance(document.getElementById('tagModal')).hide();
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