<div>
    <g:uploadForm  class="form-horizontal" action="uploadTagsFile">

        <p>
            The selected file should be simply a list of tag paths, with each hierarchical component separated by a slash '/'.
        </p>
        <p>For example</p>
        <pre>
/tag1/tag2/tag3
/tag1/tag2/tag4
        </pre>

        <div class="form-group">
            <label class="form-control" for="tagfile">Tag file</label>
            <input type="file" name="tagfile" id="tagfile" />
        </div>

        <div class="form-group">
            <button id="btnCancelUpload" class="btn btn-default">Cancel</button>
            <button id="btnUploadTags" type="submit" class="btn btn-primary">Load tags</button>
        </div>
    </g:uploadForm>

</div>
<script>
    $("#btnCancelUpload").on('click', function(e) {
        e.preventDefault();
        $('#tagModal').modal('hide');
    });
</script>