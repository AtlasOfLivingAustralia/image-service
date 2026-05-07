<div class="form-horizontal">

    <form>
        <label for="description">
            <g:message code="create.subimage.fragment.description" />
        </label>
        <input id="description" type="text" class="form-control input-xlarge" name="description" value=""/>
    </form>

    <div class="control-group">
        <div class="controls">
            <btn class="btn btn-default" id="btnCancelSubimage"><g:message code="create.subimage.fragment.cancel" /></btn>
            <btn class="btn btn-primary" id="btnCreateSubimage2"><g:message code="create.subimage.fragment.create.sub.image" /></btn>
        </div>
    </div>
</div>

<script>
    $("#btnCancelSubimage").on('click', function(e) {
        e.preventDefault();
        imgvwr.hideModal();
    });

    $("#btnCreateSubimage2").on('click', function(e) {
        e.preventDefault();
        var url = "${raw(createLink(absolute: true, controller:'webService', action:'createSubimage', id: imageInstance.imageIdentifier,  params:[x: x, y: y, width: width, height: height]))}&description=" + encodeURIComponent($('#description').val());
        $.ajax(url).done(function(results) {
            if (results.success) {
                imgvwr.hideModal();
            } else {
                alert("Failed to create sub image: " + results.message);
            }
        });
    });
</script>