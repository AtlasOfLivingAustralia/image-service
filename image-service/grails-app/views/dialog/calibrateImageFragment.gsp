<div class="form-horizontal">
    <div class="control-group">
        <g:message code="calibrate.image.how.long" />
    </div>
    <div class="control-group">
        <div class="controls">
            <g:textField class="form-control" name="pixelLength" value="${pixelLength}"/> pixels =
            <g:textField class="form-control" name="mmLength" value="" />
            <g:select name="units" class="input-small" from="${['mm','inches', 'metres','feet']}" value="mm"/>
        </div>
    </div>
    <div class="control-group">
        <div class="controls">
            <button class="btn btn-primary" id="btnCalibrateImageScale">Update</button>
            <button class="btn btn-default" id="btnCancelCalibrateImageScale">Cancel</button>
        </div>
    </div>
</div>
<script>

    $("#btnCancelCalibrateImageScale").on('click', function(e) {
        e.preventDefault();
        imgvwr.hideModal();
    });

    $("#btnCalibrateImageScale").on('click', function(e) {
        e.preventDefault();
        var units = $("#units").val();
        var pixelLength = $("#pixelLength").val();
        var actualLength = $("#mmLength").val();
        $.ajax("${createLink(absolute: true, controller:'webService', action:'calibrateImageScale', params:[imageId: imageInstance.imageIdentifier])}&units=" + units + "&pixelLength=" + pixelLength + "&actualLength=" + actualLength).done(function() {
            imgvwr.hideModal();
        });

        <g:if test="${params.callback}">
        ${params.callback}(pixelLength);
        </g:if>
    });

</script>