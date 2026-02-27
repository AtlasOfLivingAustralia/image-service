<div>

    <div class="alert">
        <g:message code="select.images.for.staging.title" />
    </div>

    <g:form name="stageImagesForm" controller="image" action="stageImages" method="post" enctype="multipart/form-data" class="form-horizontal">

        <div class="control-group">
            <div class="controls">
                <input type="file" name="imageFile" id="imageFile" multiple="multiple"/>
            </div>
        </div>

        <div class="control-group">
            <div class="controls">
                <button id="btnCancelUploadImages" class="btn"><g:message code="select.images.for.staging.cancel" /></button>
                <button id="btnUploadImages" class="btn btn-primary"><g:message code="select.images.for.staging.stage.images" /></button>
            </div>
        </div>

        <div class="control-group">
            <div id="uploadingMessage" style="display: none">
                <img:spinner /> <g:message code="select.images.for.staging.uploading.please.wait" />
            </div>
        </div>

    </g:form>

    <script>

        $("#btnCancelUploadImages").on('click', function(e) {
            e.preventDefault();
            imgvwr.hideModal();
        });

        $("#btnUploadImages").on('click', function(e) {
            e.preventDefault();
            $("#uploadingMessage").css("display", "block");
            $("#stageImagesForm").submit();
        });

    </script>

</div>