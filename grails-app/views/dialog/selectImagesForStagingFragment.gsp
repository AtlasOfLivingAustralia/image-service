<div>

    <div class="alert">
        Depending on your connection speed and the size of your images, it might be a good idea to stage images in batches of 200 or less.
    </div>

    <g:form name="stageImagesForm" controller="image" action="stageImages" method="post" enctype="multipart/form-data" class="form-horizontal">

        <div class="control-group">
            <div class="controls">
                <input type="file" name="imageFile" id="imageFile" multiple="multiple"/>
            </div>
        </div>

        <div class="control-group">
            <div class="controls">
                <button id="btnCancelUploadImages" class="btn">Cancel</button>
                <button id="btnUploadImages" class="btn btn-primary">Stage images</button>
            </div>
        </div>

        <div class="control-group">
            <div id="uploadingMessage" style="display: none">
                <img:spinner /> Uploading, please wait...
            </div>
        </div>

    </g:form>

    <script>

        $("#btnCancelUploadImages").click(function(e) {
            e.preventDefault();
            imglib.hideModal();
        });

        $("#btnUploadImages").click(function(e) {
            e.preventDefault();
            $("#uploadingMessage").css("display", "block");
            $("#stageImagesForm").submit();
        });

    </script>

</div>