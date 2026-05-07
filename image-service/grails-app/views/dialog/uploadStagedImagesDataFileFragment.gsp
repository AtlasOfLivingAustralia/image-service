<g:form controller="image" action="uploadStagingDataFile" method="post" enctype="multipart/form-data" class="form-horizontal">

    <div class="control-group">
        <label for="dataFile"><g:message code="upload.staged.images.select.a.data.file" /></label>
        <div class="controls">
            <input type="file" name="dataFile" id="dataFile"/>
        </div>
    </div>
    <br/>

    <div><g:message code="upload.staged.images.csv" />
        <ul>
            <li><g:message code="upload.staged.images.li1" /></li>
            <li><g:message code="upload.staged.images.li2" />
            </li>
            <li><g:message code="upload.staged.images.li3" /></li>
            <li><g:message code="upload.staged.images.li4" /></li>
            <li><g:message code="upload.staged.images.li5" />
            </li>
        </ul>
    </div>

    <div class="control-group">
        <div class="controls">
            <button class="btn" id="btnCancelDataFileUpload"><g:message code="upload.staged.images.cancel" /></button>
            <g:submitButton class="btn btn-primary" name="Upload Data File"/>
        </div>
    </div>

</g:form>

<script>

    $("#btnCancelDataFileUpload").on('click', function(e) {
        e.preventDefault();
        imgvwr.hideModal();
    });

</script>
