<div>
    <p>
    ${message}
    </p>
    <div class="form-horizontal">
        <div class="control-group">
            <button id="btnNo" class="btn btn-default">${negativeText}</button>
            <button id="btnYes" class="btn btn-primary">${affirmativeText}</button>
        </div>
    </div>
</div>
<script>

    $("#btnYes").on('click', function(e) {
        e.preventDefault();
        if (imgvwr.areYouSureOptions && imgvwr.areYouSureOptions.affirmativeAction) {
            imgvwr.areYouSureOptions.affirmativeAction();
        }
        imgvwr.hideModal();
    });

    $("#btnNo").on('click', function(e) {
        e.preventDefault();
        if (imgvwr.areYouSureOptions && imgvwr.areYouSureOptions.negativeAction) {
            imgvwr.areYouSureOptions.negativeAction();
        }
        imgvwr.hideModal();
    });

</script>