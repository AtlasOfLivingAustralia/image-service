<div>
    <p>
    ${message}
    </p>
    <div>
        <div class="mb-3">
            <button id="btnNo" class="btn btn-outline-dark">${negativeText}</button>
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