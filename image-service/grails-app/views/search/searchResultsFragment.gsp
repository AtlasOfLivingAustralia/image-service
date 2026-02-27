<div class="">
    <%
        def toolButtons = []
        toolButtons << [label: "Select all matching images (${totalCount})", id:"btnSelectAllImages"]
    %>
    <g:render template="/search/imageThumbnails"
              model="${[images: imageList, totalImageCount: totalCount, allowSelection: true, selectedImageMap: selectedImageMap, thumbsTitle: "${totalCount} matching images", toolButtons: toolButtons]}" />
</div>
<script>
    $("#btnSelectAllImages").on('click', function(e) {
        e.preventDefault();
        imgvwr.showSpinner("Selecting images...")
        $.ajax("${createLink(controller: 'search', action: 'ajaxSelectAllCurrentQuery')}").done(function() {
            imgvwr.hideSpinner();
            location.reload(true);
        });
    });
</script>