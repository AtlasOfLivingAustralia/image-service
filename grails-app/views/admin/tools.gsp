<!doctype html>
<html>
<head>
    <meta name="layout" content="adminLayout"/>
    <title>Images service | Admin | Tools</title>
    <style type="text/css" media="screen">
    </style>
</head>

<body>
    <content tag="pageTitle">Tools</content>
    <content tag="adminButtonBar" />

    <g:if test="${flash.message}">
        <div class="alert alert-success" style="display: block">${flash.message}</div>
    </g:if>
    <g:if test="${flash.errorMessage}">
        <div class="alert alert-danger" style="display: block">${flash.errorMessage}</div>
    </g:if>

    <table class="table">
        <tr>
            <td>
                <button id="btnImportFromLocalInbox" class="btn btn-default">Import images from local incoming directory</button>
            </td>
            <td>
                Imports image files from the designated incoming server directory ("${grailsApplication.config.getProperty('imageservice.imagestore.inbox')}")
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnRegenArtifacts" class="btn btn-default">Regenerate Image Artifacts</button>
            </td>
            <td>
                Regenerate all tiles and thumbnails for all images in the repository. Progress can be tracked on the dashboard.
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnRegenThumbnails" class="btn btn-default">Regenerate Image Thumbnails</button>
            </td>
            <td>
                Regenerate just thumbnails for all images in the repository. Progress can be tracked on the dashboard.
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnRebuildKeywords" class="btn btn-default">Rebuild Keywords</button>
            </td>
            <td>
                Rebuild the synthetic keywords based on image tags (used for fast searching)
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnDeleteIndex" class="btn btn-danger">Re-initialise Index</button>
            </td>
            <td>
                Delete and reinitialize the index (creates an empty index)
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnReindexAllImages" class="btn btn-default">Reindex All Images</button>
            </td>
            <td>
                Rebuild the full text index used for searching for images - this will take several minutes for
                1 million + images
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnRematchLicencesAllImages" class="btn btn-default">Rematch licences for all images</button>
            </td>
            <td>
                Rematch licences for images - <b>note:</b>
                rematching licences only affects the database. A full re-index is required
                to pick up the changes in the search interface (i.e. facets)
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnClearQueues" class="btn btn-default">Clear processing queues</button>
            </td>
            <td>
                Clear processing queues (tiling, background queues) - this will stop tiling, thumbnail generation
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnSearchIndex" class="btn btn-default">Search image index</button>
            </td>
            <td>
                Find image by the elastic search index (Advanced)
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnClearCollectoryCache" class="btn btn-default">Clear collectory cache</button>
            </td>
            <td>
                Clear the cache of collectory metadata for data resources (rights, license etc)
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnClearHibernateCache" class="btn btn-default">Clear hibernate query cache</button>
            </td>
            <td>
                Clear the hibernate query cache of cached data
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnMissingImagesCheck" class="btn btn-default">Missing images check</button>
            </td>
            <td>
                Missing images report - generates a CSV file of image IDs for images with missing artefacts
            </td>
        </tr>
        <tr>
            <td>
                <button id="btnPurgeDeletedImages" class="btn btn-default">Purge deleted images</button>
            </td>
            <td>
                This will run a background task that will remove deleted images from the filesystem and the database.
            </td>
        </tr>
    </table>
<script>

    $(document).ready(function() {

        $("#btnMissingImagesCheck").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'checkForMissingImages')}";
        });

        $("#btnRegenArtifacts").on('click', function(e) {
            e.preventDefault();
            $.ajax("${createLink(controller:'webService', action:'scheduleArtifactGeneration')}").done(function() {
                window.location = "${createLink(action:'tools')}";
            });
        });

        $("#btnRegenThumbnails").on('click', function(e) {
            e.preventDefault();
            $.ajax("${createLink(controller:'webService', action:'scheduleThumbnailGeneration')}").done(function() {
                window.location = "${createLink(action:'tools')}";
            });
        });

        $("#btnRebuildKeywords").on('click', function(e) {
            e.preventDefault();
            $.ajax("${createLink(controller:'webService', action:'scheduleKeywordRegeneration')}").done(function() {
                window.location = "${createLink(action:'tools')}";
            });
        });

        $("#btnRematchLicencesAllImages").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'rematchLicenses')}";
        });

        $("#btnClearQueues").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'clearQueues')}";
        });

        $("#btnImportFromLocalInbox").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'localIngest')}";
        });

        $("#btnDeleteIndex").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'reinitialiseImageIndex')}";
        });


        $("#btnReindexAllImages").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'reindexImages')}";
        });

        $("#btnSearchIndex").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'indexSearch')}";
        });

        $("#btnClearCollectoryCache").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'clearCollectoryCache')}";
        });

        $("#btnClearHibernateCache").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'clearHibernateCache')}";
        });

        $("#btnPurgeDeletedImages").on('click', function(e) {
            e.preventDefault();
            window.location = "${createLink(action:'scheduleDeletedImagesPurge')}";
        });
    });

</script>
</body>

</html>
