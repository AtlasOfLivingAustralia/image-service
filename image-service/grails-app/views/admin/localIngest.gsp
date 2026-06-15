<!doctype html>
<html>
    <head>
        <meta name="layout" content="adminLayout"/>
        <title>Local Ingestion</title>
    </head>

    <body>
        <content tag="pageTitle">Tools</content>
        <content tag="adminButtonBar" />
        <div>
            <h2>Image Metadata</h2>

            <div class="text-end">
                <button class="btn btn-primary" id="btnStartImageImport">
                    <i class="fa fa-cog"> </i>
                    Start Import
                </button>
                <div id="startMessage"></div>
            </div>
            <p>Add field definitions here to attach meta data to each image as it is ingested into the image service.</p>
            <button class="btn btn-sm btn-success" id="btnAddField"><i class=" fa fa-plus"></i>&nbsp;Add Field</button>
            <div class="" id="fieldDefinitions" style="margin-top: 5px"></div>
        </div>

        <div>
            <div class="text-end">
                <button class="btn btn-outline-dark" id="btnRefreshFileList">
                    <i class="fa fa-cog"> </i>
                    Refresh file list
                </button>
            </div>
            <h4>File List - Reading local server directory: ${grailsApplication.config.getProperty('imageservice.imagestore.inbox')}</h4>

            <div id="fileList"></div>
        </div>
        <table>
            <tr></tr>
        </table>

        <div id="ingestModal" class="modal fade" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h4 class="modal-title">Ingest</h4>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">

                    </div>
                    <div class="modal-footer">
                    </div>
                </div>
            </div>
        </div>

    <script>
        $(document).ready(function() {

            $("#btnStartImageImport").on('click', function(e) {
                e.preventDefault();
                $.ajax("${createLink(controller:'webService', action:'scheduleInboxPoll', params: [userId:userId])}").done(function(results) {
                    $("#startMessage").html('<div class="alert alert-info">Import started with batch id ' + results.importBatchId + '</div>' );
                });
            });

            $('#btnRefreshFileList').on('click', function(e) {
                renderFileList();
            });

            $("#btnAddField").on('click', function(e) {
                e.preventDefault();
                $.ajax("${createLink(action:'addFieldFragment')}").done(function(content) {
                    $("#ingestModal .modal-title").html("Add field");
                    $("#ingestModal .modal-body").html(content);
                });
                $('#ingestModal').modal('show');
            });

            $('#ingestModal').on('hidden.bs.modal', function () {
                renderFieldDefinitions();
                renderFileList();
            });

            renderFieldDefinitions();
            renderFileList();

        });

        function renderFieldDefinitions() {
            $.ajax("${createLink(action:"fieldDefinitionsFragment")}").done(function(content) {
                $("#fieldDefinitions").html(content);
                // Delete buttons
                $(".btnDeleteFieldDefinition").on('click', function(e) {
                    e.preventDefault();
                    var fieldId = $(this).closest("[fieldDefinitionId]").attr("fieldDefinitionId");
                    if (fieldId) {
                        $.ajax("${createLink(action:'deleteFieldDefinition')}/" + fieldId).done(function(data) {
                            renderFieldDefinitions();
                        });
                    }
                });
                // Edit buttons
                $(".btnEditFieldDefinition").on('click', function(e) {
                    e.preventDefault();
                    var fieldId = $(this).closest("[fieldDefinitionId]").attr("fieldDefinitionId");
                    if (fieldId) {
                        $.ajax("${createLink(action:'editFieldFragment')}/" + fieldId).done(function(content) {
                            $("#ingestModal .modal-title").html("Edit field");
                            $("#ingestModal .modal-body").html(content);
                        });
                        $('#ingestModal').modal('show');
                    }
                });
            });
        }

        function renderFileList() {
            $.ajax("${createLink(action:"inboxFileListFragment")}").done(function(content) {
                $("#fileList").html(content);
            });
        }
    </script>
    </body>

</html>
