<%@ page import="au.org.ala.images.License" %>
<!doctype html>
<html>
    <head>
        <meta name="layout" content="adminLayout"/>
        <meta name="section" content="home"/>
        <title>Upload images</title>
        <meta name="breadcrumbs" content="${g.createLink( controller: 'image', action: 'list')}, Images"/>
        <asset:stylesheet src="images-client.css" />
        <asset:javascript src="images-client.js" />
    </head>
    <body>
        <content tag="pageTitle">Upload images</content>
        <div>
            <g:if test="${flash.message}">
                <div class="alert alert-success" style="display: block">
                    <g:if test="${params.newImageId}">
                        New image created - click to view
                        <g:link controller="image" action="details" params="[imageId:params.newImageId]">
                        ${params.newImageId}
                        </g:link>
                    </g:if>
                    <g:else>
                        ${flash.message}
                    </g:else>
                </div>
            </g:if>
            <g:if test="${flash.errorMessage}">
                <div class="alert alert-danger" style="display: block">${flash.errorMessage}</div>
            </g:if>

            <div class="row">
                <div class="well">
                    <h1>Bulk Upload with CSV file</h1>
                    <g:uploadForm class="form-horizontal" name="csvFileUploadForm" >
                        <p>
                            The file must contain column headings, and must have at least one column called <code>imageUrl</code> which contains a url to an image. Data in other columns will be stored as metadata against the image.
                        </p>
                        <div class="form-group" style="margin-left:0px;">
                            <label for="imagefile">CSV file upload</label>
                            <input type="file"  class="form-control-file" name="csvfile" id="imagefile"/>
                        </div>

                        <div id="resultsDiv" style="display: none"></div>

                        <div class="control-group">
                            <div class="controls">
                                <button type="button" class="btn" id="btnCancelCSVFileUpload">Cancel</button>
                                <button type="button" class="btn btn-primary" id="btnUploadCSVImagesFile">Upload</button>
                            </div>
                        </div>
                        <script type="text/javascript">

                            var progressIntervalId = 0;

                            function updateProgress(batchId) {
                                try {
                                    $.ajax("${createLink(action:'getBatchProgress')}?batchId=" + batchId).done(function (data) {
                                        if (data.success) {
                                            if (data.taskCount > 0 && data.taskCount == data.tasksCompleted) {
                                                clearInterval(progressIntervalId);
                                                $("#resultsDiv").html("Upload complete.");
                                            } else {
                                                $("#resultsDiv").html("Uploaded " + data.tasksCompleted + " of " + data.taskCount);
                                            }
                                        } else {
                                            clearInterval(progressIntervalId);
                                        }
                                    });
                                } catch (e) {
                                    $("#resultsDiv").html("Error! " + e);
                                    clearInterval(progressIntervalId);
                                }
                            }

                            function renderProgress(batchId) {
                                progressIntervalId = setInterval(function() {
                                    updateProgress(batchId);
                                }, 1000);
                            }

                            $("#btnCancelCSVFileUpload").on('click', function(e) {
                                e.preventDefault();
                                imglib.hideModal();
                            });

                            $("#btnUploadCSVImagesFile").on('click', function(e) {
                                e.preventDefault();

                                var formData = new FormData($("#csvFileUploadForm").get(0));

                                if (formData) {
                                    $.ajax({
                                        url: "${createLink(action:'uploadImagesFromCSVFile', controller: 'admin')}",
                                        data: formData,
                                        processData: false,
                                        contentType: false,
                                        type: 'POST'
                                    }).done(function(result) {
                                        if (!result.success) {
                                            $("#resultsDiv").html('<div class="alert alert-error">' + result.message + '</div>').css("display", "block");
                                        } else {
                                            $("#resultsDiv").html('<div class="alert alert-success">' + result.message + '</div>').css("display", "block");
                                            renderProgress(result.batchId);
                                        }
                                    });
                                }
                            });

                        </script>
                    </g:uploadForm>
                </div>
            </div>

            <div class="row">
                    <div class="well">
                        <h1>Single Image Upload</h1>
                        <g:form action="storeImage" controller="admin" method="post" enctype="multipart/form-data">
                            <div class="form-group">
                                <label for="dataResourceUid">Data resource UID e.g. dr376 (retrieve from collectory)</label>
                                <input type="text" class="form-control" id="dataResourceUid" name="dataResourceUid">
                            </div>
                            <div class="form-group">
                                <label for="title">Title</label>
                                <input type="text" class="form-control" id="title" name="title">
                            </div>
                            <div class="form-group">
                                <label for="creator">Creator</label>
                                <input type="text" class="form-control" id="creator" name="creator">
                            </div>
                            <div class="form-group">
                                <label for="description">Description</label>
                                <input type="text" class="form-control" id="description" name="description">
                            </div>
                            <div class="form-group">
                                <label for="rights">Rights</label>
                                <input type="text" class="form-control" id="rights" name="rights">
                            </div>
                            <div class="form-group">
                                <label for="rightsHolder">Rights holder</label>
                                <input type="text" class="form-control" id="rightsHolder" name="rightsHolder">
                            </div>
                            <div class="form-group">
                                <label for="license">Licence</label>
                                <g:select name="license"
                                          class="form-control"
                                          from="${au.org.ala.images.License.list()}"
                                          value=""
                                          optionKey="url"
                                          optionValue="name"
                                />
                            </div>
                            <div class="form-group">
                                <input type="file" name="image"  />
                            </div>
                            <g:submitButton class="btn btn-primary" name="Upload"/>
                        </g:form>
                    </div>
            </div>
            <script>
                $(document).ready(function() {
                    $("#btnUploadFromCSV").on('click', function(e) {
                        var opts = {
                            url:"${createLink(controller:'imageClient', action:'uploadFromCSVFragment')}",
                            title:"Upload images from CSV"
                        };
                        imgvwr.showModal(opts);
                    });
                });
            </script>
        </div>

        <div id="uploadFromCSVModal" class="modal fade" role="dialog">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                        <h4 class="modal-title">Add Search Criteria</h4>
                    </div>
                    <div class="modal-body">
                        <form id="criteriaForm">
                            <div class="control-group">
                                <label class="control-label" for='searchCriteriaDefinitionId'>Criteria:</label>
                                <g:select class="form-control" id="cmbCriteria" name="searchCriteriaDefinitionId" from="${criteriaDefinitions}"
                                          optionValue="name" optionKey="id" noSelection="${[0:"<Select Criteria>"]}" />
                            </div>
                            <div id="criteriaDetail" style="margin-top:10px;">

                            </div>
                        </form>
                    </div>
                    <div class="modal-footer">
                        <button id="btnSaveCriteria" type="button" class="btn btn-small btn-primary pull-right">Add criteria</button>
                        <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>

    </body>
</html>

