<!doctype html>
<html>
    <head>
        <meta name="layout" content="adminLayout"/>
        <title>ALA Images - Admin - Tools -> Clear Failed Uploads</title>
        <style type="text/css" media="screen">
            #regexPattern {
                font-family: "Courier New", Courier, monospace;
            }
            .table-failed-uploads {
                margin-top: 20px;
            }
        </style>
    </head>

    <body>
        <div class="content">
            <content tag="pageTitle">Tools - Clear Failed Uploads</content>
            <content tag="adminButtonBar" />

            <g:if test="${flash.message}">
                <div class="alert alert-success" style="display: block">${flash.message}</div>
            </g:if>
            <g:if test="${flash.errorMessage}">
                <div class="alert alert-danger" style="display: block">${flash.errorMessage}</div>
            </g:if>

            <div class="card">
                <p>This tool allows you to delete failed upload entries based on a regular expression pattern that matches the URL.</p>
                <p>Enter a regular expression pattern below and click "Clear Failed Uploads" to delete all matching entries.</p>
                <p><strong>Warning:</strong> This action cannot be undone. Make sure your pattern is correct before proceeding.</p>
            </div>

            <g:form action="clearFailedUploads" method="get" id="clearFailedUploadsForm">
                <input type="hidden" name="preview" value="true"/>
                <div class="mb-3">
                    <label class="col-form-label" for="regexPattern">Regex Pattern:</label>
                    <div class="controls">
                        <g:textField class="form-control" name="regexPattern" value="${params.regexPattern}" placeholder="e.g. .*example\\.com/.*"/>
                    </div>
                </div>
                <div class="mb-3">
                    <label class="col-form-label" for="maxResults">Max Preview Results:</label>
                    <div class="controls">
                        <g:select name="maxResults" 
                                  from="${[10, 50, 100, 500, 1000]}" 
                                  value="${maxResults ?: 100}" 
                                  class="form-select form-select-sm"/>
                    </div>
                </div>
                <div class="mb-3">
                    <div class="controls">
                        <button type="button" id="btnDelete" class="btn btn-danger">Clear Failed Uploads</button>
                        <button type="submit" class="btn btn-primary">Preview</button>
                        <g:link action="tools" class="btn btn-outline-dark">Cancel</g:link>
                    </div>
                </div>
            </g:form>
            
            <div id="previewResults" style="display: ${matchingUploads ? 'block' : 'none'}">
                <g:if test="${matchingUploads}">
                    <div class="row">
                        <div class="col-12">
                            <h3>Preview Results (${totalCount} total, showing ${Math.min(totalCount, maxResults ?: 100)})</h3>
                            <div class="alert alert-info">
                                <p>These are the failed uploads that will be deleted if you proceed with the current regex pattern.</p>
                            </div>
                            <table class="table table-striped table-bordered table-failed-uploads">
                                <thead>
                                    <tr>
                                        <th>#</th>
                                        <th>URL</th>
                                        <th>Date Created</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <g:each in="${matchingUploads}" var="upload" status="i">
                                        <tr>
                                            <td>${i + 1}</td>
                                            <td>${upload.url}</td>
                                            <td><g:formatDate date="${upload.dateCreated}" format="yyyy-MM-dd HH:mm:ss"/></td>
                                        </tr>
                                    </g:each>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </g:if>
            </div>
        </div>
    <script>
        $(document).ready(function() {
            // Handle the delete button click
            $("#btnDelete").on('click', function(e) {
                e.preventDefault();
                var regexPattern = $("#regexPattern").val();
                
                if (!regexPattern) {
                    alert("Please enter a regex pattern");
                    return;
                }
                
                // Get the total count from the preview results if available
                var totalCount = ${totalCount ?: 0};
                var confirmMessage = "Are you sure you want to delete ";
                
                if (totalCount > 0) {
                    confirmMessage += totalCount + " failed upload entries?";
                } else {
                    confirmMessage += "all matching failed upload entries?";
                }
                
                if (confirm(confirmMessage)) {
                    // Create a temporary form to submit to doClearFailedUploads
                    var tempForm = $('<form>', {
                        'action': '${createLink(action: "doClearFailedUploads")}',
                        'method': 'post'
                    });
                    
                    // Add the regex pattern to the form
                    $('<input>').attr({
                        'type': 'hidden',
                        'name': 'regexPattern',
                        'value': regexPattern
                    }).appendTo(tempForm);
                    
                    // Append the form to the body and submit it
                    tempForm.appendTo('body').submit();
                }
            });
            
            // Form validation
            $("#clearFailedUploadsForm").on('submit', function(e) {
                var regexPattern = $("#regexPattern").val();
                if (!regexPattern) {
                    alert("Please enter a regex pattern");
                    e.preventDefault();
                }
            });
        });
    </script>
    </body>
</html>