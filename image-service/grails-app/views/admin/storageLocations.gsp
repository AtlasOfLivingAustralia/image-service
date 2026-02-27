<%@ page import="org.javaswift.joss.client.factory.AuthenticationMethod" %>
<!doctype html>
<html>
    <head>
        <meta name="layout" content="adminLayout"/>
        <meta name="section" content="home"/>
        <title>ALA Images - Admin - Storage Locations</title>
    </head>
    <body>

        <content tag="pageTitle">Storage Locations</content>
        <content tag="adminButtonBar" />

        <div class="row">
            <div class="col-md-12">
                <button class="btn btn-success" id="btn-add-storage-location"><i class="glyphicon glyphicon-plus "> </i>&nbsp;Add</button>
            </div>
        </div>

        <div class="row" style="margin-top:10px;">
            <div class="col-md-12">
                <div id="storage-location-container" class="well well-small">
                    <asset:image src="spinner.gif" />
                </div>
            </div>
        </div>

        <div id="storage-location-modal" class="modal fade" role="dialog">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                        <h4 class="modal-title">Storage Location</h4>
                    </div>
                    <div class="modal-body">
                        <form id="storage-location-form">
                            <div class="form-group">
                                <label class="radio-inline">
                                    <input type="radio" name="type" id="fs-type" value="fs" checked>
                                    File system
                                </label>
                                <label class="radio-inline">
                                    <input type="radio" name="type" id="s3-type" value="s3">
                                    AWS S3 Bucket
                                </label>
                                <label class="radio-inline">
                                    <input type="radio" name="type" id="swift-type" value="swift">
                                    Swift
                                </label>
                            </div>
                            <div id="fs-form" class="type-form">
                                <div class="form-group">
                                    <label for="basePath">Base Path</label>
                                    <input type="text" class="form-control" id="basePath" name="basePath" placeholder="/data/images/storage">
                                </div>
                            </div>
                            <div id="s3-form" class="type-form hidden">
                                <div class="form-group">
                                    <label for="region">Region</label>
                                    <input type="text" class="form-control" id="region" name="region" placeholder="ap-southeast-2">
                                </div>
                                <div class="form-group">
                                    <label for="bucket">Bucket name</label>
                                    <input type="text" class="form-control" id="bucket" name="bucket" placeholder="ala-image-service">
                                </div>
                                <div class="form-group">
                                    <label for="prefix">Object Prefix</label>
                                    <input type="text" class="form-control" id="prefix" name="prefix" placeholder="/images/prefix">
                                </div>
                                <div class="form-group">
                                    <label for="accessKey">Access Key</label>
                                    <input type="text" class="form-control" id="accessKey" name="accessKey" placeholder="asdfasdfasdf">
                                </div>
                                <div class="form-group">
                                    <label for="secretKey">Secret Key</label>
                                    <input type="text" class="form-control" id="secretKey" name="secretKey" placeholder="asdfasdfasdf">
                                </div>
                                <div class="checkbox">
                                    <label>
                                        <input type="checkbox" id="publicRead" name="publicRead"> Public read
                                    </label>
                                </div>
                                <div class="checkbox">
                                    <label>
                                        <input type="checkbox" id="privateAcl" name="privateAcl"> Explicit Private ACL
                                    </label>
                                    <p class="help-block">
                                        If checked (and Public read is not), uploaded objects get S3 canned "private" ACL.
                                        If both unchecked, no ACL header is sent and bucket defaults apply.
                                    </p>
                                </div>
                                <div class="checkbox">
                                    <label>
                                        <input type="checkbox" id="redirect" name="redirect"> Redirect
                                    </label>
                                </div>
                            </div>
                            <div id="swift-form" class="type-form hidden">
                                <div class="form-group">
                                    <label for="authUrl">Auth URL</label>
                                    <input type="url" class="form-control" id="authUrl" name="authUrl" placeholder="https://example.org/v1/auth">
                                </div>
                                <div class="form-group">
                                    <label for="authenticationMethod">Auth Method</label>
                                    <select class="form-control" id="authenticationMethod" name="authenticationMethod">
                                        <g:each in="${AuthenticationMethod.values()}" var="method">
                                            <option value="${method.name()}">${method}</option>
                                        </g:each>
                                    </select>
                                </div>
                                <div class="form-group">
                                    <label for="username">Username</label>
                                    <input type="text" class="form-control" id="username" name="username" placeholder="test:testing">
                                </div>
                                <div class="form-group">
                                    <label for="password">Password</label>
                                    <input type="text" class="form-control" id="password" name="password" placeholder="tester">
                                </div>
                                <div class="form-group">
                                    <label for="tenantId">Tenant ID</label>
                                    <input type="text" class="form-control" id="tenantId" name="tenantId" placeholder="">
                                </div>
                                <div class="form-group">
                                    <label for="tenantName">Tenant Name</label>
                                    <input type="text" class="form-control" id="tenantName" name="tenantName" placeholder="">
                                </div>
                                <div class="form-group">
                                    <label for="containerName">Container Name</label>
                                    <input type="text" class="form-control" id="containerName" name="containerName" placeholder="images">
                                </div>
                                <div class="checkbox">
                                    <label>
                                        <input type="checkbox" id="publicContainer" name="publicContainer"> Public container
                                    </label>
                                </div>
                                <div class="checkbox">
                                    <label>
                                        <input type="checkbox" id="redirect" name="redirect"> Redirect
                                    </label>
                                </div>
                            </div>
                            <button type="button" id="btn-save-storage-location" class="btn btn-default">Add</button>
                        </form>
                    </div>
                    <div class="modal-footer">
                    </div>
                </div>
            </div>
        </div>
        <div id="update-storage-location-modal" class="modal fade" role="dialog">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                        <h4 class="modal-title">Update Storage Location</h4>
                    </div>
                    <div class="modal-body">
                    </div>
                    <div class="modal-footer">
                    </div>
                </div>
            </div>
        </div>

    <asset:script type="text/javascript" asset-defer="">
        $(document).ready(function() {

            function loadLocations() {
                $('#storage-location-container').load('${createLink(controller: 'storageLocation', action: 'listFragment')}');
            }

            $('#btn-add-storage-location').on('click', function(e) {
                $('#storage-location-modal').modal('show');
            });
            $('#btn-save-storage-location').on('click', function(e) {
                 $.ajax({type: 'POST',
                         url: '${createLink(controller: 'storageLocation', action: 'create')}',
                         data: JSON.stringify($('#storage-location-form').serializeArray().reduce(function(a, x) { a[x.name] = x.value; return a; }, {})),
                         dataType: 'json',
                         contentType: 'application/json'
                }).done(function(data, status, jqXHR) {
                    $('#storage-location-modal').modal('hide');
                    loadLocations();
                }).fail(function(jqXHR, textStatus, errorThrown) {
                    $('#storage-location-modal').modal('hide');
                    console.log(errorThrown);
                    if (jqXHR.status == 409) {
                        alert(jqXHR.responseText)
                    } else {
                        alert("Couldn't save storage location");
                    }
                });
            });

            $('input[name="type"]').on('change', function(e) {
                let type = $("input[name='type']:checked").val();
                $('.type-form').addClass('hidden');
                $('#'+type+'-form').removeClass('hidden');
            });

            $('#storage-location-container').on('click', '.btn-migrate', function(e) {
                var $this = $(this);
                var source = $this.data('source');
                $('#storage-location-migrate-modal').modal('show').on('click', '#btn-migrate-storage', function(e) {
                    var $this = $(this);
                    $this.prop('disabled', true);
                    $.ajax({type: 'POST',
                        url: "${createLink(controller: 'storageLocation', action: 'migrate')}",
                        data: { src: source, dst: $('#destination').val(), deleteSrc: $('#deleteSrc').val() }
                    }).done(function(data, status, jqXHR) {
                        $('#storage-location-migrate-modal').modal('hide');
                    }).fail(function(jqXHR, textStatus, errorThrown) {
                        $('#storage-location-migrate-modal').modal('hide');
                        console.log(errorThrown);
                        alert("Couldn't migrate storage location");
                    }).always(function() {
                        $this.prop('disabled', false);
                    });

                });

            });

            $('#storage-location-container').on('change', 'input[type=radio][name=default]', function(e) {
                $.ajax({type: 'POST',
                        url: "${createLink(controller: 'storageLocation', action: 'setDefault')}",
                        data: { id: this.value }
                }).done(function(data, status, jqXHR) {
                    loadLocations();
                }).fail(function(jqXHR, textStatus, errorThrown) {
                    console.log(errorThrown);
                    alert("Couldn't set default");
                    loadLocations();
                });
            });

            $('#storage-location-container').on('click', '.btn-edit', function(e) {
                var $this = $(this);
                var id = $this.data('id');
                $.ajax("${createLink(controller:'storageLocation', action:'editFragment')}/" + id, { 'dataType': 'html' }).done(function(content) {
                    // $("#update-storage-location-modal .modal-title").html("Rename tag");
                    $("#update-storage-location-modal .modal-body").html(content);
                });
                $('#update-storage-location-modal').modal('show');
            });
            $('#update-storage-location-modal').on('hidden.bs.modal', function(e) {
                $("#update-storage-location-modal .modal-body").html('');
            });
            $('#update-storage-location-modal').on('click', '#btn-update-storage-location', function(e) {
                 $.ajax({type: 'POST',
                         url: '${createLink(controller: 'storageLocation', action: 'update')}' + '/' + $('#update-storage-location-form input[name=id]').val(),
                         data: JSON.stringify($('#update-storage-location-form').serializeArray().reduce(function(a, x) { a[x.name] = x.value; return a; }, {})),
                         dataType: 'json',
                         contentType: 'application/json'
                }).done(function(data, status, jqXHR) {
                    $('#update-storage-location-modal').modal('hide');
                    loadLocations();
                }).fail(function(jqXHR, textStatus, errorThrown) {
                    $('#update-storage-location-modal').modal('hide');
                    console.log(errorThrown);
                    alert("Couldn't save storage location");
                });
            });


            loadLocations();
        });
    </asset:script>

    </body>
</html>



