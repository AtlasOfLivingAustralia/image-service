<%@ page import="au.org.ala.images.StagingColumnType" %>
<!doctype html>
<html>
    <head>
        <meta name="layout" content="${grailsApplication.config.getProperty('skin.layout')}"/>
        <title><g:message code="staged.images.image.service.title" /> | ${grailsApplication.config.getProperty('skin.orgNameLong')}</title>
        <style>
        .numberCircle {
            border-radius: 50%;
            display: inline-block;
            width: 19px;
            height: 19px;
            padding: 6px;
            background: #fff;
            border: 2px solid #666;
            color: #666;
            text-align: center;
        }
        </style>
        <r:require modules="qtip,image-viewer"/>
    </head>

    <body class="content">

        <img:headerContent title="Image upload">
        </img:headerContent>

        <div class="well well-small">
            <div class="row-fluid">
                <div class="span3">
                    <h4><span class="numberCircle">1</span>&nbsp;<g:message code="staged.images.upload.images" /></h4>
                    <p>
                        <g:message code="staged.images.upload.your.images" />
                    </p>
                </div>
                <div class="span3">
                    <h4><span class="numberCircle">2</span>&nbsp;<g:message code="staged.images.upload.data.file" /></h4>
                    <p>
                        <g:message code="staged.images.upload.a.csv.file" />
                    </p>
                </div>
                <div class="span3">
                    <h4><span class="numberCircle">3</span>&nbsp;<g:message code="staged.images.configure.columns" /></h4>
                <p>
                    <g:message code="staged.images.add.and.configure.columns" />
                    <img:helpText>
                        <p><g:message code="staged.images.field.values.can.be.derived.from.the.image" /></p>
                        <p><g:message code="staged.images.only.data.displayed.will.be.loaded" /></p>
                    </img:helpText>
                </p>
                </div>
                <div class="span3">
                    <h4><span class="numberCircle">4</span>&nbsp;<g:message code="staged.images.upload.images.subtitle" /></h4>
                    <g:message code="staged.images.review.the.staged.images" />
                </div>
            </div>
            <div class="row-fluid">
                <div class="span3" style="text-align: center">
                    <button id="btnSelectImages" class="btn"><g:message code="staged.images.select.files" /></button>
                </div>
                <div class="span3" style="text-align: center">
                    <g:if test="${hasDataFile}">
                        <button class="btn btn-warning" id="btnClearDataFile"><g:message code="staged.images.clear.data.file" /></button>
                        <a href="${dataFileUrl}"><g:message code="staged.images.view.data.file" /></a>
                    </g:if>
                    <g:else>
                        <button class="btn" id="btnUploadDataFile"><i class="icon-upload"></i>&nbsp;<g:message code="staged.images.staged.images.upload.data.file" /></button>
                    </g:else>
                </div>
                <div class="span3" style="text-align: center">
                    <button class="btnAddFieldDefinition btn"><i class="icon-plus"></i> <g:message code="staged.images.add.column" /></button>
                </div>
                <div class="span3" style="text-align: center">
                    <div style="margin-bottom: 10px">
                        <g:checkBox name="areOccurrences" id="areOccurrences" checked="${true}"/> <g:message code="staged.images.treat.as.occurrence.records" />
                        <img:helpText>
                            <g:message code="staged.images.if.checked.these.images.will.be.the.basis.of.an.occurrence" />
                            <p/>
                            <g:message code="staged.images.metadata.attached.to.the.image" />
                        </img:helpText>
                    </div>

                    <button id="btnUploadStagedImages" class="btn btn-primary" style="margin-left: 10px"><g:message code="staged.images.upload.staged.images" /></button>
                </div>
            </div>
        </div>

        <div class="staged-files-list" style="overflow-x: scroll">
            <table class="table table-condensed table-bordered table-striped">
                <thead>
                    <th><g:message code="staged.images.filename" /></th>
                    <th><g:message code="staged.images.date.staged" /></th>
                    <g:each in="${dataFileColumns}" var="field">
                        <th columnDefinitionId="${field.id}">
                            <div class="label" style="display: block">
                                <g:set var="fieldTypeIcon" value="${[ (StagingColumnType.Literal) : "icon-font", (StagingColumnType.NameRegex): 'icon-asterisk'][field.fieldDefinitionType] ?: 'icon-file'}" />
                                <i class="icon-white ${fieldTypeIcon}"></i>
                                <span style="text-align: right">
                                <a href="#" class="btnEditField" title="Edit column definition"><i class="icon-edit icon-white"></i></a>
                                <a href="#" class="btnDeleteField" title="Remove column"><i class="icon-remove icon-white"></i></a>
                                </span>
                                <br/>
                                <div style="font-weight: normal">${field.format}</div>
                                ${field.fieldName}
                            </div>
                        </th>
                    </g:each>
                    <th style="width: 40px"></th>
                </thead>
                <tbody>
                    <g:each in="${stagedFiles}" var="stagedFile">
                        <tr stagedFileId="${stagedFile.id}">
                            <td>
                                <a href="${stagedFile.stagedFileUrl}">${stagedFile.filename}</a>
                            </td>
                            <td>
                                <img:formatDateTime date="${stagedFile.dateStaged}" />
                            </td>
                            <g:each in="${dataFileColumns}" var="field">
                                <td>
                                    ${stagedFile[field.fieldName]}
                                </td>
                            </g:each>
                            <td>
                                <button type="button" class="btn btn-small btn-danger btnDeleteStagedFile"><i class="icon-remove icon-white"></i></button>
                            </td>
                        </tr>
                    </g:each>
                </tbody>
            </table>
        </div>

        <r:script>

            $(document).ready(function () {

                imgvwr.bindTooltips("a.fieldHelp", 650);

                $(".btnDeleteStagedFile").on('click', function (e) {
                    e.preventDefault();
                    var stagedFileId = $(this).closest("[stagedFileId]").attr("stagedFileId");
                    if (stagedFileId) {
                        window.location = "${createLink(action:'deleteStagedImage')}?stagedImageId=" + stagedFileId;
                    }
                });

                $("#btnSelectImages").on('click', function(e) {
                    e.preventDefault();
                    var opts = {
                        title:<g:message code="staged.images.upload.images.to.the.staging.area" />,
                        url: "${createLink(controller: 'dialog', action:"selectImagesForStagingFragment", params:[userId: userId])}"
                    };

                    imgvwr.showModal(opts);
                });

                $("#btnUploadDataFile").on('click', function(e) {
                    e.preventDefault();
                    var options = {
                        title: <g:message code="staged.images.upload.a.data.file" />,
                        url: "${createLink(controller:'dialog', action:'uploadStagedImagesDataFileFragment', params:[userId: userId])}"
                    };
                    imgvwr.showModal(options);
                });

                $("#btnClearDataFile").on('click', function(e) {
                    e.preventDefault();
                    window.location = "${createLink(controller:'image', action:'clearStagingDataFile')}";
                });

                $(".btnAddFieldDefinition").on('click', function(e) {
                    e.preventDefault();
                    var options = {
                        title: <g:message code="staged.images.add.column.definition" />,
                        url:"${createLink(controller: 'image', action:'editStagingColumnFragment')}"
                    };
                    imgvwr.showModal(options);
                });

                $(".btnDeleteField").on('click', function(e) {
                    e.preventDefault();
                    var fieldId = $(this).parents("[columnDefinitionId]").attr("columnDefinitionId");
                    if (fieldId) {
                        window.location = "${createLink(controller:'image', action:'deleteStagingColumnDefinition')}?columnDefinitionId=" + fieldId;
                    }
                });

                $(".btnEditField").on('click', function(e) {
                    e.preventDefault();
                    var fieldId = $(this).parents("[columnDefinitionId]").attr("columnDefinitionId");
                    if (fieldId) {
                        var options = {
                            title: "Edit field definition",
                            url:"${createLink(action: 'editStagingColumnFragment')}?columnDefinitionId=" + fieldId
                        };
                        imgvwr.showModal(options);
                    }
                });

                $("#btnUploadStagedImages").on('click', function(e) {
                    e.preventDefault();
                    var areOccurrences = $("#areOccurrences").is(':checked');
                    window.location.href = "${createLink(controller:'image', action:'uploadStagedImages')}?harvestable=" + areOccurrences;
                });

            });

        </r:script>

    </body>
</html>
