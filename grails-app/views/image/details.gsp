<%@ page import="au.org.ala.web.CASRoles" %>
<g:set var="mediaTitle" value="${isImage ? 'Image' : 'Media'}" />
<!doctype html>
<html>
    <head>
        <meta name="layout" content="${grailsApplication.config.getProperty('skin.layout')}"/>
        <title>${mediaTitle} - ${imageInstance.title ? imageInstance.title : imageInstance.imageIdentifier}</title>
        <meta name="breadcrumbs" content="${g.createLink( controller: 'search', action: 'list')}, Images"/>
        <asset:stylesheet src="application.css" />
        <asset:stylesheet src="ala/images-client.css" />
        <style>
            td.property-value { font-weight: bold !important; }
            .audiojs { width: 100%; }
            .audiojs .play-pause {
                padding-left:0px;
                padding-right:5px;
            }
            .tab-pane { padding-top: 20px !important; }
            .tabbable { font-size: 9pt; margin-top:10px; }
            div#main { padding-top: 0px; }
            .subimages_thumbs { max-height:100px; }
        </style>
    </head>
    <body>
        <img:headerContent title="${mediaTitle} details ${imageInstance?.id}">
            <% pageScope.crumbs = []  %>
        </img:headerContent>
        <div class="container-fluid" style="padding-left:1px; padding-top:0px;">
            <div class="row">
                <div id="viewerContainerId" class="col-md-9">
                    <g:if test="${!imageInstance.mimeType.startsWith('image')}">
                    <div class="col-md-3"></div>
                    <div class="col-md-6">
                        <div class="document-icon" style="height: 500px; margin-bottom: 30px;"></div>
                        <g:if test="${imageInstance.mimeType.startsWith('audio')}">
                            <audio src="${createLink(controller: 'image', action:'proxyImage', params: [imageId: imageInstance.imageIdentifier])}" preload="auto" />
                        </g:if>
                    </div>
                    <div class="col-md-3"></div>
                    </g:if>
                </div>
                <div id="imageTabs" class="col-md-3">
                    <div class="tabbable" >
                        <ul class="nav nav-tabs">
                            <li class="active">
                                <a href="#tabProperties" data-toggle="tab">${mediaTitle}</a>
                            </li>
                            <li>
                                <a href="#tabExif" data-toggle="tab"><g:message code="details.embedded" /></a>
                            </li>
                            <li>
                                <a href="#tabSystem" data-toggle="tab"><g:message code="details.system" /></a>
                            </li>
                            <g:if test="${isImage}">
                                <li>
                                    <a href="#tabThumbnails" data-toggle="tab"><g:message code="details.thumbnails" /></a>
                                </li>
                            </g:if>
                            <auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN}">
                                <li>
                                    <a href="#tabAuditMessages" data-toggle="tab"><g:message code="details.audit" /></a>
                                </li>
                            </auth:ifAnyGranted>
                        </ul>

                        <div class="tab-content">

                            <div class="tab-pane active" id="tabProperties">
                                <div class="coreMetadataContainer">
                                    <g:render template="/image/coreImageMetadataFragment" />
                                </div>
                                <div id="tagsSection"></div>
                            </div>
                            <div class="tab-pane" id="tabExif" metadataSource="${au.org.ala.images.MetaDataSourceType.Embedded}">
                                <div class="metadataSource-container"></div>
                            </div>
                            <div class="tab-pane" id="tabSystem" metadataSource="${au.org.ala.images.MetaDataSourceType.SystemDefined}" >
                                <table class="table table-bordered table-condensed table-striped">
                                    <tr>
                                        <td class="property-name"><g:message code="details.data.resource.uid" /></td>
                                        <td class="property-value">${imageInstance.dataResourceUid}</td>
                                    </tr>
                                    <g:if test="${isImage}">
                                        <tr>
                                            <td class="property-name"><g:message code="details.dimensions.w.x.h" /></td>
                                            <td class="property-value">${imageInstance.width} x ${imageInstance.height}</td>
                                        </tr>
                                    </g:if>
                                    <tr>
                                        <td class="property-name"><g:message code="details.file.size" /></td>
                                        <td class="property-value"><img:sizeInBytes size="${imageInstance.fileSize}" /></td>
                                    </tr>
                                    <tr>
                                        <td class="property-name"><g:message code="details.mime.type" /></td>
                                        <td class="property-value">${imageInstance.mimeType}</td>
                                    </tr>
                                    <tr>
                                        <td class="property-name"><g:message code="details.dc.type" /></td>
                                        <td class="property-value">${imageInstance.type}</td>
                                    </tr>
                                    <tr>
                                        <td class="property-name"><g:message code="details..url" args="[mediaTitle]" /></td>
                                        <td class="property-value">
                                            <a href="${img.imageUrl([imageId: imageInstance.imageIdentifier])}">
                                                <img:imageUrl imageId="${imageInstance.imageIdentifier}" />
                                            </a>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td class="property-name"><g:message code="details.md5.hash" /></td>
                                        <td class="property-value">${imageInstance.contentMD5Hash}</td>
                                    </tr>
                                    <tr>
                                        <td class="property-name"><g:message code="details.sha1.hash" /></td>
                                        <td class="property-value">${imageInstance.contentSHA1Hash}</td>
                                    </tr>
                                    <tr>
                                        <td class="property-name"><g:message code="details.size.on.disk" /></td>
                                        <td class="property-value"><img:sizeInBytes size="${sizeOnDisk}" /></td>
                                    </tr>
                                    <auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN}">
                                    <tr>
                                        <td class="property-name">Storage location</td>
                                        <td class="property-value">${imageInstance.storageLocation}</td>
                                    </tr>
                                    </auth:ifAnyGranted>
                                </table>

                                <div class="metadataSource-container"></div>
                            </div>
                            <div class="tab-pane" id="tabThumbnails">
                                <ul class="list-unstyled list-inline">
                                    <g:each in="${squareThumbs}" var="thumbUrl">
                                        <li>
                                            <a href="${thumbUrl}" target="thumbnail">
                                                <img class="thumbnail" src="${thumbUrl}" style="width:100px;" title="${thumbUrl}" style="margin: 5px"/>
                                            </a>
                                        </li>
                                    </g:each>
                                </ul>
                            </div>
                            <auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN}">
                                <div class="tab-pane" id="tabAuditMessages">
                                    <div class="metadataSource-container"></div>
                                </div>
                            </auth:ifAnyGranted>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <asset:javascript src="ala/images-client.js"/>
        <asset:javascript src="audio.min.js"/>

    <script>

        function refreshMetadata(tabDiv) {
            var dest = $(tabDiv);
            dest.find('.metadataSource-container').html("Loading...");
            var source = dest.attr("metadataSource");
            $.ajax("${createLink(absolute: true, controller:'image', action:'imageMetadataTableFragment', id: imageInstance.id)}?source=" + source).done(function(content) {
                dest.find('.metadataSource-container').html(content);
            });
        }

        function refreshCoreMetadata() {
            $.ajax("${createLink(absolute: true, controller:'image', action:'coreImageMetadataTableFragment', id: imageInstance.id)}").done(function(content) {
                $('#imageTabs').find('.coreMetadataContainer').html(content);
            });
        }

        <auth:ifAnyGranted roles="${CASRoles.ROLE_ADMIN}">

        function refreshAuditTrail() {
            $.ajax("${createLink(absolute: true, controller: 'image', action:'imageAuditTrailFragment', id: imageInstance.id)}").done(function(content) {
                $("#tabAuditMessages").html(content);
            });
        }

        </auth:ifAnyGranted>

        $(document).ready(function() {

            var options = {
                auxDataUrl : "${auxDataUrl ? auxDataUrl : ''}",
                imageServiceBaseUrl : "${createLink(absolute: true, uri: '/')}",
                imageClientBaseUrl : "${createLink(absolute: true, uri: '/')}",
                zoomFudgeFactor: 0.65
            };
            imgvwr.setImageClientBaseUrl(options.imageClientBaseUrl);
            imgvwr.setImageServiceBaseUrl(options.imageServiceBaseUrl);

            var screenHeight = $(window).height();
            $('#viewerContainerId').css('height', (screenHeight - 172) + 'px');


            <g:if test="${imageInstance.mimeType.startsWith('image')}">
                imgvwr.viewImage($("#viewerContainerId"), '${imageInstance.imageIdentifier}', "", "", options);
            </g:if>
            <g:elseif test="${imageInstance.mimeType.startsWith('audio')}">
            $('#viewerContainerId .document-icon').css('background-image', 'url("<img:imageThumbUrl imageId='${imageInstance.imageIdentifier}'/>")');
            $('#viewerContainerId .document-icon').css('background-repeat', 'no-repeat');
            $('#viewerContainerId').css('background-position', 'center');
            audiojs.createAll({});
            </g:elseif>
            <g:else>
            $('#viewerContainerId .document-icon').css('background-image', 'url("<img:imageThumbUrl imageId='${imageInstance.imageIdentifier}'/>")');
            $('#viewerContainerId .document-icon').css('background-repeat', 'no-repeat');
            $('#viewerContainerId .document-icon').css('background-position', 'center');
            </g:else>

            $('a[data-toggle="tab"]').on('click', function (e) {

                var dest = $($(this).attr("href"));
                if (dest.attr("metadataSource")) {
                    refreshMetadata(dest);
                } else {
                    if (dest.attr("id") == "tabAuditMessages") {
                        refreshAuditTrail();
                    }
                }
            });

            $("#btnViewImage").on('click', function(e) {
                e.preventDefault();
                window.location = "${createLink(absolute: true, controller:'image', action:'view', id: imageInstance.imageIdentifier)}";
            });

            $("#btnRegen").on('click', function(e) {
                e.preventDefault();

                $.ajax("${createLink(absolute: true, controller:'image', action:'scheduleArtifactGeneration', id: imageInstance.imageIdentifier)}").done(function(data) {
                    console.log(data);
                    alert(<g:message code="details.regeneration.scheduled" /> + data.message);
                }).fail(function(){
                    alert(<g:message code="details.problem.regeneration" />);
                });
            });

            $("#btnDeleteImage").on('click', function(e) {
                e.preventDefault();
                var options = {
                    message: <g:message code="details.warning.delete.this.image" />,
                    title: <g:message code="details.delete.this.image" />,
                    affirmativeAction: function() {
                        $.ajax("${createLink(absolute: true, controller:'image', action:'deleteImage', id: imageInstance.imageIdentifier)}").done(function() {
                            window.location = "${createLink(absolute: true, controller:'search', action:'list')}";
                        });
                    }
                };
                imgvwr.areYouSure(options);
            });

            $(".image-info-button").each(function() {
                var imageId = $(this).closest("[imageId]").attr("imageId");
                if (imageId) {
                    $(this).qtip({
                        content: {
                            text: function(event, api) {
                                $.ajax("${createLink(absolute: true, controller:'image', action:"imageTooltipFragment")}/" + imageId).then(function(content) {
                                    api.set("content.text", content);
                                },
                                function(xhr, status, error) {
                                    api.set("content.text", status + ": " + error);
                                });
                            }
                        }
                    });
                }
            });

            loadTags();
        });

        function loadTags() {
            $.ajax("${createLink(absolute: true, controller: 'image', action:'tagsFragment', id:imageInstance.id)}").done(function(html) {
                $("#tagsSection").html(html);
            });
        }

        function calibrationCallback(data){
            refreshCoreMetadata();
        }

        function createSubImageCallback(){
            refreshCoreMetadata();
        }
    </script>
    </body>
</html>

