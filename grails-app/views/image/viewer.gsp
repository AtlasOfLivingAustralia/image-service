<%@ page contentType="text/html;charset=UTF-8" %>
<!DOCTYPE html>
<html>
    <head>
        <title><g:message code="viewer.image.title" args="[img.maskUrlCredentials(value: imageInstance.originalFilename)]" /> | <g:message code="viewer.image.service.title" /> | ${grailsApplication.config.getProperty('skin.orgNameLong')}</title>
        <style>
        html, body {
            height:100%;
            padding: 0;
            margin:0;
        }
        #imageViewerContainer {
            height: 100%;
            padding: 0;
        }
        #imageViewer {
            width: 100%;
            height: 100%;
            margin: 0;
        }
        </style>
        <link rel="stylesheet" href="/assets/font-awesome-4.7.0/css/font-awesome.css?compile=false" />
        <asset:stylesheet src="ala/images-client.css" />
    </head>
    <body style="padding:0;">
        <div class="search-hero" style="padding:20px 0; margin-bottom:0;">
            <div class="hero-inner container">
                <h1 class="hero-title"><g:message code="viewer.image.title" args="[img.maskUrlCredentials(value: imageInstance.originalFilename)]" /></h1>
                <div class="hero-actions">
                    <a class="btn btn-default" href="${createLink(controller:'search', action:'list')}">
                        <span class="glyphicon glyphicon-arrow-left"></span>
                        <g:message code="list.back.to.search" default="Back to search" />
                    </a>
                </div>
            </div>
        </div>
        <div id="imageViewerContainer" class="container-fluid">
            <div id="imageViewer"> </div>
        </div>
%{--        <asset:javascript src="head.js"/>--}%
        <script type="text/javascript"
                src="${grailsApplication.config.getProperty('headerAndFooter.baseURL')}/js/jquery.min.js"></script>
        <script type="text/javascript"
                src="${grailsApplication.config.getProperty('headerAndFooter.baseURL')}/js/jquery-migration.min.js"></script>
        <script type="text/javascript"
                src="${grailsApplication.config.getProperty('headerAndFooter.baseURL')}/js/autocomplete.min.js"></script>

%{--        <script type="text/javascript" src="${grailsApplication.config.getProperty('headerAndFooter.baseURL')}/js/application.js"--}%
%{--                defer></script>--}%
        <script type="text/javascript"
                src="${grailsApplication.config.getProperty('headerAndFooter.baseURL')}/js/bootstrap.min.js"></script>
        <asset:javascript src="ala/images-client.js"/>
        <script>
            $(document).ready(function() {
                var options = {
                    auxDataUrl : "${auxDataUrl ? auxDataUrl : ''}",
                    imageServiceBaseUrl : "${createLink(absolute: true, uri: '/')}",
                    imageClientBaseUrl : "${createLink(absolute: true, uri: '/')}"
                };
                imgvwr.viewImage($("#imageViewer"), "${imageInstance.imageIdentifier}", "", "", options);
            });
        </script>
    </body>
</html>
