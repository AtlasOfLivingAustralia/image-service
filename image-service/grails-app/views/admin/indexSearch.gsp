<!doctype html>
<html>
    <head>
        <meta name="layout" content="adminLayout"/>
        <title>ALA Images - Admin - Tools -> Index Search</title>
        <style type="text/css" media="screen">
            #query {
                font-family: "Courier New", Courier, monospace;
            }
        </style>
    </head>

    <body>
        <div class="content">
            <content tag="pageTitle">Tools - Index Search</content>
            <content tag="adminButtonBar" />

            <div class="form-horizontal" action="indexSearch">
                <div class="control-group">
                    <div class="controls">
                        <g:textArea name="q" class="input-xxlarge" rows="10" value="${query}">${query}</g:textArea>
                    </div>
                </div>
                <div class="control-group">
                    <div class="controls">
                        <button type="button" id="btnDoSearch" class="btn btn-primary">Search</button>
                    </div>
                </div>
            </div>

            <g:if test="${results}" >
                <g:render template="../search/imageThumbnails" model="${[images: results.list, totalImageCount: results.totalCount, allowSelection: true, thumbsTitle:"Search Results' (${results.totalCount} images)"]}" />
            </g:if>
        </div>
    </body>
    <script>

    $(document).ready(function() {

        $("#btnDoSearch").on('click', function(e) {
            e.preventDefault();
            window.location.href = "${createLink(action:'indexSearch')}?q=" + encodeURIComponent($("#q").val());
        });

    });

    </script>
</html>
