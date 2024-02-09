
<!-- HTML for static distribution bundle build -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Image Service API</title>
    <link rel="stylesheet" type="text/css" href="${grailsApplication.config.getProperty('server.contextPath')}/webjars/swagger-ui/3.20.9/swagger-ui.css" >
    <link href="${grailsApplication.config.getProperty('skin.favicon')}" rel="shortcut icon"  type="image/x-icon"/>
    <style>
    html
    {
        box-sizing: border-box;
        overflow: -moz-scrollbars-vertical;
        overflow-y: scroll;
    }

    *,
    *:before,
    *:after
    {
        box-sizing: inherit;
    }

    body
    {
        margin:0;
        background: #fafafa;
    }
    </style>
</head>

<body>


<div id="swagger-ui"></div>
<script src="${grailsApplication.config.getProperty('server.contextPath')}/webjars/swagger-ui/3.20.9/swagger-ui-bundle.js"> </script>
<script src="${grailsApplication.config.getProperty('server.contextPath')}/webjars/swagger-ui/3.20.9/swagger-ui-standalone-preset.js"> </script>
<script>
    window.onload = function() {
        // Begin Swagger UI call region
        const ui = SwaggerUIBundle({
            url: "${g.createLink(controller: 'ws', params: [json:true])}",
            dom_id: '#swagger-ui',
            deepLinking: true,
            presets: [
                SwaggerUIBundle.presets.apis,
                SwaggerUIStandalonePreset
            ],
            plugins: [
                SwaggerUIBundle.plugins.DownloadUrl
            ],
            layout: "StandaloneLayout",
            validatorUrl: "" //validation disabled due to duplicate operation Swagger annotation bug
        })
        // End Swagger UI call region
        window.ui = ui
    }
</script>
<style>
    .topbar { display:none; }
</style>
</body>
</html>
