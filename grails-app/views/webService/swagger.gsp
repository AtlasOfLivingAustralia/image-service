
<!-- HTML for static distribution bundle build -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Image Service API</title>
    <link rel="stylesheet" type="text/css" href="./webjars/swagger-ui/3.20.9/swagger-ui.css" >
    <link href="${grailsApplication.config.skin.favicon}" rel="shortcut icon"  type="image/x-icon"/>
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
<script src="./webjars/swagger-ui/3.20.9/swagger-ui-bundle.js"> </script>
<script src="./webjars/swagger-ui/3.20.9/swagger-ui-standalone-preset.js"> </script>
<script>
    window.onload = function() {
        // Begin Swagger UI call region
        const ui = SwaggerUIBundle({
            url: "./ws/api",
            dom_id: '#swagger-ui',
            deepLinking: true,
            presets: [
                SwaggerUIBundle.presets.apis,
                SwaggerUIStandalonePreset
            ],
            plugins: [
                SwaggerUIBundle.plugins.DownloadUrl
            ],
            layout: "StandaloneLayout"
        })
        // End Swagger UI call region
        window.ui = ui
    }
</script>
</body>
</html>
