<!doctype html>
<html>
    <head>
        <meta name="layout" content="${grailsApplication.config.getProperty('skin.layout')}"/>
        <meta name="section" content="home"/>
        <title>ALA Image Service - Home</title>
    </head>

    <body class="content">
        <div class="row">
            <div class="">
                <h1>ALA Image Service</h1>
                <p>
                Welcome to the Atlas of Living Australia's Image Service.
                </p>
                <p>
                Some more words...
                </p>
                <div class="row-fluid">
                    <div class="span8 offset2">
                        <div class="form-horizontal">
                            <div class="control-group">
                                <label class="control-label" for="search">
                                    Find images
                                </label>
                                <div class="controls">
                                    <g:textField name="search" id="search"/>
                                    <button class="btn" id="btnSearch">Search</button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

            </div>
        </div>
    </body>
    <script>
        $(document).ready(function() {
            $("#btnSearch").on('click', function(e) {
                e.preventDefault();
                window.location = "${createLink(controller:'search', action: 'list')}?q=" + $("#search").val();
            });
        });
    </script>
</html>
