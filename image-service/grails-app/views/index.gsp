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
                <div class="row">
                    <div class="col-8 offset-2">
                        <div class="row mb-3">
                            <label class="col-form-label col-sm-3" for="search">
                                Find images
                            </label>
                            <div class="col-sm-9">
                                <g:textField name="search" id="search" class="form-control"/>
                                <button class="btn btn-primary mt-2" id="btnSearch">Search</button>
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
