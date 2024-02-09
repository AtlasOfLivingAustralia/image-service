<!doctype html>
<html>
    <head>
        <meta name="layout" content="${grailsApplication.config.getProperty('skin.layout')}"/>
        <title><g:message code="list.title.images" /></title>
        <asset:stylesheet src="application.css" />
        <asset:stylesheet src="search.css" />
        <asset:javascript src="search.js" />
    </head>
    <body>
        <g:if test="${flash.message}">
            <div class="alert alert-success" style="display: block">${flash.message}</div>
        </g:if>
        <g:if test="${flash.errorMessage}">
            <div class="alert alert-danger" style="display: block">${flash.errorMessage}</div>
        </g:if>

        <div class="row">
            <div class="col-md-3">
                <h1 style="margin-top:0;"><g:message code="list.total.images" args="[g.formatNumber(number:totalImageCount, format:'###,###,###')]" /></h1>
            </div>
            <!-- search box -->
            <div class="search col-md-7" style="margin-bottom:20px;">
                <g:form action="list" controller="search" method="get">
                    <div class="input-group">
                        <input type="text" class="input-large form-control" id="keyword" name="q" value="${params.q}" />
                        <div class="input-group-btn">
                            <button class="btn btn-primary" type="submit">
                                <span class="glyphicon glyphicon-search"></span>
                                <g:message code="list.search" />
                            </button>
                            <a id="btnAddCriteria" class="btn btn-default">
                                <g:message code="list.advanced.search" />
                            </a>
                            <a class="btn btn-default" href="${createLink(controller:'search', action:'download')}?${request.getQueryString()}">
                                <span class="glyphicon glyphicon-download"></span>
                                <g:message code="list.download.results" />
                            </a>
                        </div>
                    </div>
                </g:form>
            </div>

            <div class="col-md-2">
                <g:if test="${isAdmin}">
                    <g:link   controller="admin" action="dashboard" class="btn btn-danger" type="submit">
                        <span class="glyphicon glyphicon-cog"></span>
                        <g:message code="list.admin" />
                    </g:link>
                </g:if>
                <g:link mapping="api_doc" class="btn btn-info" type="submit">
                    <span class="glyphicon glyphicon-wrench"></span>
                    <g:message code="list.view.api" />
                </g:link>
            </div>
        </div>

        <!-- results -->
        <g:render template="imageThumbnails"
                  model="${[images: images, facets: facets, totalImageCount: totalImageCount, allowSelection: isLoggedIn,
                            selectedImageMap: selectedImageMap]}" />

        <div id="addCriteriaModal" class="modal fade" role="dialog">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                        <h4 class="modal-title"><g:message code="list.advanced.search" /></h4>
                    </div>
                    <div class="modal-body">
                        <form id="criteriaForm">
                            <div class="control-group">
                                <label class="control-label" for='searchCriteriaDefinitionId'>Criteria:</label>
                                <g:select class="form-control" id="cmbCriteria" name="searchCriteriaDefinitionId" from="${criteriaDefinitions}"
                                          optionValue="name" optionKey="id" noSelection="${[0:"<Select Criteria>"]}" />
                            </div>
                            <div id="criteriaDetail" style="margin-top:10px;">
                            </div>
                        </form>
                    </div>
                    <div class="modal-footer">
                        <button id="btnSaveCriteria" type="button" class="btn btn-small btn-primary pull-right"><g:message code="list.add.criteria" /></button>
                        <button type="button" class="btn btn-default" data-dismiss="modal"><g:message code="list.close" /></button>
                    </div>
                </div>
            </div>
        </div>

    <script>

        $(document).ready(function() {
            $("#btnAddCriteria").on('click', function (e) {
                e.preventDefault();
                $('#addCriteriaModal').modal('show');
            });

            $("#btnSearch").on('click', function(e) {
                e.preventDefault();
                doSearch();
            });


            $("#cmbCriteria").on('change', function(e) {
                // $("#criteriaDetail").html(loadingSpinner());
                var criteriaDefinitionId = $(this).val();
                if (criteriaDefinitionId == 0) {
                    $("#criteriaDetail").html("");
                    $("#addButtonDiv").css('display', 'none');
                } else {
                    // $("#criteriaDetail").html(loadingSpinner());
                    $.ajax("${createLink(controller:'search',action: "criteriaDetailFragment")}?searchCriteriaDefinitionId=" + criteriaDefinitionId).done(function(content) {
                        $("#addButtonDiv").css("display", "block");
                        $("#criteriaDetail").html(content);
                    });
                }
            });

            $("#btnSaveCriteria").on('click', function(e) {

                var formData = $("#criteriaForm").serialize();
                var errorDiv = $("#errorMessageDiv");
                errorDiv.css("display",'none');
                $.post('${createLink(controller:'search', action:'ajaxAddSearchCriteria')}',formData, function(data) {
                    if (data.errorMessage) {
                        errorDiv.html(data.errorMessage);
                        errorDiv.css("display",'block');
                    } else {
                        console.log(data.criteriaID);
                        // renderCriteria()
                        if (window.location.href.includes("?")){
                            window.location.href = window.location.href + "&criteria=" + data.criteriaID;
                        } else {
                            window.location.href = window.location.href + "?criteria=" + data.criteriaID;
                        }

                        $('#addCriteriaModal').modal('hide');
                    }
                });
            });

            <g:if test="${hasCriteria}">
            renderCriteria();
            doSearch();
            </g:if>

        });

        function clearResults() {
            $("#searchResults").html("");
        }

        function doSearch() {
            doAjaxSearch("${createLink(controller:'search', action:'searchResultsFragment')}");
        }

        function doAjaxSearch(url) {
            $("#searchResults").html('<div>Searching...<img src="${resource(dir:'images', file:'spinner.gif')}"></img></div>');
            $.ajax(url).done(function(content) {
                $("#searchResults").html(content);
                $(".pagination a").on('click', function(e) {
                    e.preventDefault();
                    doAjaxSearch($(this).attr("href"));
                });
                layoutImages();
                $('.thumb-caption').removeClass('hide');
            });
        }

        function renderCriteria() {
            $.ajax("${createLink(action: 'criteriaListFragment', params:[:])}").done(function (content) {
                $("#searchCriteria").html(content);
            });
        }
    </script>

    </body>
</html>
