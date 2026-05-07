<!doctype html>
<html>
<head>
    <meta name="layout" content="adminLayout"/>
    <title>ALA Images - Admin - Tools</title>
    <style type="text/css" media="screen">
    </style>
</head>
<body>
    <content tag="pageTitle">Analytics</content>
    <content tag="adminButtonBar" />
    <g:if test="${flash.message}">
        <div class="alert alert-success" style="display: block">${flash.message}</div>
    </g:if>
    <g:if test="${flash.errorMessage}">
        <div class="alert alert-danger" style="display: block">${flash.errorMessage}</div>
    </g:if>

    <g:if test="${!results}">
        <div class="alert alert-warning" style="display: block">No results for analytics. Check GA credentials are available at ${grailsApplication.config.getProperty('analytics.credentialsJson')}</div>
    </g:if>
    <g:each in="${results}" var="resultsPeriod">
        <h3><g:message code="${resultsPeriod.key}" default="${resultsPeriod.key}"/> - total views: ${resultsPeriod.value.totalEvents}</h3>
        <table class="table table-condensed table-striped">
            <g:each in="${resultsPeriod.value.entities.sort({  a, b -> a.name <=> b.name })}" var="entity">
                <tr>
                    <td>${entity.name}</td>
                    <td>${entity.count}</td>
                </tr>
            </g:each>
        </table>
    </g:each>
</body>
</html>
