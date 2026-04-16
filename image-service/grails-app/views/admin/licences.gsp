<!doctype html>
<html>
<head>
    <meta name="layout" content="adminLayout"/>
    <meta name="section" content="home"/>
    <title>Update Licences</title>
    <meta name="breadcrumbs" content="${g.createLink( controller: 'image', action: 'list')}, Images"/>
</head>
<body>

<content tag="pageTitle">Update Licences</content>
<div>
    <g:if test="${flash.message}">
        <div class="alert alert-success" style="display: block">${flash.message}</div>
    </g:if>
    <g:if test="${flash.errorMessage}">
        <div class="alert alert-danger" style="display: block">${flash.errorMessage}</div>
    </g:if>

    <div class="row">
        <h1>Update Licences</h1>
        <div class="well">
            <g:form class="form-horizontal" name="licencesUpdate" action="updateStoredLicences" controller="admin" method="POST">
                <h3>Licences</h3>
                <p>
                    Please paste in CSV data into the text box below (without column headers).
                    Text must be in CSV format with the following columns:
                <ul>
                <li><b>acronym</b> e.g. 'CC BY' - this is used to determine uniqueness</li>
                    <li><b>name</b>  e.g. 'Creative Commons by Attribution' </li>
                    <li><b>url</b> - the canonical URL representing the data licence e.g. 'https://creativecommons.org/publicdomain/zero/1.0/' </li>
                    <li><b>imageUrl</b> - a URL for an icon representing the data licence  e.g. 'https://licensebuttons.net/l/zero/1.0/88x31.png' </li>
                </ul>
                <div class="form-group">
                    <label class="control-label" for="licenses">Licences</label>
                    <textarea class="form-control" rows="10" id="licenses" name="licenses">${licenceCSV}</textarea>
                </div>

                <h3>Licence Mapping</h3>
                <p>
                    Please paste in CSV data into the text box below.
                    Text must be in CSV format with the following columns:
                <ul>
                    <li><b>acronym</b> e.g. "CC BY"</li>
                    <li><b>value</b> "Creative Commons CC BY"</li>
                </ul>
                </p>
                <div class="form-group">
                    <label class="control-label" for="licenseMapping">Licence Mapping</label>
                    <textarea class="form-control" rows="10" id="licenseMapping" name="licenseMapping">${licenceCSVMapping}</textarea>
                </div>
                <div class="form-group">
                    <button type="submit" class="btn btn-primary" id="btnUploadCSVImagesFile">Update</button>
                </div>
            </g:form>
        </div>
    </div>
</div>

</body>
</html>

