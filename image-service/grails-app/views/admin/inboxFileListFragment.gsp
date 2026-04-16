<%@ page import="au.org.ala.images.ImportFieldValueExtractor" %>
${fileList.size()} files found.
<table class="table table-bordered table-condensed table-striped">
    <thead>
        <tr>
            <th>Name</th>
            <th>Size</th>
            <g:each in="${fieldDefinitions}" var="field">
                <th>${field.fieldName}</th>
            </g:each>
        </tr>
    </thead>
    <tbody>
        <g:each in="${fileList}" var="file">
            <tr>
                <td>${file.name}</td>
                <td><img:sizeInBytes size="${file.length()}" /></td>
                <g:each in="${fieldDefinitions}" var="field">
                    <td>
                        ${ImportFieldValueExtractor.extractValue(field, file)}
                    </td>
                </g:each>
            </tr>
        </g:each>
    </tbody>
</table>