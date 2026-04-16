<%@ page import="au.org.ala.cas.util.AuthenticationUtils" %>
<!doctype html>
<html>
<head>
    <meta name="layout" content="adminLayout"/>
    <title>ALA Images - Admin - Duplicates</title>
    <style type="text/css" media="screen">
    </style>
</head>

<body>

    <content tag="pageTitle">Duplicates</content>
    <content tag="adminButtonBar" />
    <div class="content">
        <h4>${totalCount} images have one or more duplicates</h4>
        <table class="table table-bordered">
            <thead>
                <tr>
                    <td>Image</td>
                    <td>MD5 Hash</td>
                    <td>Count</td>
                </tr>
            </thead>
            <g:each in="${results}" var="dupe">
                <tr>
                    <td>
                        <img src="<img:imageThumbUrl imageId="${dupe.image.imageIdentifier}" />" width="100">
                    </td>
                    <td>
                        <a href="${createLink(controller:'search', action:'list', params:[q:"contentMD5Hash=${dupe.hash}"])}">
                            ${dupe.hash}
                        </a>
                    </td>
                    <td>${dupe.count}</td>
                </tr>
            </g:each>
        </table>

        <div class="pagination">
            <tb:paginate total="${totalCount}" prev="" next="" />
        </div>
    </div>
</body>
</html>
