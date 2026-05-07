<!doctype html>
<html>
<head>
    <meta name="layout" content="adminLayout"/>
    <title>ALA Images - Admin - Settings</title>
</head>

<body>
    <h3>Settings</h3>
    <p>Below is a listing of dynamic settings the system is using.
        These can manipulated in the database to have an effect on the running service.
    </p>
    <table class="table table-bordered table-striped">
        <thead>
            <th>Property name</th>
            <th>Description</th>
            <th>Current value</th>
        </thead>
        <g:each in="${settings}" var="setting">
            <tr>
                <td>
                    ${setting.name}
                </td>
                <td>
                    ${setting.description}
                </td>
                <td>
                    ${setting.value}
                </td>
            </tr>
        </g:each>
    </table>
</div>
</body>
</html>
