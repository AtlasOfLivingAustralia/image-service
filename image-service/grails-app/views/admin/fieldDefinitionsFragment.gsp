<div>
    <table class="table table-striped table-bordered">
        <thead>
            <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Value</th>
                <th />
            </tr>
        </thead>
        <tbody>
            <g:each in="${fieldDefinitions}" var="field">
                <tr fieldDefinitionId="${field.id}">
                    <td>${field.fieldName}</td>
                    <td>${field.fieldType}</td>
                    <td>${field.value}</td>
                    <td>
                        <button class="btn btn-sm btn-danger btnDeleteFieldDefinition"><i class="fa fa-remove"></i></button>
                        <button class="btn btn-sm btn-outline-dark btnEditFieldDefinition"><i class="fa fa-edit"></i></button>
                    </td>
                </tr>
            </g:each>
        </tbody>
    </table>
</div>