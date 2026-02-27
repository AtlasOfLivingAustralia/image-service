%{--
- ﻿Copyright (C) 2013 Atlas of Living Australia
- All Rights Reserved.
-
- The contents of this file are subject to the Mozilla Public
- License Version 1.1 (the "License"); you may not use this file
- except in compliance with the License. You may obtain a copy of
- the License at http://www.mozilla.org/MPL/
-
- Software distributed under the License is distributed on an "AS
- IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
- implied. See the License for the specific language governing
- rights and limitations under the License.
--}%

<div class="form-horizontal" style="height: 200px">

    <div class="alert alert-error" id="errorMessageDiv" style="display: none">
    </div>

    <form id="criteriaForm">
        <div class="control-group">
            <label class="control-label" for='searchCriteriaDefinitionId'><g:message code="add.search.criteria.frament.criteria" /></label>
            <div class="controls">
                <g:select id="cmbCriteria" name="searchCriteriaDefinitionId" from="${criteriaDefinitions}" optionValue="name" optionKey="id" noSelection="${[0:"<Select Criteria>"]}" />
            </div>
        </div>
        <div id="criteriaDetail"></div>
    </form>

    <div id="addButtonDiv" style="display: none">
        <button id="btnSaveCriteria" type="button" class="btn btn-small btn-primary pull-right"><g:message code="add.search.criteria.frament.list.add.criteria" /></button>
    </div>

    <script type="text/javascript">

        $( document ).ready(function() {

            $("#cmbCriteria").on('change', function(e) {
                $("#criteriaDetail").html(loadingSpinner());
                var criteriaDefinitionId = $(this).val();
                if (criteriaDefinitionId == 0) {
                    $("#criteriaDetail").html("");
                    $("#addButtonDiv").css('display', 'none');
                } else {
                    $("#criteriaDetail").html(loadingSpinner());
                    $.ajax("${createLink(action: "criteriaDetailFragment")}?searchCriteriaDefinitionId=" + criteriaDefinitionId).done(function(content) {
                        $("#addButtonDiv").css("display", "block");
                        $("#criteriaDetail").html(content);
                    });
                }
            });

            $("#btnSaveCriteria").on('click', function(e) {
                var formData = $("#criteriaForm").serialize();
                var errorDiv = $("#errorMessageDiv");
                errorDiv.css("display",'none');
                $.post('${createLink(action:'ajaxAddSearchCriteria')}',formData, function(data) {
                    if (data.errorMessage) {
                        errorDiv.html(data.errorMessage);
                        errorDiv.css("display",'block');
                    } else {
                        imgvwr.hideModal();
                    }
                });
            });

        });

    </script>
</div>

