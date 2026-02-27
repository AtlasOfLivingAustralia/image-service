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
<%@ page defaultCodec="none" %>
<div>
    <h4>${criteria?.criteriaDefinition?.name}</h4>
    <div class="alert alert-error" id="errorMessageDiv" style="display: none">
    </div>
    <form id="criteriaForm">
        <g:hiddenField name="criteriaId" value="${criteria?.id}" />
        <g:hiddenField name="searchCriteriaDefinitionId" value="${criteria?.criteriaDefinition?.id}" />
        <div id="criteriaDetail"></div>
    </form>
</div>

<div id="buttonDiv">
    <button id="btnSaveCriteria" type="button" class="btn btn-small btn-primary pull-right"><g:message code="edit.search.save.changes" /></button>
    <button id="btnCancelEdit" style="margin-right: 10px" type="button" class="btn btn-small pull-right"><g:message code="edit.search.cancel" /></button>
</div>


<script type="text/javascript">

    var url = "${createLink(action: "criteriaDetailFragment", params:[criteriaId:criteria?.id, searchCriteriaDefinitionId: criteria?.criteriaDefinition?.id])}";

    $("#criteriaDetail").html(loadingSpinner());

    $.ajax(url).done(function(content) {
        $("#criteriaDetail").html(content);
    });

    $("#btnCancelEdit").on('click', function(e) {
        e.preventDefault();
        imgvwr.hideModal();
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
</script>