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
<%@ page import="au.org.ala.images.CriteriaValueType; au.org.ala.images.SearchCriteriaUtils" %>
<div>
    <g:if test="${searchCriteria}">
        <h5 style="margin-top:10px; margin-bottom:10px;"><g:message code="criteria.list.each.criteria.must.be.met" /></h5>
        <ul class="list-unstyled list-inline">
        <g:each in="${searchCriteria}" var="criteria">
            <li searchCriteriaId="${criteria.id}">
                <div class="alert alert-info">
                    <img:searchCriteriaDescription criteria="${criteria}" />
                    <button type="button" class="btn-danger btn-sm pull-right btnDeleteCriteria" title="Remove this search criteria">
                        <i class="glyphicon glyphicon-icon-remove"> </i> <g:message code="criteria.list.delete" />
                    </button>&nbsp;
                </div>
            </li>
        </g:each>
        </ul>
    </g:if>
    <script type="text/javascript">
        $(".btnDeleteCriteria").on('click', function(e) {
            e.preventDefault();
            var criteriaId = $(this).parent().parent().attr("searchCriteriaId");
            if (criteriaId) {
                $.ajax("${createLink(controller: 'search', action:'ajaxRemoveSearchCriteria')}?searchCriteriaId=" + criteriaId).done(function(results) {
                    renderCriteria();
                    if (doSearch) {
                        doSearch();
                    }
                });
            }
        });

        $(".btnEditCriteria").on('click', function(e) {
            e.preventDefault();
            var criteriaId = $(this).parents("tr[searchCriteriaId]").attr("searchCriteriaId");
            if (criteriaId) {
                imgvwr.showModal({
                    url: "${createLink(action:'editSearchCriteriaFragment')}?criteriaId=" + criteriaId,
                    title: "Edit search criteria",
                    height: 520,
                    width: 700,
                    onClose: function () {
                        renderCriteria();
                    }
                });
            }
        });
    </script>
</div>

