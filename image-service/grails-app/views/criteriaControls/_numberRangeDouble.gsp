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

<%@ page import="au.org.ala.images.SearchCriteriaUtils;" %>
<div>
    <div>
        <g:if test="${value}">
            <g:set var="evaluator" value="${new SearchCriteriaUtils.DoubleCriteriaTranslator(value as String)}" />
        </g:if>
        <label class="radio inline" style="white-space: nowrap">
            <g:radio class="radioButton" name="operator" value="gt" checked="${evaluator == null || evaluator?.operator == 'gt' ? 'checked' : ''}"/>
            <g:message code="number.greater.than.or.equal.to" />
        </label>
        <label class="radio inline" style="white-space: nowrap">
            <g:radio class="radioButton" name="operator" value="lt" checked="${evaluator?.operator == 'lt' ? 'checked' : ''}" />
            <g:message code="number.less.than.or.equal.to" />
        </label>
        <label class="radio inline" style="white-space: nowrap">
            <g:radio class="radioButton" name="operator" value="bt" checked="${evaluator?.operator == 'bt' ? 'checked' : ''}" />
            <g:message code="number.date.range.between" />
        </label>
    </div>
    <div style="margin-top: 15px">
        <g:textField class="input-small" style="margin-left: 20px" name="numberValue" placeholder="Value" value="${evaluator?.value1}"/>
        <span class="numberRangeOther" style="display: ${evaluator?.operator == 'bt' ? 'inline-block' : 'none' }">
            &nbsp;<g:message code="number.and" />&nbsp;
            <g:textField class="input-small" style="margin-left: 20px" name="numberValue2" placeholder="Value" value="${evaluator?.value2}"/>
        </span>
        <g:if test="${units}">
            <span>&nbsp;(${units})</span>
        </g:if>

    </div>
    <g:hiddenField name="units" value="${units}" />
</div>
<script type="text/javascript">

    $(document).ready(function() {

        $(".radioButton").on('click', function(e) {
            var operator = $(this).val();
            if (operator == "bt") {
                $(".numberRangeOther").css("display", "inline-block");
            } else {
                $(".numberRangeOther").css("display", "none");
            }
        });

    });

</script>
