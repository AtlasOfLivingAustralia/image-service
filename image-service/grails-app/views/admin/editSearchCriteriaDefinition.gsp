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

<%@ page import="org.apache.commons.lang3.StringEscapeUtils" %>
<!doctype html>
<html>
    <head>
        <meta name="layout" content="adminLayout"/>
        <title>Soils to Satellites - Admin - Search Criteria</title>
    </head>

    <body>
        <content tag="pageTitle">Search Criteria</content>

        <content tag="adminButtonBar">
            <a class="btn btn-small" href="${createLink(controller: 'admin', action:'searchCriteria')}">Back to Search Criteria</a>
        </content>
        <g:form class="form-horizontal" action="saveSearchCriteriaDefinition" controller="admin">

            <g:hiddenField name="searchCriteriaDefinitionId" value="${criteriaDefinition?.id}" />

            <div class="well well-small">
                <h5>
                    <g:if test="${criteriaDefinition}">
                        Edit Search Criteria Definition
                    </g:if>
                    <g:else>
                        New Search Criteria Definition
                    </g:else>
                </h5>

                <div class="control-group">
                    <label class="control-label" for='name'>Name:</label>
                    <div class="controls">
                        <g:textField class="input-xlarge" name="name" placeholder="Name" value="${criteriaDefinition?.name}"/>
                    </div>
                </div>

                <div class="control-group">
                    <label class="control-label" for='description'>Description:</label>
                    <div class="controls">
                        <g:textField class="input-xlarge" name="description" placeholder="Description" value="${criteriaDefinition?.description}"/>
                    </div>
                </div>

                <div class="control-group">
                    <label class="control-label" for='type'>Type:</label>
                    <div class="controls">
                        <g:select name="type" from="${au.org.ala.images.CriteriaType.values()}" value="${criteriaDefinition?.type}" />
                    </div>
                </div>

                <div class="control-group">
                    <label class="control-label" for='valueType'>Value Type:</label>
                    <div class="controls">
                        <g:select name="valueType" from="${au.org.ala.images.CriteriaValueType.values()}" value="${criteriaDefinition?.valueType}" />
                    </div>
                </div>

                <div class="control-group">
                    <label class="control-label" for='fieldName'>Field:</label>
                    <div class="controls">
                        <g:textField class="input-xlarge" name="fieldName" placeholder="Field name" value="${criteriaDefinition?.fieldName}"/>
                    </div>
                </div>

                <div class="control-group">
                    <label class="control-label" for='units'>Units (optional):</label>
                    <div class="controls">
                        <g:textField class="input-xlarge" name="units" placeholder="Units" value="${criteriaDefinition?.units}"/>
                    </div>
                </div>


                <div class="control-group">
                    <div class="controls">
                        <g:submitButton class="btn btn-primary" name="submit" value='Save'/>
                    </div>
                </div>

            </div>

        </g:form>

        <script type="text/javascript">

            $(document).ready(function(e) {

            });

        </script>

    </body>
</html>
