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

<div>
    <div>
        <span><g:message code="image.metadata.criteria.select.item" /></span>
        <div>
            <g:select from="${metadataNames}" name="metadataItemName" value="${metadataItemName}" />
        </div>
    </div>

    <div>
        <img:criteriaValueControl criteriaDefinition="${criteriaDefinition}" value="${metadataItemValue}"/>
    </div>
</div>