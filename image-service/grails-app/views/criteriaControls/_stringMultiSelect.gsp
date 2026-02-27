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
    <div class="alert alert-info" style="color: black">
        <strong>${criteriaDefinition.description}</strong>
    </div>
    <g:message code="string.multi.please.select.one" />
    <table>
        <tr>
            <td>
                <div style="margin-top: 5px; margin-bottom: 5px;">
                    <g:select class="form-control" id="fieldListBox" name="fieldValue" from="${allowedValues?.sort({ it.value })}" optionValue="value" optionKey="name" multiple="multiple" size="12" style="width: 300px" value="${criteria?.value?.split("\\|")?.toList()}" />
                </div>
            </td>
            <td style="vertical-align: top">
                <div style="margin-left: 10px" class="muted">
                    <strong><g:message code="string.multi.selected.values" /></strong>
                    <div id="selectedItemsList">
                    </div>
                </div>
            </td>
        </tr>
    </table>

</div>

<script type="text/javascript">

    var listBox = $("#fieldListBox");

    listBox.change(function(e) {
        renderSelectedItemsList();
    });

    function renderSelectedItemsList() {
        var html = "<ul>"

        var values = $("#fieldListBox").val();
        if (values) {
            for (itemIndex in values) {
                var item = values[itemIndex]
                html += "<li>" + item + "</li>";
            }
        }
        html += "</ul>"
        $("#selectedItemsList").html(html);
    }

    renderSelectedItemsList();
    listBox.focus();
</script>
