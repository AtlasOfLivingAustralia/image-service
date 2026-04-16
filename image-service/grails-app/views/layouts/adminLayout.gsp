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
<g:applyLayout name="_main">
    <head>
        <title>Admin</title>
        <meta name="breadcrumbs" content="${g.createLink( controller: 'search', action: 'list')}, Images"/>
    </head>
    <body>
        <asset:stylesheet src="application.css" />
        <div class="container-fluid">
            <div class="row-fluid">
                <div class="col-md-2">
                    <h1>Admin tools</h1>
                    <ul class="nav nav-pills nav-stacked">
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'dashboard')}" title="Dashboard" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'batchUploads')}" title="Batch uploads" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'upload')}" title="Upload images" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'tools')}" title="Tools" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'duplicates')}" title="Duplicates" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'searchCriteria')}" title="Search Criteria" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'licences')}" title="Update Licences" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'tags')}" title="Tags" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'storageLocations')}" title="Storage Locations" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'settings')}" title="Settings" />
                        <img:menuNavItem href="${createLink(controller: 'admin', action: 'analytics')}" title="Analytics" />
                    </ul>
                </div>
                <div class="col-md-10">
                    <g:layoutBody/>
                </div>
            </div>
        </div>
    </body>
</g:applyLayout>
