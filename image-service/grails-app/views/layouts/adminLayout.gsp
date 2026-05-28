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
        <asset:stylesheet src="application.css" />
    </head>
    <body>
        <div class="container-fluid">
            <div class="row">
                <div class="col-md-2">
                    <h1 class="mb-3">Admin tools</h1>
                    <ul class="nav nav-pills flex-column">
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'dashboard')}" title="Dashboard" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'batchUploads')}" title="Batch uploads" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'upload')}" title="Upload images" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'tools')}" title="Tools" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'duplicates')}" title="Duplicates" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'searchCriteria')}" title="Search Criteria" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'licences')}" title="Update Licences" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'tags')}" title="Tags" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'storageLocations')}" title="Storage Locations" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'settings')}" title="Settings" />
                        </li>
                        <li class="nav-item">
                            <img:menuNavItem href="${createLink(controller: 'admin', action: 'analytics')}" title="Analytics" />
                        </li>
                    </ul>
                </div>
                <div class="col-md-10">
                    <g:layoutBody/>
                </div>
            </div>
        </div>
    </body>
</g:applyLayout>
