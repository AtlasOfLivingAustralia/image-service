<%@ page import="org.javaswift.joss.client.factory.AuthenticationMethod; au.org.ala.images.SwiftStorageLocation; au.org.ala.images.S3StorageLocation; au.org.ala.images.FileSystemStorageLocation" %>
<form id="update-storage-location-form">
    <g:hiddenField name="id" value="${fileSystemStorageLocation?.id ?: s3StorageLocation?.id ?: swiftStorageLocation?.id}" />

    <div class="form-group">
        <label class="radio-inline">
            <g:radio disabled="true" name="class" value="${FileSystemStorageLocation}" checked="${fileSystemStorageLocation?.class}" />
            File system
        </label>
        <label class="radio-inline">
            <g:radio disabled="true" name="class" value="${S3StorageLocation}" checked="${s3StorageLocation?.class}" />
            AWS S3 Bucket
        </label>
        <label class="radio-inline">
            <g:radio disabled="true" name="class" value="${SwiftStorageLocation}" checked="${swiftStorageLocation?.class}" />
            Swift
        </label>
    </div>
    <g:if test="${fileSystemStorageLocation}">
        <div id="fs-form">
            <div class="form-group">
                <label for="basePath">Base Path</label>
                <g:textField class="form-control" name="basePath" value="${fileSystemStorageLocation.basePath}" placeholder="/data/images/storage" />
            </div>
        </div>
    </g:if>
    <g:elseif test="${s3StorageLocation}">
        <div id="s3-form">
            <div class="form-group">
                <label for="region">Region</label>
                <g:textField class="form-control" name="region" value="${s3StorageLocation.region}" placeholder="ap-southeast-2" />
            </div>
            <div class="form-group">
                <label for="bucket">Bucket name</label>
                <g:textField class="form-control" name="bucket" value="${s3StorageLocation.bucket}" placeholder="ala-image-service" />
            </div>
            <div class="form-group">
                <label for="prefix">Object Prefix</label>
                <g:textField class="form-control" name="prefix" value="${s3StorageLocation.prefix}" placeholder="images/prefix" />
            </div>
            <div class="form-group">
                <label>
                    <g:checkBox name="containerCredentials" value="${s3StorageLocation.containerCredentials}" /> Use Container Credentials instead of Access Key and Secret Key
                </label>
            </div>
            <div class="form-group">
                <label for="accessKey">Access Key</label>
                <g:textField class="form-control" name="accessKey" value="${s3StorageLocation.accessKey}" placeholder="" />
            </div>
            <div class="form-group">
                <label for="secretKey">Secret Key</label>
                <g:textField class="form-control" name="secretKey" value="${s3StorageLocation.secretKey}" placeholder="" />
            </div>
            <div class="checkbox">
                <label>
                    <g:checkBox name="publicRead" value="${s3StorageLocation.publicRead}" /> Public read
                </label>
                <p class="help-block">
                    When checked (and Private ACL is not), uploaded objects will be set to S3 "public read" via canned ACL.
                    When both are unchecked, no ACL header is sent and bucket defaults apply.
                </p>
            </div>
            <div class="checkbox">
                <label>
                    <g:checkBox name="privateAcl" value="${s3StorageLocation.privateAcl}" /> Explicit Private ACL
                </label>
                <p class="help-block">
                    When checked (and Public read is not), uploaded objects will be set to S3 "private" via canned ACL.
                    When both are unchecked, no ACL header is sent and bucket defaults apply.
                </p>
            </div>
            <div class="checkbox">
                <label>
                    <g:checkBox name="redirect" value="${s3StorageLocation.redirect}" /> Redirect
                </label>
            </div>
            <div class="form-group">
                <label for="endpoint">Cloudfront Redirect Domain</label>
                <g:textField class="form-control" name="cloudfrontDomain" value="${s3StorageLocation.cloudfrontDomain}" placeholder="images-assets.example.org" />
            </div>
        </div>
    </g:elseif>
    <g:elseif test="${swiftStorageLocation}">
        <div id="swift-form">
            <div class="form-group">
                <label for="authUrl">Auth URL</label>
                <g:textField type="url" class="form-control" name="authUrl" value="${swiftStorageLocation.authUrl}" placeholder="https://example.org/v1/auth" />
            </div>
            <div class="form-group">
                <label for="authenticationMethod">Auth Method</label>
                <g:select name="authenticationMethod" from="${AuthenticationMethod.values()}" value="${swiftStorageLocation.authenticationMethod}" />
            </div>
            <div class="form-group">
                <label for="username">Username</label>
                <g:textField class="form-control" name="username" value="${swiftStorageLocation.username}" placeholder="test:testing" />
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <g:textField class="form-control" name="password" value="${swiftStorageLocation.password}" placeholder="tester" />
            </div>
            <div class="form-group">
                <label for="tenantId">Tenant ID</label>
                <g:textField class="form-control" name="tenantId" value="${swiftStorageLocation.tenantId}" placeholder="" />
            </div>
            <div class="form-group">
                <label for="tenantName">Tenant Name</label>
                <g:textField class="form-control" name="tenantName" value="${swiftStorageLocation.tenantName}" placeholder="" />
            </div>
            <div class="form-group">
                <label for="containerName">Container Name</label>
                <g:textField class="form-control" name="containerName" value="${swiftStorageLocation.containerName}" placeholder="images" />
            </div>
            <div class="checkbox">
                <label>
                    <g:checkBox name="publicContainer" value="${swiftStorageLocation.publicContainer}"/> Public container
                </label>
            </div>
            <div class="checkbox">
                <label>
                    <g:checkBox name="redirect" value="${swiftStorageLocation.redirect}"/> Redirect
                </label>
            </div>
        </div>
    </g:elseif>
    <button type="button" id="btn-update-storage-location" class="btn btn-default">Save</button>
</form>
