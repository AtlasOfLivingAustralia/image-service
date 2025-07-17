# image-service   [![Build Status](https://travis-ci.com/AtlasOfLivingAustralia/image-service.svg?branch=master)](https://travis-ci.org/AtlasOfLivingAustralia/image-service)

This Grails application provides the webservices and backend for the storage of all images in the Atlas.
It includes:

* Support for large images, generation of thumbnails and tile views
* Extensible key/value pair storage for image metadata
* Support for subimaging and maintaining the relationships between parent and child images
* Exif extraction
* Tile view for large images compatible with GIS Javascript clients such as LeafletJS, OpenLayers and Google Maps
* Web services for image upload
* Generate of derivative images for thumbnail presentation
* Tagging support via webservices
* Administrator console for image management
* Swagger API definition
* Integration with google analytics to monitor image usage by data resource
* Support for image storage in S3, Swift
* Support for batch uploads with AVRO

There are other related repositories to this one:
* [images-client-plugin](https://github.com/AtlasOfLivingAustralia/images-client-plugin) - a grails plugin to provide a Javascript based viewer to be used in other applications requiring a image viewer. This viewer is based on LeafletJS.
* [image-tiling-agent](https://github.com/AtlasOfLivingAustralia/image-tiling-agent) - a utility to run tiling jobs for the image-service. This is intended to used on multiple machine as tiling is CPU intensive and best parallelised.
* [image-loader](https://github.com/AtlasOfLivingAustralia/image-loader) - utility for bulk loading images into the image-service.

## Upgrading from 1.0

Please see the [Upgrading from 1.0 to 1.1](https://github.com/AtlasOfLivingAustralia/image-service/wiki/Upgrading-from-1.0-to-1.1) wiki page before upgrading an image-service 1.0 or earlier installation to the latest version.

## Architecture

* Grails 6.1.0 web application ran as standalone executable jar
* Open JDK 11
* Postgres database (11 or above)
* Elastic search 7
* Debian package install

## Installation

There are ansible scripts for this applications (and other ALA tools) in the [ala-install](https://github.com/AtlasOfLivingAustralia/ala-install) project. The ansible playbook for the image-service is [here](https://github.com/AtlasOfLivingAustralia/ala-install/blob/master/ansible/image-service.yml)

You can also run this application locally by following the instructions on its [wiki page](https://github.com/AtlasOfLivingAustralia/image-service/wiki)

## Running it locally

### Postgres
There is a docker-compose YML file that can be used to run postgres locally for local development purposes.
To use run:
```$xslt
docker-compose -f postgres.yml up -d
```
And to shutdown
```$xslt
docker-compose -f postgres.yml kill
```

### Elastic search
There is a docker-compose YML file that can be used to run elastic search locally for local development purposes.
To use run:
```$xslt
docker-compose -f elastic.yml up -d
```
And to shutdown
```$xslt
docker-compose -f elastic.yml kill
```

# Configuring a Global StorageOperations

This section describes how to configure a global StorageOperations instance that will be used in preference to the StorageLocation attached to the Image domain object in all places a StorageOperation is referenced.

## Overview

The image-service application now supports configuring a global StorageOperations instance in the application.yml file. When enabled, this StorageOperations instance will be used in preference to the StorageLocation attached to the Image domain object in all places a StorageOperation is referenced.

This feature is useful when you want to use a different storage backend for all images, without having to migrate the images to a new StorageLocation. For example, you might want to use an S3 bucket for all images, even though they were originally stored in a file system.

In the future, this feature will allow images to be served without checking the database, which may improve performance.

## Configuration

To configure a global StorageOperations instance, add the following configuration to your application.yml file:

```yaml
imageservice:
  storageOperations:
    enabled: true
    # Type of storage operations: 'fs', 's3', or 'swift'
    type: 'fs'
    basePath: '/data/image-service/store'
```

The configuration options depend on the type of StorageOperations you want to use:

### File System Storage

```yaml
imageservice:
  storageOperations:
    enabled: true
    type: 'fs'
    basePath: '/data/image-service/store'
```

### S3 Storage

```yaml
imageservice:
  storageOperations:
    enabled: true
    type: 's3'
    region: 'ap-southeast-2'
    bucket: 'my-bucket'
    prefix: 'images'
    accessKey: 'my-access-key'
    secretKey: 'my-secret-key'
    containerCredentials: false
    publicRead: true
    redirect: true
    pathStyleAccess: false
    hostname: ''
    cloudfrontDomain: 'my-cloudfront-domain.cloudfront.net'
```

### Swift Storage

```yaml
imageservice:
  storageOperations:
    enabled: true
    type: 'swift'
    authUrl: 'https://swift.example.com/auth/v1.0'
    containerName: 'my-container'
    username: 'my-username'
    password: 'my-password'
    tenantId: 'my-tenant-id'
    tenantName: 'my-tenant-name'
    authenticationMethod: 'BASIC'
    publicContainer: true
    redirect: true
```

## Disabling the Feature

To disable the feature, set `enabled: false` in the configuration:

```yaml
imageservice:
  storageOperations:
    enabled: false
```

When disabled, the application will use the StorageLocation attached to the Image domain object for all storage operations, as it did before this feature was added.