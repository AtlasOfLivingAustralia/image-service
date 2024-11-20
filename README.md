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
* Open JDK ii
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
