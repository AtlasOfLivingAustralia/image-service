# image-service   [![Build Status](https://github.com/AtlasOfLivingAustralia/image-service/actions/workflows/ci.yml/badge.svg)](https://github.com/AtlasOfLivingAustralia/image-service/actions/workflows/ci.yml)

This Grails application provides the webservices and backend for the storage of all images in the Atlas.
It includes:

* Support for large images, generation of thumbnails and tile views
* Extensible key/value pair storage for image metadata
* Support for subimaging and maintaining the relationships between parent and child images
* Exif extraction
* Tile view for large images compatible with GIS Javascript clients such as LeafletJS, OpenLayers and Google Maps
* Web services for image upload
* Generation of derivative images for thumbnail presentation
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

* Grails 6.2.x web application ran as standalone executable jar
* Open JDK 21
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
```bash
docker compose -f postgres.yml up -d
```
And to shutdown
```bash
docker compose -f postgres.yml kill
```

### Elastic search
There is a docker-compose YML file that can be used to run elastic search locally for local development purposes.
To use run:
```$bash
docker compose -f elastic-dc2.yml up -d
```
And to shutdown
```$bash
docker compose -f elastic-dc2.yml kill
```

### Disable caching (HTTP + in-memory)

You can disable all caching for image responses and bypass the in‑memory Caffeine caches used for thumbnails and tiles by setting the following configuration property:

```
images:
  disableCache: true
```

Effects when `images.disableCache = true`:

- Controllers that serve image bytes (standard image endpoints and IIIF) will not return `304 Not Modified` responses and will replace any cache headers with the following no‑cache headers on every response:
  - `Cache-Control: no-store, no-cache, must-revalidate`
  - `Pragma: no-cache`
  - `Expires: 0`
- `ImageStoreService` will bypass the Caffeine caches for thumbnail and tile lookups; results will not be cached in memory.

By default, `images.disableCache` is `false`.

### Redirects to storage and presigned URLs

When a storage location has redirects enabled, the image and IIIF endpoints answer with a `302` to the storage URL instead of streaming the bytes (add `?nr` to opt out per request). The redirect gets its own `Cache-Control`, separate from the image bytes:

- CloudFront / public URLs: `public, max-age=<images.redirect.cache.maxAgeSeconds>` (default 3600)
- Presigned S3 URLs (`privateAcl` without CloudFront or `publicRead`, valid for 1 hour): `private, max-age=<the smaller of images.redirect.cache.presignedMaxAgeSeconds (default 300) and the time left before the URL expires minus images.redirect.cache.expirySafetyMarginSeconds (default 60)>`
- `?i`, `images.disableCache=true`, or a presigned URL that is about to expire: `no-store`

**Important: keep the in-memory lookup caches shorter than the presigned URL TTL.** `ImageStoreService` caches lookup results, including the redirect URL, in Caffeine caches configured by:

- `image.store.lookup.cache.originalConfig`
- `image.store.lookup.cache.thumbnailsConfig`
- `image.store.lookup.cache.tilesConfig`

The defaults (`maximumSize=10000`) have **no time-based expiry**, so with presigned redirects a cached entry can keep handing out a URL that has already expired (clients get `403` from S3). If you use presigned redirects, add an expiry comfortably below the 1 hour signature lifetime, e.g.:

```
image:
  store:
    lookup:
      cache:
        originalConfig: maximumSize=10000,expireAfterWrite=30m
        thumbnailsConfig: maximumSize=10000,expireAfterWrite=30m
        tilesConfig: maximumSize=10000,expireAfterWrite=30m
```

Use `expireAfterWrite`, not `expireAfterAccess`: an entry that keeps being read would otherwise never expire. Public and CloudFront URLs don't expire, so this doesn't apply to them.
