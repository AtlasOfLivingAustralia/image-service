# image-project

The **image-project** provides a high-performance system for image storage, metadata management, and processing (thumbnailing and tiling), optimized for large-scale image repositories.

## Overview

The goal of this project is to provide a scalable and efficient way to store and serve images, with a focus on:
*   **Metadata Management**: Flexible key/value pair storage for image metadata.
*   **High Performance**: Native library integration (`libvips`) via JNA or Java's Foreign Function & Memory (FFM) API.
*   **Memory Efficiency**: Streaming-based image processing that avoids loading large images into Java heap memory.
*   **Tiling Support**: Generation of GIS-compatible tile views for very large images.

## Project Structure

This repository is organized into several modules:

### [image-service](./image-service/README.md)
The main **Grails 6** web application. It provides the RESTful API for image upload, metadata management, and serves thumbnails and tiles. It also includes the base **JNA-based** integration for `libvips`.

### [image-utils](./image-utils/README.md)
A shared **Java 11** core library that defines the common interfaces (`IImageThumbnailer`, `IImageTiler`) and provides a baseline pure Java implementation for image processing.

### [image-utils-ffm-libvips](./image-service-ffm-libvips/README.md)
A high-performance image processing implementation using the **Java FFM API** (introduced in Java 22). It uses `jextract`-generated bindings for `libvips`.

### [image-service-photofox](./image-service-photofox/README.md)
An alternative FFM implementation that leverages the [lopcode/vips-ffm](https://github.com/lopcode/vips-ffm) library for a more idiomatic Java wrapper around `libvips`.

## Building and Running

The root gradle project includes the `image-utils` and `image-service` modules.  To build and run the image-service, follow the instructions in the [image-service README](./image-service/README.md).

The repo also includes nested gradle projects for:
*   `image-service-ffm-libvips` - FFM-based implementation using `jextract` bindings for `libvips`
*   `image-service-photofox` - FFM-based implementation using the `lopcode/vips-ffm` library

Both these modules require Java 22+ to build but *currently* the Grails 6 web application only supports Java 21.

To build the main projects:
```bash
./gradlew build
```

To build the FFM based submodules:
```bash
./gradlew :image-service-ffm-libvips:build
./gradlew :image-service-photofox:build
```

## Dependencies

The native image processing implementations (JNA and FFM) require **libvips** to be installed on the host system.
*   **Ubuntu/Debian**: `sudo apt-get install libvips-dev libvips-tools`
*   **macOS**: `brew install vips`

## Deployment

The image-utils modules are designed to be extensible.  The `image-utils` provides a Service Provider for loading the
implementation at runtime. The `image-service` module is configured to include and use the JNA-based implementation by 
default.

Other image-utils clients can drop the `image-utils-photofox` module into the classpath and it will be found as the
highest priority implementation by the service provider.

The `image-utils-ffm-libvips` module includes support for the Java 21 FFM preview, so it can be used in the 
`image-service` application *if* the `image-service` is run with the JVM enable preview flag switch on (e.g. `--enable-preview`).
When present on the classpath the ffm-libvips implementation has a higher priority than the JNA-based implementation.