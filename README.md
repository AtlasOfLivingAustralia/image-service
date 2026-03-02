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

### [image-service-ffm](./image-service-ffm/README.md)
A high-performance image processing implementation using the **Java FFM API** (introduced in Java 22). It uses `jextract`-generated bindings for `libvips`.

### [image-service-vips-ffm](./image-service-vips-ffm/README.md)
An alternative FFM implementation that leverages the [lopcode/vips-ffm](https://github.com/lopcode/vips-ffm) library for a more idiomatic Java wrapper around `libvips`.

## Building and Running

The project supports a multi-version Java build strategy.

*   **Java 21**: The base project can be built and run on Java 21, which will use the JNA or CLI-based `libvips` integration.
*   **Java 22+**: When built with Java 22 or newer, the FFM-based modules are automatically included, providing enhanced performance for image processing.

To build all projects:
```bash
./gradlew build
```

## Dependencies

The native image processing implementations (JNA and FFM) require **libvips** to be installed on the host system.
*   **Ubuntu/Debian**: `sudo apt-get install libvips-dev libvips-tools`
*   **macOS**: `brew install vips`

## Deployment

The image-service is designed to be extensible. FFM-based libraries can be added to the runtime classpath during deployment on Java 22+ hosts to automatically upgrade performance without code changes.

For more information, see:
*   [FFM Implementation Comparison](./FFM_IMPLEMENTATIONS_COMPARISON.md)
*   [Multi-Version Build Guide](./FFM_MULTI_VERSION_BUILD.md)
*   [Streaming JNA Implementation](./STREAMING_JNA_IMPLEMENTATION.md)
