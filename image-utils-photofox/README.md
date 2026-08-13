# image-service-vips-ffm

This module provides an alternative high-performance image processing implementation for `image-service` using the Java Foreign Function & Memory (FFM) API (introduced in Java 22).

It uses the [lopcode/vips-ffm](https://github.com/lopcode/vips-ffm) library, which is a high-level, idiomatic Java wrapper for `libvips` using FFM.

## Key Features
*   **Modern Native Interop**: Uses the stable FFM API (JEP 454) for efficient interaction with `libvips`.
*   **Streaming Support**: Implements `IImageThumbnailer` and `IImageTiler` with full streaming support, avoiding large memory allocations.
*   **High-Level API**: Leverages the `vips-ffm` library for a more Java-like interaction with native memory.

## Requirements
*   **Java 22+**: This module uses the FFM API and is only included in the build when running on Java 22 or newer.
*   **libvips**: The native library must be installed on the host system.

## Integration
This module is discovered at runtime via `ServiceLoader`. If the JAR is present on the classpath and the runtime is Java 22+, `image-service` will prefer this implementation for thumbnailing and tiling operations.
