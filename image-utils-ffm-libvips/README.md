# Image Service FFM Module

This module provides Foreign Function & Memory (FFM) API implementations for high-performance native libvips integration, requiring **Java 22 or later**.

## Overview

The FFM module uses Java's Panama Foreign Function & Memory API (introduced in Java 22) to interface with libvips for:
- **High-performance image thumbnailing** (`FfmStreamingImageThumbnailer`)
- **Image tiling for Deep Zoom** (`FfmStreamingImageTiler`)
- **Streaming I/O** to avoid loading entire images into memory

This replaces the older JNA-based implementation with modern Java native interop.

## Requirements

- **Java 22+** (for FFM API support)
- **libvips 8.9+** installed on the system
- **libvips development headers** (for jextract binding generation, optional)

## Building

### Multi-Version Build Support

The project supports building with both Java 21 and Java 22+:

#### Building with Java 21
```bash
# Uses JNA-based implementations only
./gradlew clean build
```

#### Building with Java 22+
```bash
# Includes both JNA and FFM implementations
./gradlew clean build
```

The build system automatically:
- Detects the Java version at build time
- Conditionally includes the `image-service-ffm` module for Java 22+
- Allows runtime discovery via ServiceLoader

### Runtime Detection

At runtime, the image-service will:
1. Check if the FFM module is on the classpath (Java 22+ build)
2. Use `ServiceLoader` to discover FFM implementations
3. Fall back to JNA or process-based implementations if FFM unavailable

## Generating libvips Bindings with jextract

The module includes a Gradle task to auto-generate FFM bindings from libvips headers:

```bash
# Generate bindings (requires libvips dev headers installed)
./gradlew :image-service-ffm:generateVipsBindings
```

### Installing libvips Headers

**Ubuntu/Debian:**
```bash
sudo apt-get install libvips-dev
```

**macOS (Homebrew):**
```bash
brew install vips
```

**macOS (MacPorts):**
```bash
sudo port install vips
```

### jextract Configuration

The build task searches for libvips headers in standard locations:
- `/usr/include/vips/vips.h`
- `/usr/local/include/vips/vips.h`
- `/opt/homebrew/include/vips/vips.h` (macOS ARM)
- `/opt/local/include/vips/vips.h` (MacPorts)

If headers are in a non-standard location, set:
```bash
export VIPS_INCLUDE_PATH=/path/to/vips/include
```

### Generated Bindings

jextract generates bindings in:
```
src/main/java/au/org/ala/images/ffm/generated/
```

These bindings provide type-safe, zero-overhead access to libvips functions.

## Architecture

### ServiceLoader Pattern

The FFM module uses Java's ServiceLoader for dynamic discovery:

```
image-service (Java 21)
  └── FfmLibraryLoader (discovers implementations)
       └── FfmLibraryFactory (interface)
            └── [discovered at runtime]

image-service-ffm (Java 22+)
  └── FfmLibraryFactoryImpl (provides implementations)
       ├── FfmStreamingImageThumbnailer
       ├── FfmStreamingImageTiler
       └── VipsLibraryFFM (FFM bindings)
```

### Key Classes

- **`VipsLibraryFFM`**: FFM bindings to libvips functions
- **`NativeLibraryDetectorFFM`**: Loads and initializes libvips
- **`InputStreamVipsSourceFFM`**: Streams image data to libvips via callbacks
- **`FfmStreamingImageThumbnailer`**: FFM thumbnailer implementation
- **`FfmStreamingImageTiler`**: FFM tiler implementation
- **`FfmLibraryFactoryImpl`**: ServiceLoader provider

## Usage in Code

```groovy
import au.org.ala.images.ffm.FfmLibraryLoader

// Check if FFM is available
if (FfmLibraryLoader.isAvailable()) {
    println "Using FFM: ${FfmLibraryLoader.implementationName}"

    // Create FFM-based thumbnailer
    def factory = FfmLibraryLoader.factory
    def thumbnailer = factory.createThumbnailer(fallbackThumbnailer)

    // Create FFM-based tiler
    def tiler = factory.createTiler(fallbackTiler, 256)
}

// Cleanup on shutdown
FfmLibraryLoader.shutdown()
```

## Performance Benefits

FFM offers several advantages over JNA:

1. **Zero-overhead calls**: Direct native function invocation
2. **Better memory management**: Arena-based lifecycle management
3. **Type safety**: Compile-time checked native interactions
4. **Modern API**: Integrated with Java's memory model
5. **Standard support**: Part of core Java (no external dependencies)

## Troubleshooting

### FFM Module Not Found

**Symptom:** FFM implementations not discovered at runtime

**Solutions:**
- Verify you're running with Java 22+: `java -version`
- Check module is built: `ls image-service-ffm/build/libs/`
- Verify ServiceLoader config exists:
  ```bash
  jar tf image-service-ffm/build/libs/image-service-ffm-*.jar | grep FfmLibraryFactory
  ```

### libvips Not Found

**Symptom:** `NativeLibraryDetectorFFM` reports libvips unavailable

**Solutions:**
- Install libvips: `sudo apt-get install libvips42` (Ubuntu/Debian)
- Check library path: `ldconfig -p | grep vips`
- Set `LD_LIBRARY_PATH` if needed:
  ```bash
  export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
  ```

### Binding Generation Fails

**Symptom:** `generateVipsBindings` task fails

**Solutions:**
- Install development headers: `sudo apt-get install libvips-dev`
- Verify header location: `find /usr -name vips.h 2>/dev/null`
- Check jextract version compatibility

## Development

### Adding New libvips Functions

1. Update `VipsLibraryFFM.groovy` with method handle and descriptor
2. Add function to jextract `--include-function` list in `build.gradle`
3. Regenerate bindings: `./gradlew generateVipsBindings`

### Testing FFM Code

```bash
# Run tests with Java 22+
./gradlew :image-service-ffm:test

# Run full integration tests
./gradlew :image-service:test
```

## Future Enhancements

- [ ] Full varargs support for libvips operations
- [ ] Memory-mapped file I/O for large images
- [ ] Parallel tile generation
- [ ] Custom arena management for long-lived operations
- [ ] Performance benchmarking vs JNA

## References

- [Java FFM API (JEP 454)](https://openjdk.org/jeps/454)
- [jextract Tool](https://jdk.java.net/jextract/)
- [libvips Documentation](https://www.libvips.org/API/current/)
- [Panama Project](https://openjdk.org/projects/panama/)
