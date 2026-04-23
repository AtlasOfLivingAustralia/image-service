# Native DZ Visual Comparator Harness

This harness compares full-pyramid tile output from:

- `JnaStreamingImageTiler` (reference)
- `NativeDzStreamingImageTiler` (candidate)

Comparison is visual (decoded pixels), not byte-identical.

## Spec

- `image-utils/src/test/groovy/au/org/ala/images/tiling/NativeDzVisualEquivalenceSpec.groovy`

## Default behavior

- Uses benchmark medium image from test fixtures.
- Compares full pyramid (`minLevel=0`, `maxLevel=Integer.MAX_VALUE`).
- Uses PNG by default for strict equivalence (`rmse=0`, `maxChannelDelta=0`).

## Useful system properties

- `tiler.compare.image=/abs/path/to/image.jpg`
- `tiler.compare.format=png|jpeg`
- `tiler.compare.minLevel=0`
- `tiler.compare.maxLevel=7`
- `tiler.compare.maxRmse=6.0`
- `tiler.compare.maxChannelDelta=40`
- `images.nativeDzTiler.bridgeLibraryPath=/abs/path/to/libala_vips_dztiler.so`

## Run (root build, Java 21)

```bash
cd /home/bea18c/dev/github/Atlas/image-service-420-ai-review
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 PATH=/usr/lib/jvm/java-21-openjdk-amd64/bin:$PATH ./gradlew :image-utils:test --tests au.org.ala.images.tiling.NativeDzVisualEquivalenceSpec
```

## Run (JPEG tolerance example)

```bash
cd /home/bea18c/dev/github/Atlas/image-service-420-ai-review
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 PATH=/usr/lib/jvm/java-21-openjdk-amd64/bin:$PATH ./gradlew :image-utils:test --tests au.org.ala.images.tiling.NativeDzVisualEquivalenceSpec -Dtiler.compare.format=jpeg -Dtiler.compare.maxRmse=6.0 -Dtiler.compare.maxChannelDelta=40
```

