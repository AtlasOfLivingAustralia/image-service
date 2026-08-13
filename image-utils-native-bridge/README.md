# image-utils-native-bridge

This directory contains a C bridge (`src/main/c/ala_vips_dztiler.c`) and an experimental Rust `cdylib` bridge (`src/lib.rs`).

Both implementations preserve the same `ala_vips_dztiler` ABI, mirror the Google-style tile geometry with a bottom-left (TMS) row origin, and emit each tile to Java through a callback.

The C implementation is the default and remains the reference implementation for ABI and geometry parity.

## Scope

- Full pyramid generation (level range controlled by Java `minLevel/maxLevel`)
- Tile extraction origin: bottom-left
- Encoders: JPEG and PNG
- Output destination: callback only (no direct filesystem tile writes)

## Build assumptions

- System `libvips` must be installed (baseline tested with `vips-8.15.1`)
- C build uses `pkg-config` + system C compiler
- Rust build uses `pkg-config`, Cargo, and a Rust toolchain

## Build

```bash
cd image-utils-native-bridge
./gradlew stageNativeBridge
```

`stageNativeBridge` now selects the implementation via Gradle property:

- Default: `-PnativeBridgeImplementation=c`
- Experimental Rust: `-PnativeBridgeImplementation=rust`

Example (Rust):

```bash
cd image-utils-native-bridge
./gradlew stageNativeBridge -PnativeBridgeImplementation=rust
```

You can also force Rust staging directly with:

```bash
cd image-utils-native-bridge
./gradlew stageNativeBridgeRust
```

Output is staged to:

- `build/native/<os>-<arch>/libala_vips_dztiler.so` (Linux)
- `build/native/<os>-<arch>/libala_vips_dztiler.dylib` (macOS)

