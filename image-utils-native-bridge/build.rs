fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");

    pkg_config::Config::new()
        .atleast_version("8.15.0")
        .probe("vips")
        .expect("pkg-config could not resolve libvips; install libvips development files and ensure pkg-config is configured");
}

