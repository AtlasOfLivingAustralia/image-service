use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

const VIPS_ACCESS_RANDOM: c_int = 0;
const GBOOLEAN_TRUE: c_int = 1;
const UNKNOWN_ERROR: &str = "unknown error";
const CALLBACK_FAILURE: &str = "tile callback returned failure";
const INVALID_ARGUMENT: &str = "invalid argument: input_source, subsamples and callback are required";
const INVALID_LEVEL_SIZE: &str = "invalid argument: level_count and tile_size must be > 0";
const INVALID_LEVEL_BOUNDS: &str = "invalid argument: level bounds are outside available pyramid levels";
const INVALID_SUBSAMPLE: &str = "invalid subsample value in pyramid";
const PANIC_MESSAGE: &str = "panic in rust native dz tiler bridge";

static EMPTY_STRING: &[u8] = b"\0";
static ACCESS_OPTION: &[u8] = b"access\0";
static COMPRESSION_OPTION: &[u8] = b"compression\0";
static STRIP_OPTION: &[u8] = b"strip\0";
static EXTEND_OPTION: &[u8] = b"extend\0";
static BACKGROUND_OPTION: &[u8] = b"background\0";
static PNG_CONTENT_TYPE: &[u8] = b"image/png\0";
static JPEG_CONTENT_TYPE: &[u8] = b"image/jpeg\0";
const VIPS_EXTEND_BACKGROUND: c_int = 2;

type VipsSource = c_void;
type VipsImage = c_void;
type VipsArea = c_void;
type VipsArrayDouble = c_void;
type AlaVipsTileCallback = unsafe extern "C" fn(
    level: c_int,
    x: c_int,
    y: c_int,
    content_type: *const c_char,
    data: *const c_void,
    length: size_t,
    user_data: *mut c_void,
) -> c_int;

unsafe extern "C" {
    fn g_strdup(input: *const c_char) -> *mut c_char;
    fn g_free(ptr: *mut c_void);
    fn g_object_ref(object: *mut c_void) -> *mut c_void;
    fn g_object_unref(object: *mut c_void);
    fn vips_area_unref(area: *mut VipsArea);
    fn vips_array_double_new(array: *const f64, n: c_int) -> *mut VipsArrayDouble;

    fn vips_error_buffer() -> *const c_char;
    fn vips_error_clear();

    fn vips_image_new_from_source(
        source: *mut VipsSource,
        option_string: *const c_char,
        ...
    ) -> *mut VipsImage;
    fn vips_resize(input: *mut VipsImage, output: *mut *mut VipsImage, scale: f64, ...) -> c_int;
    fn vips_crop(
        input: *mut VipsImage,
        output: *mut *mut VipsImage,
        left: c_int,
        top: c_int,
        width: c_int,
        height: c_int,
        ...
    ) -> c_int;
    fn vips_embed(
        input: *mut VipsImage,
        output: *mut *mut VipsImage,
        x: c_int,
        y: c_int,
        width: c_int,
        height: c_int,
        ...
    ) -> c_int;
    fn vips_pngsave_buffer(
        input: *mut VipsImage,
        buf: *mut *mut c_void,
        len: *mut size_t,
        ...
    ) -> c_int;
    fn vips_jpegsave_buffer(
        input: *mut VipsImage,
        buf: *mut *mut c_void,
        len: *mut size_t,
        ...
    ) -> c_int;
    fn vips_image_get_width(image: *mut VipsImage) -> c_int;
    fn vips_image_get_height(image: *mut VipsImage) -> c_int;
}

fn is_png_suffix_bytes(bytes: &[u8]) -> bool {
    bytes.starts_with(b".png")
}

unsafe fn is_png_suffix(suffix: *const c_char) -> bool {
    if suffix.is_null() {
        return false;
    }

    // SAFETY: `suffix` is a C string provided by the caller or libvips-facing Java code.
    let suffix_bytes = unsafe { CStr::from_ptr(suffix) }.to_bytes();
    is_png_suffix_bytes(suffix_bytes)
}

fn to_glib_owned_string(message: &str) -> *mut c_char {
    let sanitized = if message.contains('\0') {
        message.replace('\0', " ")
    } else {
        message.to_owned()
    };

    let c_string = CString::new(sanitized).unwrap_or_else(|_| CString::new(UNKNOWN_ERROR).unwrap());
    // SAFETY: `c_string` is a valid NUL-terminated string and `g_strdup` copies it.
    unsafe { g_strdup(c_string.as_ptr()) }
}

unsafe fn set_error(error_out: *mut *mut c_char, message: &str) {
    if error_out.is_null() {
        return;
    }

    // SAFETY: caller provided storage for a `char *`; we store a GLib-owned duplicate.
    unsafe {
        *error_out = to_glib_owned_string(message);
    }
}

unsafe fn set_vips_error(error_out: *mut *mut c_char, prefix: &str) {
    // SAFETY: libvips returns either a valid static/thread-local C string or null.
    let error_message = unsafe {
        let detail_ptr = vips_error_buffer();
        let detail = if detail_ptr.is_null() {
            "(no error message)".to_owned()
        } else {
            CStr::from_ptr(detail_ptr).to_string_lossy().into_owned()
        };
        format!("{prefix}: {detail}")
    };

    unsafe {
        set_error(error_out, &error_message);
        vips_error_clear();
    }
}

unsafe fn encode_tile(
    tile: *mut VipsImage,
    suffix: *const c_char,
    jpeg_quality: c_int,
    png_compression: c_int,
    buf: *mut *mut c_void,
    len: *mut size_t,
    error_out: *mut *mut c_char,
) -> c_int {
    if unsafe { is_png_suffix(suffix) } {
        let compression = png_compression.clamp(0, 9);
        // SAFETY: `tile`, `buf`, and `len` are valid pointers managed by the caller; varargs mirror the C bridge.
        let rc = unsafe {
            vips_pngsave_buffer(
                tile,
                buf,
                len,
                COMPRESSION_OPTION.as_ptr().cast::<c_char>(),
                compression,
                STRIP_OPTION.as_ptr().cast::<c_char>(),
                GBOOLEAN_TRUE,
                ptr::null::<c_void>(),
            )
        };
        if rc != 0 {
            unsafe { set_vips_error(error_out, "vips_pngsave_buffer failed") };
            return -1;
        }
        return 0;
    }

    let quality = jpeg_quality.clamp(1, 100);
    // SAFETY: `tile`, `buf`, and `len` are valid pointers managed by the caller; varargs mirror the C bridge.
    let rc = unsafe {
        vips_jpegsave_buffer(
            tile,
            buf,
            len,
            b"Q\0".as_ptr().cast::<c_char>(),
            quality,
            STRIP_OPTION.as_ptr().cast::<c_char>(),
            GBOOLEAN_TRUE,
            ptr::null::<c_void>(),
        )
    };
    if rc != 0 {
        unsafe { set_vips_error(error_out, "vips_jpegsave_buffer failed") };
        return -1;
    }

    0
}

unsafe fn tiles_from_source_impl(
    input_source: *mut VipsSource,
    subsamples: *const c_int,
    level_count: c_int,
    tile_size: c_int,
    min_level: c_int,
    max_level: c_int,
    suffix: *const c_char,
    jpeg_quality: c_int,
    png_compression: c_int,
    pad_tiles: c_int,
    background_red: f64,
    background_green: f64,
    background_blue: f64,
    callback: Option<AlaVipsTileCallback>,
    user_data: *mut c_void,
    error_out: *mut *mut c_char,
) -> c_int {
    if input_source.is_null() || subsamples.is_null() || callback.is_none() {
        unsafe { set_error(error_out, INVALID_ARGUMENT) };
        return -1;
    }
    if level_count <= 0 || tile_size <= 0 {
        unsafe { set_error(error_out, INVALID_LEVEL_SIZE) };
        return -1;
    }
    if min_level < 0 || max_level < min_level || max_level >= level_count {
        unsafe { set_error(error_out, INVALID_LEVEL_BOUNDS) };
        return -1;
    }

    // SAFETY: arguments mirror the existing C bridge contract; trailing null terminates varargs options.
    let input = unsafe {
        vips_image_new_from_source(
            input_source,
            EMPTY_STRING.as_ptr().cast::<c_char>(),
            ACCESS_OPTION.as_ptr().cast::<c_char>(),
            VIPS_ACCESS_RANDOM,
            ptr::null::<c_void>(),
        )
    };
    if input.is_null() {
        unsafe { set_vips_error(error_out, "vips_image_new_from_source failed") };
        return -1;
    }

    let callback_fn = callback.expect("callback presence already checked");
    let png = unsafe { is_png_suffix(suffix) };

    for level in min_level..=max_level {
        // SAFETY: bounds validated above and `subsamples` points to `level_count` ints.
        let subsample = unsafe { *subsamples.add(level as usize) };
        if subsample <= 0 {
            unsafe {
                g_object_unref(input.cast());
                set_error(error_out, INVALID_SUBSAMPLE);
            }
            return -1;
        }

        let mut level_image: *mut VipsImage = ptr::null_mut();
        if subsample == 1 {
            level_image = input;
            // SAFETY: `input` is a valid `GObject` reference and we balance it with an unref below.
            unsafe {
                g_object_ref(level_image.cast());
            }
        } else {
            // SAFETY: `input` is a valid image handle, `level_image` storage is valid, and null terminates options.
            let resize_rc = unsafe {
                vips_resize(
                    input,
                    &mut level_image,
                    1.0 / f64::from(subsample),
                    ptr::null::<c_void>(),
                )
            };
            if resize_rc != 0 {
                unsafe {
                    g_object_unref(input.cast());
                    set_vips_error(error_out, "vips_resize failed while creating pyramid level");
                }
                return -1;
            }
        }

        // SAFETY: `level_image` is a valid image produced above.
        let level_width = unsafe { vips_image_get_width(level_image) };
        let level_height = unsafe { vips_image_get_height(level_image) };
        let cols = (level_width + tile_size - 1) / tile_size;
        let rows = (level_height + tile_size - 1) / tile_size;

        for col in 0..cols {
            for tms_row in 0..rows {
                let left = col * tile_size;
                let mut top = level_height - (tms_row + 1) * tile_size;
                if top < 0 {
                    top = 0;
                }
                let bottom = level_height - (tms_row * tile_size);
                let remaining_width = level_width - left;
                let width = remaining_width.min(tile_size);
                let height = bottom - top;

                let mut tile: *mut VipsImage = ptr::null_mut();
                // SAFETY: crop coordinates follow the same math as the C implementation; null terminates options.
                let crop_rc = unsafe {
                    vips_crop(
                        level_image,
                        &mut tile,
                        left,
                        top,
                        width,
                        height,
                        ptr::null::<c_void>(),
                    )
                };
                if crop_rc != 0 {
                    unsafe {
                        g_object_unref(level_image.cast());
                        g_object_unref(input.cast());
                        set_vips_error(error_out, "vips_crop failed for tile extraction");
                    }
                    return -1;
                }

                let mut output_tile = tile;
                if pad_tiles != 0 && (width != tile_size || height != tile_size) {
                    let background_values = [background_red, background_green, background_blue, 0.0_f64];
                    let background_len = if png { 4 } else { 3 };
                    let background_array = unsafe {
                        vips_array_double_new(background_values.as_ptr(), background_len)
                    };
                    if background_array.is_null() {
                        unsafe {
                            g_object_unref(tile.cast());
                            g_object_unref(level_image.cast());
                            g_object_unref(input.cast());
                            set_error(error_out, "failed to allocate background colour array for tile padding");
                        }
                        return -1;
                    }
                    let mut padded_tile: *mut VipsImage = ptr::null_mut();
                    let embed_rc = unsafe {
                        vips_embed(
                            tile,
                            &mut padded_tile,
                            0,
                            tile_size - height,
                            tile_size,
                            tile_size,
                            EXTEND_OPTION.as_ptr().cast::<c_char>(),
                            VIPS_EXTEND_BACKGROUND,
                            BACKGROUND_OPTION.as_ptr().cast::<c_char>(),
                            background_array,
                            ptr::null::<c_void>(),
                        )
                    };
                    unsafe {
                        vips_area_unref(background_array.cast());
                    }
                    if embed_rc != 0 {
                        unsafe {
                            g_object_unref(tile.cast());
                            g_object_unref(level_image.cast());
                            g_object_unref(input.cast());
                            set_vips_error(error_out, "vips_embed failed while padding edge tile");
                        }
                        return -1;
                    }
                    unsafe {
                        g_object_unref(tile.cast());
                    }
                    output_tile = padded_tile;
                }

                let mut buf: *mut c_void = ptr::null_mut();
                let mut len: size_t = 0;
                let encode_rc = unsafe {
                    encode_tile(
                        output_tile,
                        suffix,
                        jpeg_quality,
                        png_compression,
                        &mut buf,
                        &mut len,
                        error_out,
                    )
                };
                // SAFETY: tile/output_tile was created by libvips above.
                unsafe {
                    g_object_unref(output_tile.cast());
                }
                if encode_rc != 0 {
                    unsafe {
                        g_object_unref(level_image.cast());
                        g_object_unref(input.cast());
                    }
                    return -1;
                }

                let content_type = if unsafe { is_png_suffix(suffix) } {
                    PNG_CONTENT_TYPE.as_ptr().cast::<c_char>()
                } else {
                    JPEG_CONTENT_TYPE.as_ptr().cast::<c_char>()
                };

                // SAFETY: callback pointer validated above; `buf`/`len` come from libvips encoder.
                let callback_rc = unsafe {
                    callback_fn(level, col, tms_row, content_type, buf.cast_const(), len, user_data)
                };
                // SAFETY: libvips allocates output buffers with GLib-compatible allocators.
                unsafe {
                    g_free(buf);
                }
                if callback_rc != 0 {
                    unsafe {
                        g_object_unref(level_image.cast());
                        g_object_unref(input.cast());
                        set_error(error_out, CALLBACK_FAILURE);
                    }
                    return -1;
                }
            }
        }

        // SAFETY: balances either the extra ref on `input` or the resize-produced image.
        unsafe {
            g_object_unref(level_image.cast());
        }
    }

    // SAFETY: balances the original `vips_image_new_from_source` reference.
    unsafe {
        g_object_unref(input.cast());
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn ala_vips_google_tms_tiles_from_source(
    input_source: *mut VipsSource,
    subsamples: *const c_int,
    level_count: c_int,
    tile_size: c_int,
    min_level: c_int,
    max_level: c_int,
    suffix: *const c_char,
    jpeg_quality: c_int,
    png_compression: c_int,
    pad_tiles: c_int,
    background_red: f64,
    background_green: f64,
    background_blue: f64,
    callback: Option<AlaVipsTileCallback>,
    user_data: *mut c_void,
    error_out: *mut *mut c_char,
) -> c_int {
    if !error_out.is_null() {
        // SAFETY: caller provided writable storage for a `char *` out-param.
        unsafe {
            *error_out = ptr::null_mut();
        }
    }

    match catch_unwind(AssertUnwindSafe(|| unsafe {
        tiles_from_source_impl(
            input_source,
            subsamples,
            level_count,
            tile_size,
            min_level,
            max_level,
            suffix,
            jpeg_quality,
            png_compression,
            pad_tiles,
            background_red,
            background_green,
            background_blue,
            callback,
            user_data,
            error_out,
        )
    })) {
        Ok(rc) => rc,
        Err(_) => {
            unsafe { set_error(error_out, PANIC_MESSAGE) };
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ala_vips_google_tms_tiles_from_file(
    input_source: *mut VipsSource,
    subsamples: *const c_int,
    level_count: c_int,
    tile_size: c_int,
    min_level: c_int,
    max_level: c_int,
    suffix: *const c_char,
    jpeg_quality: c_int,
    png_compression: c_int,
    pad_tiles: c_int,
    background_red: f64,
    background_green: f64,
    background_blue: f64,
    callback: Option<AlaVipsTileCallback>,
    user_data: *mut c_void,
    error_out: *mut *mut c_char,
) -> c_int {
    unsafe {
        ala_vips_google_tms_tiles_from_source(
            input_source,
            subsamples,
            level_count,
            tile_size,
            min_level,
            max_level,
            suffix,
            jpeg_quality,
            png_compression,
            pad_tiles,
            background_red,
            background_green,
            background_blue,
            callback,
            user_data,
            error_out,
        )
    }
}

#[no_mangle]
pub unsafe extern "C" fn ala_vips_google_tms_free_error(error_message: *mut c_char) {
    if !error_message.is_null() {
        // SAFETY: bridge error strings are allocated with `g_strdup` and must be freed with `g_free`.
        unsafe {
            g_free(error_message.cast());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_png_suffix_bytes;

    #[test]
    fn png_suffix_detection_matches_c_prefix_rule() {
        assert!(is_png_suffix_bytes(b".png"));
        assert!(is_png_suffix_bytes(b".png[Q=90]"));
        assert!(!is_png_suffix_bytes(b".jpg"));
        assert!(!is_png_suffix_bytes(b"png"));
    }
}
