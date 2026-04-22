#include <glib.h>
#include <vips/vips.h>

#include <math.h>
#include <stdio.h>
#include <string.h>

typedef int (*ala_vips_tile_callback)(
        int level,
        int x,
        int y,
        const char *content_type,
        const void *data,
        size_t length,
        void *user_data);

static void ala_set_error(char **error_out, const char *message) {
    if (error_out != NULL) {
        *error_out = g_strdup(message != NULL ? message : "unknown error");
    }
}

static void ala_set_vips_error(char **error_out, const char *prefix) {
    const char *vips_error = vips_error_buffer();
    char *combined = g_strdup_printf("%s: %s", prefix, vips_error != NULL ? vips_error : "(no error message)");
    ala_set_error(error_out, combined);
    g_free(combined);
    vips_error_clear();
}

static int ala_encode_tile(
        VipsImage *tile,
        const char *suffix,
        int jpeg_quality,
        int png_compression,
        void **buf,
        size_t *len,
        char **error_out) {
    gboolean png = suffix != NULL && g_str_has_prefix(suffix, ".png");

    if (png) {
        int compression = png_compression;
        if (compression < 0) {
            compression = 0;
        }
        if (compression > 9) {
            compression = 9;
        }

        if (vips_pngsave_buffer(tile, buf, len,
                "compression", compression,
                "strip", TRUE,
                NULL) != 0) {
            ala_set_vips_error(error_out, "vips_pngsave_buffer failed");
            return -1;
        }
    } else {
        int quality = jpeg_quality;
        if (quality < 1) {
            quality = 1;
        }
        if (quality > 100) {
            quality = 100;
        }

        if (vips_jpegsave_buffer(tile, buf, len,
                "Q", quality,
                "strip", TRUE,
                NULL) != 0) {
            ala_set_vips_error(error_out, "vips_jpegsave_buffer failed");
            return -1;
        }
    }

    return 0;
}

int ala_vips_google_tms_tiles_from_source(
        VipsSource *input_source,
        const int *subsamples,
        int level_count,
        int tile_size,
        int min_level,
        int max_level,
        const char *suffix,
        int jpeg_quality,
        int png_compression,
        ala_vips_tile_callback callback,
        void *user_data,
        char **error_out) {

    if (error_out != NULL) {
        *error_out = NULL;
    }

    if (input_source == NULL || subsamples == NULL || callback == NULL) {
        ala_set_error(error_out, "invalid argument: input_source, subsamples and callback are required");
        return -1;
    }
    if (level_count <= 0 || tile_size <= 0) {
        ala_set_error(error_out, "invalid argument: level_count and tile_size must be > 0");
        return -1;
    }
    if (min_level < 0 || max_level < min_level || max_level >= level_count) {
        ala_set_error(error_out, "invalid argument: level bounds are outside available pyramid levels");
        return -1;
    }

    VipsImage *input = vips_image_new_from_source(
            input_source,
            "",
            "access", VIPS_ACCESS_RANDOM,
            NULL);
    if (input == NULL) {
        ala_set_vips_error(error_out, "vips_image_new_from_source failed");
        return -1;
    }

    for (int level = min_level; level <= max_level; level++) {
        int subsample = subsamples[level];
        if (subsample <= 0) {
            g_object_unref(input);
            ala_set_error(error_out, "invalid subsample value in pyramid");
            return -1;
        }

        VipsImage *level_image = NULL;
        if (subsample == 1) {
            level_image = input;
            g_object_ref(level_image);
        } else {
            if (vips_resize(input, &level_image, 1.0 / (double) subsample, NULL) != 0) {
                g_object_unref(input);
                ala_set_vips_error(error_out, "vips_resize failed while creating pyramid level");
                return -1;
            }
        }

        int level_width = vips_image_get_width(level_image);
        int level_height = vips_image_get_height(level_image);
        int cols = (level_width + tile_size - 1) / tile_size;
        int rows = (level_height + tile_size - 1) / tile_size;

        for (int col = 0; col < cols; col++) {
            for (int tms_row = 0; tms_row < rows; tms_row++) {
                int left = col * tile_size;
                int top = level_height - (tms_row + 1) * tile_size;
                if (top < 0) {
                    top = 0;
                }
                int bottom = level_height - (tms_row * tile_size);
                int width = tile_size;
                int remaining_width = level_width - left;
                if (remaining_width < width) {
                    width = remaining_width;
                }
                int height = bottom - top;

                VipsImage *tile = NULL;
                if (vips_crop(level_image, &tile, left, top, width, height, NULL) != 0) {
                    g_object_unref(level_image);
                    g_object_unref(input);
                    ala_set_vips_error(error_out, "vips_crop failed for tile extraction");
                    return -1;
                }

                void *buf = NULL;
                size_t len = 0;
                int encode_rc = ala_encode_tile(tile, suffix, jpeg_quality, png_compression, &buf, &len, error_out);
                g_object_unref(tile);
                if (encode_rc != 0) {
                    g_object_unref(level_image);
                    g_object_unref(input);
                    return -1;
                }

                const char *content_type = (suffix != NULL && g_str_has_prefix(suffix, ".png"))
                        ? "image/png"
                        : "image/jpeg";

                int callback_rc = callback(level, col, tms_row, content_type, buf, len, user_data);
                g_free(buf);
                if (callback_rc != 0) {
                    g_object_unref(level_image);
                    g_object_unref(input);
                    ala_set_error(error_out, "tile callback returned failure");
                    return -1;
                }
            }
        }

        g_object_unref(level_image);
    }

    g_object_unref(input);
    return 0;
}

int ala_vips_google_tms_tiles_from_file(
        VipsSource *input_source,
        const int *subsamples,
        int level_count,
        int tile_size,
        int min_level,
        int max_level,
        const char *suffix,
        int jpeg_quality,
        int png_compression,
        ala_vips_tile_callback callback,
        void *user_data,
        char **error_out) {
    return ala_vips_google_tms_tiles_from_source(
            input_source,
            subsamples,
            level_count,
            tile_size,
            min_level,
            max_level,
            suffix,
            jpeg_quality,
            png_compression,
            callback,
            user_data,
            error_out);
}

void ala_vips_google_tms_free_error(char *error_message) {
    if (error_message != NULL) {
        g_free(error_message);
    }
}

