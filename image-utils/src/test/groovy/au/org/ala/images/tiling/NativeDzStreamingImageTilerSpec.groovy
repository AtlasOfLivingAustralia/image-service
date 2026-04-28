package au.org.ala.images.tiling

import au.org.ala.images.jna.NativeDzTilerLibrary
import au.org.ala.images.jna.VipsLibrary
import com.sun.jna.Pointer
import com.sun.jna.ptr.PointerByReference
import spock.lang.Specification

class NativeDzStreamingImageTilerSpec extends Specification {

    def "uses source-based native entrypoint when stream is seekable"() {
        given:
        def fallback = Mock(IImageTiler)
        def nativeLib = Mock(NativeDzTilerLibrary)
        def vips = Mock(VipsLibrary)
        def config = new ImageTilerConfig(Runnable::run, Runnable::run, 256, 6, TileFormat.JPEG)
        def tiler = new NativeDzStreamingImageTiler(fallback, config, nativeLib, vips)

        def input = new ByteArrayInputStream(new byte[64])
        def sink = Mock(TilerSink)

        Pointer source = Pointer.createConstant(0x11)
        Pointer image = Pointer.createConstant(0x22)

        vips.vips_source_custom_new() >> source
        2 * vips.g_signal_connect_data(*_) >> 1L
        vips.vips_image_new_from_source(source, "", "access", 1, null) >> image
        vips.vips_image_get_width(image) >> 512
        vips.vips_image_get_height(image) >> 512

        when:
        def result = tiler.tileImage(input, sink, 0, 10)

        then:
        result.success
        result.zoomLevels > 0
        1 * nativeLib.ala_vips_google_tms_tiles_from_source(
                source,
                _ as int[],
                _ as int,
                256,
                0,
                _ as int,
                ".jpg",
                82,
                6,
                true,
                221d,
                221d,
                221d,
                _ as NativeDzTilerLibrary.TileCallback,
                Pointer.NULL,
                _ as PointerByReference
        ) >> 0
        0 * fallback.tileImage(_, _, _, _)
        1 * vips.g_object_unref(image)
        1 * vips.g_object_unref(source)
    }

    def "falls back immediately when stream is not seekable"() {
        given:
        def fallback = Mock(IImageTiler)
        def nativeLib = Mock(NativeDzTilerLibrary)
        def vips = Mock(VipsLibrary)
        def config = new ImageTilerConfig(Runnable::run, Runnable::run, 256, 6, TileFormat.JPEG)
        def tiler = new NativeDzStreamingImageTiler(fallback, config, nativeLib, vips)

        def input = new InputStream() {
            @Override
            int read() {
                return -1
            }

            @Override
            boolean markSupported() {
                return false
            }
        }
        def sink = Mock(TilerSink)
        def fallbackResult = new ImageTilerResults(true, 3)

        Pointer source = Pointer.createConstant(0x33)
        vips.vips_source_custom_new() >> source
        2 * vips.g_signal_connect_data(*_) >> 1L

        when:
        def result = tiler.tileImage(input, sink, 0, 1)

        then:
        result.is(fallbackResult)
        1 * fallback.tileImage(input, sink, 0, 1) >> fallbackResult
        0 * nativeLib.ala_vips_google_tms_tiles_from_source(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)
        1 * vips.g_object_unref(source)
    }
}

