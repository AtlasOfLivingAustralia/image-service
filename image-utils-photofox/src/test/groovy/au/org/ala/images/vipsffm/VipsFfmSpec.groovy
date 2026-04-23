package au.org.ala.images.vipsffm

import app.photofox.vipsffm.VImage
import app.photofox.vipsffm.VipsOption
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.optimisation.CommandExecutor
import spock.lang.Specification

import java.awt.Color
import java.util.concurrent.Executor

class VipsFfmSpec extends Specification {

    def "VipsFfmLibraryFactoryImpl creates thumbnailer when available"() {
        given:
        def factory = new VipsFfmLibraryFactoryImpl()
        def mockExecutor = Mock(CommandExecutor)
        def mockFallback = Mock(IImageThumbnailer)

        when:
        def thumbnailer = factory.createThumbnailer(mockExecutor, [vips: "vips"], mockFallback)

        then:
        // Note: thumbnailer will be null if libvips is not installed on the system,
        // which might be the case in CI environments.
        // We'll just verify that the factory doesn't crash.
        thumbnailer != null || !factory.isAvailable([vips: "vips"])
    }

    def "VipsFfmLibraryFactoryImpl creates on-demand tiler when available"() {
        given:
        def factory = new VipsFfmLibraryFactoryImpl()
        def mockExecutor = Mock(CommandExecutor)
        def mockFallback = Mock(IOnDemandImageTiler)
        def mockPool = Mock(Executor)
        def config = new ImageTilerConfig(mockPool, mockPool)

        when:
        def tiler = factory.createOnDemandTiler(mockExecutor, config, [vips: "vips"], mockFallback)

        then:
        tiler != null || !factory.isAvailable([vips: "vips"])
    }

    def "VipsFfmLibraryFactoryImpl creates IIIF processor when available"() {
        given:
        def factory = new VipsFfmLibraryFactoryImpl()
        def mockExecutor = Mock(CommandExecutor)

        when:
        def processor = factory.createIiifProcessor(mockExecutor, [vips: "vips"], null)

        then:
        processor != null || !factory.isAvailable([vips: "vips"])
    }

    def "VipsFfmStreamingImageThumbnailer calculates options correctly"() {
        given:
        def mockFallback = Mock(IImageThumbnailer)
        def thumbnailer = new VipsFfmStreamingImageThumbnailer(mockFallback)
        def mockImage = Mock(VImage)
//        def thumbDef = new ThumbDefinition(name: "test.jpg", maximumDimension: 100, square: true, centreCrop: true)
        def thumbDef = new ThumbDefinition(100, 100, true, true, null, "test.jpg")

        when:
        thumbnailer.callVipsThumbnail(mockImage, thumbDef, 100, Color.BLACK)

        then:
        1 * mockImage.thumbnailImage(100, _ as VipsOption, _ as VipsOption) >> mockImage
    }

    def "VipsFfmStreamingImageThumbnailer calculates width-only options correctly"() {
        given:
        def mockFallback = Mock(IImageThumbnailer)
        def thumbnailer = new VipsFfmStreamingImageThumbnailer(mockFallback)
        def mockImage = Mock(VImage)
//        def thumbDef = new ThumbDefinition(name: "test.jpg", maximumDimension: 100, square: false, width: 50)
        def thumbDef = new ThumbDefinition(100, 50, false, false, null, "test.jpg")

        when:
        thumbnailer.callVipsThumbnail(mockImage, thumbDef, 100, Color.BLACK)

        then:
        1 * mockImage.thumbnailImage(50) >> mockImage
    }

}
