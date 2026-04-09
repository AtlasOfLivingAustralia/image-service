package au.org.ala.images.jna

import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ImageThumbnailer
import au.org.ala.images.thumb.JnaStreamingImageThumbnailer
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.ImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.JnaStreamingImageTiler
import spock.lang.Specification

class JnaImageLibraryFactorySpec extends Specification {

    def "createThumbnailer uses provided fallback when non-null"() {
        given:
        def factory = new JnaImageLibraryFactory()
        def commandExecutor = Mock(CommandExecutor)
        def fallback = Mock(IImageThumbnailer)
        
        // Ensure factory is "available" for the test if possible, 
        // or mock isAvailable if we can (it's a public method)
        def factorySpy = Spy(JnaImageLibraryFactory)
        factorySpy.isAvailable() >> true

        when:
        def thumbnailer = factorySpy.createThumbnailer(commandExecutor, "vips", fallback)

        then:
        thumbnailer instanceof au.org.ala.images.thumb.JnaStreamingImageThumbnailer
        thumbnailer.fallbackThumbnailer == fallback
    }

    def "createTiler uses provided fallback when non-null"() {
        given:
        def factory = new JnaImageLibraryFactory()
        def commandExecutor = Mock(CommandExecutor)
        def fallback = Mock(IImageTiler)
        def config = new ImageTilerConfig()
        
        def factorySpy = Spy(JnaImageLibraryFactory)
        factorySpy.isAvailable() >> true

        when:
        def tiler = factorySpy.createTiler(commandExecutor, config, "vips", fallback)

        then:
        tiler instanceof au.org.ala.images.tiling.JnaStreamingImageTiler
        tiler.fallbackTiler == fallback
    }

    def "createThumbnailer creates default fallback when null"() {
        given:
        def factory = new JnaImageLibraryFactory()
        def commandExecutor = Mock(CommandExecutor)
        commandExecutor.isInstalled("vips") >> false
        
        def factorySpy = Spy(JnaImageLibraryFactory)
        factorySpy.isAvailable() >> true

        when:
        def thumbnailer = factorySpy.createThumbnailer(commandExecutor, "vips", null)

        then:
        thumbnailer instanceof au.org.ala.images.thumb.JnaStreamingImageThumbnailer
        thumbnailer.fallbackThumbnailer instanceof au.org.ala.images.thumb.ImageThumbnailer
    }
}
