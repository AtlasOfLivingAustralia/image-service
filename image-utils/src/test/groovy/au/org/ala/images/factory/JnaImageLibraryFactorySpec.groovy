package au.org.ala.images.factory

import au.org.ala.images.factory.JnaImageLibraryFactory
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
        def commands = [vips: "vips"]
        
        // Ensure factory is "available" for the test if possible, 
        // or mock isAvailable if we can (it's a public method)
        def factorySpy = Spy(JnaImageLibraryFactory)
        factorySpy.isAvailable(commands) >> true

        when:
        def thumbnailer = factorySpy.createThumbnailer(commandExecutor, commands, fallback)

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
        def commands = [vips: "vips"]
        
        def factorySpy = Spy(JnaImageLibraryFactory)
        factorySpy.isAvailable(commands) >> true

        when:
        def tiler = factorySpy.createTiler(commandExecutor, config, commands, fallback)

        then:
        tiler instanceof au.org.ala.images.tiling.JnaStreamingImageTiler
        tiler.fallbackTiler == fallback
    }

    def "createThumbnailer with null fallback"() {
        given:
        def factory = new JnaImageLibraryFactory()
        def commandExecutor = Mock(CommandExecutor)
        def commands = [vips: "vips"]
        
        def factorySpy = Spy(JnaImageLibraryFactory)
        factorySpy.isAvailable(commands) >> true

        when:
        def thumbnailer = factorySpy.createThumbnailer(commandExecutor, commands, null)

        then:
        thumbnailer instanceof au.org.ala.images.thumb.JnaStreamingImageThumbnailer
        thumbnailer.fallbackThumbnailer == null
    }
}
