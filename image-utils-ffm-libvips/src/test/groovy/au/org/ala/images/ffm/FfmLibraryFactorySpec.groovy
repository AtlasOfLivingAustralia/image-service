package au.org.ala.images.ffm

import au.org.ala.images.test.AbstractImageLibrarySpec
import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.tiling.FfmNativeDzStreamingImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TileFormat
import spock.lang.IgnoreIf

class FfmLibraryFactorySpec extends AbstractImageLibrarySpec {

    @Override
    ImageLibraryFactory getFactory() {
        return new FfmLibraryFactoryImpl()
    }

    @Override
    Map<String, String> getCommands() {
        return [:]
    }

    def "FfmLibraryFactoryImpl provides metadata"() {
        given:
        def factory = new FfmLibraryFactoryImpl()

        expect:
        factory.getImplementationName() != null
        factory.getPriority() == 20
    }

    @IgnoreIf({ !NativeLibraryDetectorFFM.vipsAvailable })
    def "native dz ffm flag selects FfmNativeDzStreamingImageTiler"() {
        given:
        def factory = new FfmLibraryFactoryImpl()
        def config = new ImageTilerConfig(Runnable::run, Runnable::run, 256, 6, TileFormat.JPEG)

        when:
        def tiler = factory.createTiler(null, config, [("nativeDzTilerFfmEnabled"): "true"], null)

        then:
        tiler instanceof FfmNativeDzStreamingImageTiler
    }
}
