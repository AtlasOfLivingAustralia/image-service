package au.org.ala.images.ffm

import au.org.ala.images.test.AbstractImageLibrarySpec
import au.org.ala.images.factory.ImageLibraryFactory

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
}
