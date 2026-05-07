package au.org.ala.images.vipsffm

import au.org.ala.images.test.AbstractImageLibrarySpec
import au.org.ala.images.factory.ImageLibraryFactory

class VipsFfmLibraryFactorySpec extends AbstractImageLibrarySpec {

    @Override
    ImageLibraryFactory getFactory() {
        return new VipsFfmLibraryFactoryImpl()
    }

    @Override
    Map<String, String> getCommands() {
        return [:]
    }

    def "VipsFfmLibraryFactoryImpl provides metadata"() {
        given:
        def factory = new VipsFfmLibraryFactoryImpl()

        expect:
        factory.getImplementationName() != null
        factory.getPriority() == 25
    }
}
