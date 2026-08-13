package au.org.ala.images.factory

import au.org.ala.images.test.AbstractImageLibrarySpec
import au.org.ala.images.factory.VipsCliImageLibraryFactory
import au.org.ala.images.factory.ImageLibraryFactory

class VipsCliImageLibraryFactorySpec extends AbstractImageLibrarySpec {

    @Override
    ImageLibraryFactory getFactory() {
        return new VipsCliImageLibraryFactory()
    }

    @Override
    Map<String, String> getCommands() {
        return [vips: "vips"]
    }
}
