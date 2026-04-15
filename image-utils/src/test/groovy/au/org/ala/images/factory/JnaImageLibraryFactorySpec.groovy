package au.org.ala.images.factory

import au.org.ala.images.test.AbstractImageLibrarySpec
import au.org.ala.images.factory.JnaImageLibraryFactory
import au.org.ala.images.factory.ImageLibraryFactory

class JnaImageLibraryFactorySpec extends AbstractImageLibrarySpec {

    @Override
    ImageLibraryFactory getFactory() {
        return new JnaImageLibraryFactory()
    }

    @Override
    Map<String, String> getCommands() {
        return [vips: "vips"]
    }
}
