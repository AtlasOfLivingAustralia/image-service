package au.org.ala.images.factory

import au.org.ala.images.test.AbstractImageLibrarySpec

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
