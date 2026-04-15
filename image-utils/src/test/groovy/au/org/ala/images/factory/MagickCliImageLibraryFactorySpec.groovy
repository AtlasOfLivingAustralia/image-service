package au.org.ala.images.factory

import au.org.ala.images.test.AbstractImageLibrarySpec
import au.org.ala.images.factory.MagickCliImageLibraryFactory
import au.org.ala.images.factory.ImageLibraryFactory

class MagickCliImageLibraryFactorySpec extends AbstractImageLibrarySpec {

    @Override
    ImageLibraryFactory getFactory() {
        return new MagickCliImageLibraryFactory()
    }

    @Override
    Map<String, String> getCommands() {
        return [magick: "magick"]
    }
}
