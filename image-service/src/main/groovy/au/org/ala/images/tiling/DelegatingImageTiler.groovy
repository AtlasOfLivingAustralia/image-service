package au.org.ala.images.tiling

import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.tiling.ImageTilerConfig
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Delegating tiler that uses ServiceLoader to discover and use the best available tiler.
 */
@Slf4j
@CompileStatic
class DelegatingImageTiler implements IImageTiler {

    private final IImageTiler delegate
    private final List<String> implementationNames = []

    DelegatingImageTiler(CommandExecutor commandExecutor, ImageTilerConfig config, String tool = 'vips', boolean preferNative = true) {
        ServiceLoader<ImageLibraryFactory> loader = ServiceLoader.load(ImageLibraryFactory)
        List<ImageLibraryFactory> factories = loader.toList().findAll { it.available }.sort { -it.priority }

        IImageTiler bestTiler = null
        IImageTiler javaFallback = new ImageTiler(config)

        for (ImageLibraryFactory factory : factories) {
            implementationNames.add(factory.implementationName)
            if (preferNative || factory.priority == 0) {
                IImageTiler tiler = factory.createTiler(commandExecutor, config, tool, javaFallback)
                if (tiler != null) {
                    bestTiler = tiler
                    log.info("Using tiler from factory: {}", factory.implementationName)
                    break
                }
            }
        }

        this.delegate = bestTiler ?: javaFallback
        if (bestTiler == null) {
            log.info("No native tiler available, using pure Java implementation")
        }
    }

    @Override
    ImageTilerResults tileImage(InputStream imageInputStream, TilerSink tilerSink, int minLevel, int maxLevel) throws IOException, InterruptedException {
        return delegate.tileImage(imageInputStream, tilerSink, minLevel, maxLevel)
    }

    List<String> getImplementationNames() {
        return implementationNames
    }
}
