package au.org.ala.images.thumb

import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.optimisation.CommandExecutor
import com.google.common.io.ByteSource
import au.org.ala.images.util.ByteSinkFactory
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Delegating thumbnailer that uses ServiceLoader to discover and use the best available thumbnailer.
 * Best is defined by the factory priority and whether the implementation is available.
 */
@Slf4j
@CompileStatic
class DelegatingImageThumbnailer implements IImageThumbnailer {

    private final IImageThumbnailer delegate
    private final List<String> implementationNames = []

    DelegatingImageThumbnailer(CommandExecutor commandExecutor, IImageThumbnailer javaFallback, String tool = 'vips', boolean preferNative = true) {
        ServiceLoader<ImageLibraryFactory> loader = ServiceLoader.load(ImageLibraryFactory)
        List<ImageLibraryFactory> factories = loader.toList().findAll { it.available }.sort { -it.priority }

        IImageThumbnailer bestThumbnailer = null

        for (ImageLibraryFactory factory : factories) {
            implementationNames.add(factory.implementationName)
            if (preferNative || factory.priority == 0) {
                IImageThumbnailer thumbnailer = factory.createThumbnailer(commandExecutor, tool, javaFallback)
                if (thumbnailer != null) {
                    bestThumbnailer = thumbnailer
                    log.info("Using thumbnailer from factory: {}", factory.implementationName)
                    break
                }
            }
        }

        this.delegate = bestThumbnailer ?: javaFallback
        if (bestThumbnailer == null) {
            log.info("No native thumbnailer available, using pure Java implementation")
        }
    }

    @Override
    List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
        return delegate.generateThumbnails(imageBytes, byteSinkFactory, thumbDefs)
    }

    List<String> getImplementationNames() {
        return implementationNames
    }
}
