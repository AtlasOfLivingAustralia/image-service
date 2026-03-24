package au.org.ala.images.iiif;

import au.org.ala.images.factory.ImageLibraryFactory;
import com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Delegating IIIF image processor that uses ServiceLoader to discover and use the best available processor.
 */
public class DelegatingIiifImageProcessor implements IiifImageProcessor {

    private static final Logger log = LoggerFactory.getLogger(DelegatingIiifImageProcessor.class);

    private final IiifImageProcessor delegate;
    private final List<String> implementationNames = new ArrayList<>();

    public DelegatingIiifImageProcessor() {
        ServiceLoader<ImageLibraryFactory> loader = ServiceLoader.load(ImageLibraryFactory.class);
        List<ImageLibraryFactory> factories = new ArrayList<>();
        for (ImageLibraryFactory factory : loader) {
            if (factory.isAvailable()) {
                factories.add(factory);
            }
        }
        factories.sort((a, b) -> b.getPriority() - a.getPriority());

        IiifImageProcessor bestProcessor = null;
        IiifImageProcessor javaFallback = new JavaIiifImageProcessor();

        for (ImageLibraryFactory factory : factories) {
            implementationNames.add(factory.getImplementationName());
            IiifImageProcessor processor = factory.createIiifProcessor(javaFallback);
            if (processor != null) {
                bestProcessor = processor;
                log.info("Using IIIF processor from factory: {}", factory.getImplementationName());
                break;
            }
        }

        this.delegate = bestProcessor != null ? bestProcessor : javaFallback;
        if (bestProcessor == null) {
            log.info("No native IIIF processor available, using pure Java implementation");
        }
    }

    @Override
    public Result process(ByteSource imageBytes, Region region, Size size, Rotation rotation, Quality quality, Format format, OutputStream out) throws IOException {
        return delegate.process(imageBytes, region, size, rotation, quality, format, out);
    }

    public List<String> getImplementationNames() {
        return implementationNames;
    }
}
