package au.org.ala.images

import com.google.common.io.ByteSource
import com.google.common.io.Files
import com.google.common.base.Optional
import groovy.util.logging.Slf4j

@Slf4j
class CloseableByteSource extends ByteSource implements AutoCloseable {
    private File file

    @Delegate
    private ByteSource delegate

    CloseableByteSource(File file) {
        this.file = file
        this.delegate = Files.asByteSource(file)
    }

    CloseableByteSource(byte [] bytes) {
        this.delegate = ByteSource.wrap(bytes)
    }

    Optional<Long> sizeIfKnown() {
        return delegate.sizeIfKnown()
    }

    @Override
    long size() throws IOException {
        return delegate.size()
    }

    @Override
    void close() throws Exception {
        if (file) {
            if (!file.delete()) {
                log.warn("Failed to delete temporary file: {}", file)
            }
        }
    }
}
