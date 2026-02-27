package au.org.ala.images.thumb;

import au.org.ala.images.util.ByteSinkFactory;
import au.org.ala.images.util.FileByteSinkFactory;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import org.apache.commons.io.output.NullOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;

public class LocalThumbnailer {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            usage();
            System.exit(0);
        }

        for (String filename : args) {
            processFile(filename);
        }

        System.out.println("Complete...");
        System.in.read();
    }

    private static void processFile(String filename) {
        File f = new File(filename);
        if (!f.exists()) {
            error(String.format("Invalid file name: %s", filename));
        }

        File dest = new File(f.getParentFile().getAbsolutePath(), f.getName() + "-thumbs");
        dest.mkdirs();

        ImageThumbnailer thumbnailer = new ImageThumbnailer();
        try {
            long start = System.currentTimeMillis();
            var byteSinkFactory = new FileByteSinkFactory(dest);
//            var byteSource = ByteSource.wrap(Files.asByteSource(f).read());
            var byteSource = Files.asByteSource(f);
            var results = thumbnailer.generateThumbnailsNoIntermediateEncode(byteSource, byteSinkFactory, ThumbDefinition.DEFAULT_THUMBS);
            long end = System.currentTimeMillis();
            if (results.isEmpty()) {
                System.out.println("Thumbnails failed!");
            } else {
                System.out.printf("Thumbnails completed for %s (%d thumbs) in %s%n", filename, results.size(), Duration.ofMillis(end - start));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void usage() {
        System.out.println("LocalThumbnailer <filename>");
    }

    private static void error(String message) {
        System.err.println(message);
        System.exit(-1);
    }

    final static ByteSinkFactory NULL_BYTE_SINK_FACTORY = new ByteSinkFactory() {
        @Override
        public void prepare() throws IOException {

        }

        @Override
        public ByteSink getByteSinkForNames(String... names) {
            return new ByteSink() {
                @Override
                public OutputStream openStream() throws IOException {
                    return NullOutputStream.NULL_OUTPUT_STREAM;
                }
            };
        }
    };

}
