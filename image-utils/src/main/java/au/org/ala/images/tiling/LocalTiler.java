package au.org.ala.images.tiling;

import au.org.ala.images.util.FileByteSinkFactory;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class LocalTiler {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            usage();
            System.exit(0);
        }

        String filename = args[0];
        File f = new File(filename);
        if (!f.exists()) {
            error(String.format("Invalid file name: %s", filename));
        }

        File dest = new File(f.getParentFile().getAbsolutePath() + "/tiles");
        dest.mkdirs();

        var ioPool = Executors.newFixedThreadPool(2);
        var workPool = ForkJoinPool.commonPool();//Executors.newFixedThreadPool(2);
//        var ioPool = Executors.newVirtualThreadPerTaskExecutor();
//        var workPool = Executors.newVirtualThreadPerTaskExecutor();

        ImageTilerConfig config = new ImageTilerConfig(ioPool, workPool);
        IImageTiler tiler = new ImageTiler4(config);
//        ImageTiler tiler = new ImageTiler(config);
        try (FileInputStream fis = new FileInputStream(f)) {
            var sink = new TilerSink.PathBasedTilerSink(new FileByteSinkFactory(dest));
            long start = System.nanoTime();
            ImageTilerResults results = tiler.tileImage(fis, sink);
            if (results.getZoomLevels() == 0) {
                System.out.println("Tiling failed!");
            } else {
                long end = System.nanoTime();
                System.out.println(String.format("Tiling completed (%d zoom levels) in %s", results.getZoomLevels(), Duration.ofNanos(end - start)));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        workPool.shutdown();
        ioPool.shutdown();
        workPool.awaitTermination(30, TimeUnit.MINUTES);
        ioPool.awaitTermination(30, TimeUnit.MINUTES);

    }

    private static void usage() {
        System.out.println("LocalTiler <filename>");
    }

    private static void error(String message) {
        System.err.println(message);
        System.exit(-1);
    }
}
