package au.org.ala.images.tiling;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ProfileMe {

    static final Logger log = LoggerFactory.getLogger(ProfileMe.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        List<String> fileNames = List.of("1.jpg", "2.jpg", "3.jpg", "4.jpg", "5.jpg", "6.jpg", "7.jpg", "8.jpg", "9.jpg", "10.jpg", "11.jpg", "12.jpg", "13.jpg", "14.jpg");
        File parent = new File("/home/bea18c/large-images/");
        List<File> files = fileNames.stream().map(s -> new File(parent, s)).collect(Collectors.toList());
        System.out.println("press a key...");
        System.in.read();

        log.info("profile me starting");

        var ioPool = Executors.newFixedThreadPool(1);
        var workPool = ForkJoinPool.commonPool();//Executors.newFixedThreadPool(2);
//        var ioPool = Executors.newVirtualThreadPerTaskExecutor();
//        var workPool = Executors.newVirtualThreadPerTaskExecutor();

        ImageTilerConfig config = new ImageTilerConfig(ioPool, workPool);
        Stopwatch sw = Stopwatch.createUnstarted();
        for (var file : files) {
            sw.reset().start();
            File dest = new File(parent, "tiles-"+file.getName());
            dest.mkdirs();

//            ImageTiler tiler = new ImageTiler(config);
//            ImageTiler2 tiler = new ImageTiler2(config);
            ImageTiler3 tiler = new ImageTiler3(config);
            try {
                ImageTilerResults results = tiler.tileImage(file, dest);
                if (results.getZoomLevels() == 0) {
                    System.out.printf("Tiling failed after %1$s!%n", sw.elapsed());
                } else {
                    System.out.printf("Tiling %1$s completed (%2$d zoom levels) in %3$s! %n", file.getName(), results.getZoomLevels(), sw.elapsed());
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        workPool.shutdown();
        ioPool.shutdown();
        workPool.awaitTermination(30, TimeUnit.MINUTES);
        ioPool.awaitTermination(30, TimeUnit.MINUTES);

        System.out.println("Complete...");
        System.in.read();

    }
}
