package au.org.ala.images.tiling;

import java.awt.*;
import java.io.File;

public class LocalTiler {

    public static void main(String[] args) {
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

        ImageTilerConfig config = new ImageTilerConfig();
        // config.setTileFormat(TileFormat.PNG);
        config.setTileBackgroundColor(Color.pink);
        ImageTiler tiler = new ImageTiler(config);
        try {
            ImageTilerResults results = tiler.tileImage(f, dest);
            if (results.getZoomLevels() == 0) {
                System.out.println("Tiling failed!");
            } else {
                System.out.println(String.format("Tiling completed (%d zoom levels)", results.getZoomLevels()));
            }
        } catch (Exception ex) {
            System.out.println("Here!");
            ex.printStackTrace();
        }

    }

    private static void usage() {
        System.out.println("LocalTiler <filename>");
    }

    private static void error(String message) {
        System.err.println(message);
        System.exit(-1);
    }
}
