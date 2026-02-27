package au.org.ala.images.util;

import com.google.common.io.ByteSink;
import com.google.common.io.MoreFiles;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileByteSinkFactory implements ByteSinkFactory {

    public static final Logger log = LoggerFactory.getLogger(FileByteSinkFactory.class);

    final File parentDir;
    final boolean cleanParentDir;

    public FileByteSinkFactory(File parentDir) {
        this(parentDir, true);
    }

    public FileByteSinkFactory(File parentDir, boolean cleanParentDir) {
        this.parentDir = parentDir;
        this.cleanParentDir = cleanParentDir;
    }


    @Override
    public void prepare() throws IOException {
        if (parentDir.exists() && cleanParentDir) {
            FileUtils.deleteDirectory(parentDir);
        }
        parentDir.mkdirs();
    }

    public ByteSink getByteSinkForNames(String... names) {
        Path path = Paths.get(parentDir.getAbsolutePath(), names);
        File parent = path.getParent().toFile();
        if (!parent.exists() && !parent.mkdirs()) {
            log.error("Unable to create directories for {}", path);
        }
        return MoreFiles.asByteSink(path);
    }
}
