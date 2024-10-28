package au.org.ala.images

import au.org.ala.images.util.ByteSinkFactory
import au.org.ala.images.util.FileByteSinkFactory
import groovy.transform.EqualsAndHashCode
import net.lingala.zip4j.io.inputstream.ZipInputStream
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils

import java.nio.file.FileVisitOption
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Collectors

@EqualsAndHashCode(includes = ['basePath'])
class FileSystemStorageLocation extends StorageLocation {

    String basePath

    static constraints = {
    }

    static mapping = {
        cache true
    }

    @Override
    boolean isSupportsRedirect() {
        return false
    }

    @Override
    URI redirectLocation(String path) {
        throw new UnsupportedOperationException("FS never redirects")
    }

    @Override
    boolean verifySettings() {
        def baseDirFile = new File(basePath)
        boolean isDir = (baseDirFile.exists() || baseDirFile.mkdirs()) && baseDirFile.isDirectory()
        if (baseDirFile.exists()) {
            if (!baseDirFile.isDirectory()) {
                log.warn("FS {} exists but is not a directory", baseDirFile)
                return false
            }
        }
        else {
            if (!baseDirFile.mkdirs()) {
                log.warn("FS {} didn't exist but the application can't create it", baseDirFile)
                return false
            }
        }
        boolean canRead = isDir && baseDirFile.canRead()
        if (!canRead) {
            log.warn("FS {} exists but the application can't read from it", baseDirFile)
            return false
        }
        boolean canWrite = canRead && baseDirFile.canWrite()
        if (!canWrite) {
            log.warn("FS {} exists but the application can't write to it", baseDirFile)
            return false
        }
        return true
    }

    @Override
    void store(String uuid, InputStream inputStream, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        String path = createOriginalPathFromUUID(uuid)

        File f = new File(path)
        f.parentFile.mkdirs()
        inputStream.withStream {
            FileUtils.copyInputStreamToFile(inputStream, f)
        }
    }

    @Override
    byte[] retrieve(String uuid) {
        def imageFile = createOriginalPathFromUUID(uuid)
        def file = new File(imageFile)
        if (file.exists()) {
            return file.bytes
        } else {
            throw new FileNotFoundException("FS path $imageFile")
        }
    }

    @Override
    InputStream inputStream(String path, Range range) throws FileNotFoundException {
        def file = new File(path)
        def is = file.newInputStream()
        return range?.wrapInputStream(is) ?: is
    }

    @Override
    boolean stored(String uuid) {
        return new File(createOriginalPathFromUUID(uuid))?.exists() ?: false
    }

    @Override
    boolean thumbnailExists(String uuid, String type) {
        return new File(createThumbLargePathFromUUID(uuid, type))?.exists() ?: false
    }

    @Override
    boolean tileExists(String uuid, int x, int y, int z) {
        return new File(createTilesPathFromUUID(uuid, x, y, z))?.exists() ?: false
    }

    @Override
    void storeTileZipInputStream(String uuid, String zipFileName, String contentType, long length = 0, ZipInputStream zipInputStream) {
        def path = createTilesPathFromUUID(uuid)
        Files.createDirectories(Paths.get(path))
        FileUtils.copyInputStreamToFile(zipInputStream, new File(FilenameUtils.normalize(path + '/' + zipFileName)))
    }

    @Override
    long consumedSpace(String uuid) {
        def original = new File(createOriginalPathFromUUID(uuid))
        if (original && original.exists()) {
            return FileUtils.sizeOfDirectory(original.parentFile)
        }
        return 0
    }

    @Override
    boolean deleteStored(String uuid) {
        if (uuid) {
            File f = new File(createOriginalPathFromUUID(uuid))
            if (f && f.exists()) {
                FileUtils.deleteQuietly(f.parentFile)
                AuditService.submitLog(uuid, "Image deleted from store", "N/A")
                return true
            }
        }
        return false
    }

    StoragePathStrategy storagePathStrategy() {
        new DefaultStoragePathStrategy(basePath, false, true) // this probably doesn't work on winders...
    }

    @Override
    ByteSinkFactory thumbnailByteSinkFactory(String uuid) {
        return new FileByteSinkFactory(new File(storagePathStrategy().createPathFromUUID(uuid,'')))
    }

    @Override
    ByteSinkFactory tilerByteSinkFactory(String uuid) {
        return new FileByteSinkFactory(new File(createTilesPathFromUUID(uuid)))
    }

    @Override
    protected storeAnywhere(String uuid, InputStream inputStream, String relativePath, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        def file = new File(storagePathStrategy().createPathFromUUID(uuid, relativePath))
        Files.createDirectories(file.parentFile.toPath())
        inputStream.withStream {
            FileUtils.copyInputStreamToFile(inputStream, file)
        }
    }

    @Override
    void migrateTo(String uuid, String contentType, StorageLocation destination) {
        def basePath = createBasePathFromUUID(uuid)
        Files.walk(Paths.get(basePath), FileVisitOption.FOLLOW_LINKS).map { Path path ->
            if (Files.isRegularFile(path)) {
                destination.storeAnywhere(uuid, new BufferedInputStream(Files.newInputStream(path)), path.toString() - basePath, contentType, null, Files.size(path))
            }
        }.collect(Collectors.toList())
    }

    @Override
    long storedLength(String path) {
        def file = new File(path)
        if (file.exists()) {
            return file.length()
        } else {
            throw new FileNotFoundException("FS location $path")
        }
    }

    @Override
    String toString() {
        "Filesystem($id): $basePath"
    }
}
