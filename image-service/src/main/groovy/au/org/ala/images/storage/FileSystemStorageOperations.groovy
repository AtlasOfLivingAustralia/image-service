package au.org.ala.images.storage

import au.org.ala.images.AuditService
import au.org.ala.images.DefaultStoragePathStrategy
import au.org.ala.images.ImageInfo
import au.org.ala.images.Range
import au.org.ala.images.StoragePathStrategy
import au.org.ala.images.util.ByteSinkFactory
import au.org.ala.images.util.FileByteSinkFactory
import groovy.transform.EqualsAndHashCode
import groovy.transform.NamedVariant
import groovy.util.logging.Slf4j
import net.lingala.zip4j.io.inputstream.ZipInputStream
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.tika.config.TikaConfig
import org.apache.tika.mime.MimeType
import org.apache.tika.mime.MimeTypeException
import org.apache.tika.mime.MimeTypes

import java.nio.file.FileVisitOption
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Collectors

@EqualsAndHashCode(includes = ['basePath'])
@Slf4j
class FileSystemStorageOperations implements StorageOperations {

    String basePath
    private boolean probeFilesForImageInfo = false

    @NamedVariant
    FileSystemStorageOperations(String basePath, boolean probeFilesForImageInfo = false) {
        this.basePath = basePath
        this.probeFilesForImageInfo = probeFilesForImageInfo
    }

    boolean isProbeFilesForImageInfo() {
        return this.probeFilesForImageInfo
    }

    void setProbeFilesForImageInfo(boolean probe) {
        this.probeFilesForImageInfo = probe
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
        return new FileByteSinkFactory(new File(createTilesPathFromUUID(uuid)), false)
    }

    @Override
    void storeAnywhere(String uuid, InputStream inputStream, String relativePath, String contentType = 'image/jpeg', String contentDisposition = null, Long length = null) {
        def file = new File(storagePathStrategy().createPathFromUUID(uuid, relativePath))
        Files.createDirectories(file.parentFile.toPath())
        inputStream.withStream {
            FileUtils.copyInputStreamToFile(inputStream, file)
        }
    }

    @Override
    void migrateTo(String uuid, String contentType, StorageOperations destination) {
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
    ImageInfo originalImageInfo(String uuid) {
        def path = createOriginalPathFromUUID(uuid)
        def file = new File(path)
        def jPath = Paths.get(path)
        if (Files.exists(jPath)) {
            String contentType
            String ext
            if (probeFilesForImageInfo) {
                contentType = Files.probeContentType(jPath)
                ext = getExtensionFromMimeType(contentType)
            } else {
                contentType = 'image/jpeg'
                ext = 'jpg'
            }
            long lastModified = Files.getLastModifiedTime(jPath).toMillis()
            long length = Files.size(jPath)
            String etag = "${Long.toHexString(lastModified)}-${Long.toHexString(length)}"
            return new ImageInfo(
                    exists: true,
                    imageIdentifier: uuid,
                    length: length,
                    lastModified: new Date(lastModified),
                    contentType: contentType,
                    extension: ext,
                    etag: etag,
                    inputStreamSupplier: { file.newInputStream() }
            )
        } else {
            return new ImageInfo(exists: false, imageIdentifier: uuid)
        }
    }

    static String getExtensionFromMimeType(String mimeTypeName) {
        try {
            TikaConfig config = TikaConfig.getDefaultConfig()
            MimeTypes allTypes = config.getMimeRepository()

            MimeType mimeType = allTypes.forName(mimeTypeName)

            return mimeType.getExtension()
        } catch (MimeTypeException e) {
            // Handle cases where the MIME type name is not recognized
            log.error("Unrecognized MIME type: " + mimeTypeName, e)
            return ""
        } catch (Exception e) {
            log.error("Error getting extension for MIME type: " + mimeTypeName, e)
            return ""
        }
    }

    @Override
    ImageInfo thumbnailImageInfo(String uuid, String type) {
        def path = createThumbLargePathFromUUID(uuid, type)
        def file = new File(path)
        if (file.exists()) {
            return new ImageInfo(
                    exists: true,
                    imageIdentifier: uuid,
                    length: file.length(),
                    lastModified: new Date(file.lastModified()),
                    contentType: type == 'square' ? 'image/png' : 'image/jpeg',
                    extension: type == 'square' ? 'png' : 'jpg',
                    inputStreamSupplier: { range -> inputStream(path, range ?: Range.emptyRange(file.length())) }
            )
        } else {
            return new ImageInfo(exists: false, imageIdentifier: uuid)
        }
    }

    @Override
    ImageInfo tileImageInfo(String uuid, int x, int y, int z) {
        def path = createTilesPathFromUUID(uuid, x, y, z)
        def file = new File(path)
        if (file.exists()) {
            return new ImageInfo(
                    exists: true,
                    imageIdentifier: uuid,
                    length: file.length(),
                    lastModified: new Date(file.lastModified()),
                    contentType: 'image/png',
                    extension: 'png',
                    inputStreamSupplier: { file.newInputStream() }
            )
        } else {
            return new ImageInfo(exists: false, imageIdentifier: uuid)
        }
    }

    void clearTilesForImage(String uuid) {
        def tilesPath = createTilesPathFromUUID(uuid)
        def tilesDir = new File(tilesPath)
        if (tilesDir.exists() && tilesDir.isDirectory()) {
            try {
                FileUtils.deleteDirectory(tilesDir)
            } catch (IOException e) {
                log.error("Failed to clear tiles for image $uuid at $tilesPath", e)
            }
        }
    }

    @Override
    String toString() {
        "FilesystemStorageOperations: $basePath"
    }
}
