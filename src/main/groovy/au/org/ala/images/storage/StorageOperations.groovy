package au.org.ala.images.storage

import au.org.ala.images.ImageInfo
import au.org.ala.images.Range
import au.org.ala.images.StoragePathStrategy
import au.org.ala.images.util.ByteSinkFactory
import net.lingala.zip4j.io.inputstream.ZipInputStream

interface StorageOperations {

    abstract boolean isSupportsRedirect()

    abstract URI redirectLocation(String path)

    default URI originalRedirectLocation(String uuid) {
        redirectLocation(storagePathStrategy().createOriginalPathFromUUID(uuid))
    }

    default URI thumbnailRedirectLocation(String uuid) {
        redirectLocation(storagePathStrategy().createThumbPathFromUUID(uuid))
    }

    default URI thumbnailTypeRedirectLocation(String uuid, String type) {
        redirectLocation(storagePathStrategy().createThumbLargePathFromUUID(uuid, type))
    }

    default URI tileRedirectLocation(String uuid, int x, int y, int z) {
        redirectLocation(storagePathStrategy().createTilesPathFromUUID(uuid, x, y, z))
    }

    /**
     * Store a byte array as the original file for the given uuid in this StorageLocation.
     *
     * @param uuid The uuid
     * @param bytes The bytes
     * @param contentType The content type of the bytes
     * @param contentDisposition The content disposition if any
     */
    default void store(String uuid, byte[] bytes, String contentType = 'image/jpeg', String contentDisposition = null) {
        store(uuid, new ByteArrayInputStream(bytes), contentType, contentDisposition, bytes.length)
    }

    /**
     * Store an InputStream as the original file for the given uuid in this StorageLocation.
     *
     * @param uuid The uuid
     * @param stream The input stream to read, will be closed by this method
     * @param contentType The content type of the bytes
     * @param contentDisposition The content disposition if any
     * @param length The length of the input stream content if known
     */
    void store(String uuid, InputStream stream, String contentType/* = 'image/jpeg'*/, String contentDisposition/* = null*/, Long length/* = null*/)

    /**
     * Retrieve the original file contents as bytes
     * @param uuid The uuid
     * @return The bytes the comprise the original file
     */
    abstract byte[] retrieve(String uuid)

    /**
     * Retrieve an InputStream for the original file
     * @param uuid The uuid
     * @param range The byte range to restrict this to, if any.
     * @return The input stream for the original file and range
     */
    default InputStream originalInputStream(String uuid, Range range) throws FileNotFoundException {
        inputStream(createOriginalPathFromUUID(uuid), range)
    }

    /**
     * Retrieve an InputStream for the thumbnail file
     * @param uuid The uuid
     * @param range The byte range to restrict this to, if any.
     * @return The input stream for the thumbnail file and range
     */
    default InputStream thumbnailInputStream(String uuid, Range range) throws FileNotFoundException {
        inputStream(createThumbPathFromUUID(uuid), range)
    }

    /**
     * Retrieve an InputStream for a thumbnail type file
     * @param uuid The uuid
     * @param range The byte range to restrict this to, if any.
     * @return The input stream for the thumbnail file and range
     */
    default InputStream thumbnailTypeInputStream(String uuid, String type, Range range) throws FileNotFoundException {
        inputStream(createThumbLargePathFromUUID(uuid, type), range)
    }

    default InputStream tileInputStream(String uuid, int x, int y, int z, Range range) throws FileNotFoundException {
        inputStream(createTilesPathFromUUID(uuid, x, y, z), range)
    }

    abstract InputStream inputStream(String path, Range range) throws FileNotFoundException

    default long originalStoredLength(String uuid) throws FileNotFoundException {
        storedLength(createOriginalPathFromUUID(uuid))
    }

    default long thumbnailStoredLength(String uuid) throws FileNotFoundException {
        storedLength(createThumbPathFromUUID(uuid))
    }

    default long thumbnailTypeStoredLength(String uuid, String type) throws FileNotFoundException {
        storedLength(createThumbLargePathFromUUID(uuid, type))
    }

    default long tileStoredLength(String uuid, int x, int y, int z) throws FileNotFoundException {
        storedLength(createTilesPathFromUUID(uuid, x, y, z))
    }

    abstract long storedLength(String path) throws FileNotFoundException

    abstract boolean stored(String uuid)

    abstract void storeTileZipInputStream(String uuid, String zipFileName, String contentType, long length/* = 0*/, ZipInputStream zipInputStream)

    abstract long consumedSpace(String uuid)

    abstract boolean deleteStored(String uuid)

    default String createBasePathFromUUID(String uuid) {
        storagePathStrategy().createPathFromUUID(uuid, '')
    }

    default String createThumbPathFromUUID(String uuid) {
        storagePathStrategy().createThumbPathFromUUID(uuid)
    }

    default String createThumbLargePathFromUUID(String uuid, String type) {
        storagePathStrategy().createThumbLargePathFromUUID(uuid, type)
    }

    default String createTilesPathFromUUID(String uuid) {
        storagePathStrategy().createTilesPathFromUUID(uuid)
    }

    default String createTilesPathFromUUID(String uuid, int x, int y, int z) {
        storagePathStrategy().createTilesPathFromUUID(uuid, x, y, z)
    }

    default String createOriginalPathFromUUID(String uuid) {
        storagePathStrategy().createOriginalPathFromUUID(uuid)
    }

    abstract StoragePathStrategy storagePathStrategy()

    abstract ByteSinkFactory thumbnailByteSinkFactory(String uuid)

    abstract ByteSinkFactory tilerByteSinkFactory(String uuid)

    abstract void storeAnywhere(String uuid, InputStream inputStream, String relativePath, String contentType/* = 'image/jpeg'*/, String contentDisposition/* = null*/, Long length/* = null*/)

    abstract void migrateTo(String uuid, String contentType, StorageOperations destination)

    abstract boolean verifySettings()

    abstract boolean thumbnailExists(String uuid, String type)

    abstract boolean tileExists(String uuid, int x, int y, int z)

    abstract ImageInfo originalImageInfo(String uuid)

    abstract ImageInfo thumbnailImageInfo(String uuid, String type)

    abstract ImageInfo tileImageInfo(String uuid, int x, int y, int z)

}
