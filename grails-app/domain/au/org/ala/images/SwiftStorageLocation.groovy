package au.org.ala.images

import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import groovy.transform.EqualsAndHashCode
import net.lingala.zip4j.io.inputstream.ZipInputStream
import org.apache.commons.io.FilenameUtils
import org.javaswift.joss.client.factory.AccountFactory
import org.javaswift.joss.client.factory.AuthenticationMethod
import org.javaswift.joss.exception.NotFoundException
import org.javaswift.joss.headers.object.range.MidPartRange
import org.javaswift.joss.instructions.DownloadInstructions
import org.javaswift.joss.instructions.UploadInstructions
import org.javaswift.joss.model.Account
import org.javaswift.joss.model.Container
import org.javaswift.joss.model.StoredObject

import java.nio.file.Files

@EqualsAndHashCode(includes=['authUrl', 'containerName', 'tenantId', 'tenantName', 'publicContainer'])
class SwiftStorageLocation extends StorageLocation {

    AuthenticationMethod authenticationMethod = AuthenticationMethod.BASIC
    String authUrl
    String username
    String password

    String tenantId
    String tenantName

    String containerName

    boolean publicContainer
    boolean redirect
    boolean mock = false

    static constraints = {
        tenantId nullable: false, blank: true
        tenantName nullable: false, blank: true
    }

    static mapping = {
//        cache true
    }

    static transients = ['_account', '_container', 'mock']

    Account _account
    Container _container

    Account getAccount() {
        if (!_account) {
            def accountFactory = new AccountFactory()
                    .setUsername(username)
                    .setPassword(password)
                    .setAuthUrl(authUrl)
                    .setAuthenticationMethod(authenticationMethod)
                    .setMock(mock)



            if (tenantId) {
                accountFactory.setTenantId(tenantId)
            }
            if (tenantName) {
                accountFactory.setTenantName(tenantName)
            }
            _account = accountFactory.createAccount()
        }
        return _account
    }

    Container getContainer() {
        if (_container == null) {
            _container = account.getContainer(containerName)
            if (!_container.exists()) {
                _container.create()
            }
            if (_container.public != publicContainer) {
                _container.public ? _container.makePrivate() : _container.makePublic()
            }

        }
        return _container
    }

    @Override
    boolean isSupportsRedirect() {
        return redirect
    }

    @Override
    URI redirectLocation(String path) {
        container.getObject(path).publicURL.toURI()
    }

    boolean verifySettings() {
        try {
            def container = container
            container.exists() && account.list().any { it.name == containerName && it.public == publicContainer }
        } catch (e) {
            log.error("Exception while verifying settings for Swift container {}: {}", this, e.message)
            return false
        }
    }

    @Override
    void store(String uuid, InputStream stream, String contentType, String contentDisposition, Long length) {
        String path = createOriginalPathFromUUID(uuid)
        storeInternal(stream, path, contentType, contentDisposition, length)
    }

    @Override
    byte[] retrieve(String uuid) {
        def path = createOriginalPathFromUUID(uuid)
        try {
            container.getObject(path).downloadObject()
        } catch (NotFoundException e) {
            throw new FileNotFoundException(path)
        }
    }

    @Override
    InputStream inputStream(String path, Range range) throws FileNotFoundException {
        try {
            def obj = container.getObject(path)
            def downloadInstructions = new DownloadInstructions()
            if (!range.empty) {
                downloadInstructions.range = new MidPartRange(range.start() as int, range.end() + (mock ? 1 : 0) as int) // there's a bug in the Swift mock impl that has an off by one error for range requests
            }
            return obj.downloadObjectAsInputStream(downloadInstructions)
        } catch (NotFoundException e) {
            throw new FileNotFoundException(path)
        }
    }

    @Override
    long storedLength(String path) throws FileNotFoundException {
        try {
            container.getObject(path).contentLength
        } catch (NotFoundException e) {
            throw new FileNotFoundException(path)
        }
    }

    @Override
    boolean stored(String uuid) {
        container.getObject(createOriginalPathFromUUID(uuid)).exists()
    }

    @Override
    boolean thumbnailExists(String uuid, String type) {
        return container.getObject(createThumbLargePathFromUUID(uuid, type)).exists()
    }

    @Override
    boolean tileExists(String uuid, int x, int y, int z) {
        return container.getObject(createTilesPathFromUUID(uuid, x, y, z)).exists()
    }

    @Override
    void storeTileZipInputStream(String uuid, String zipFileName, String contentType, long length = 0, ZipInputStream zipInputStream) {
        def path = FilenameUtils.normalize(createTilesPathFromUUID(uuid) + '/' + zipFileName)
        def obj = container.getObject(path)
//        obj.contentLength = length
        zipInputStream.withStream { stream ->
            UploadInstructions upload
            if (length) {
                upload = new UploadInstructions(stream, length)
            } else {
                upload = new UploadInstructions(stream)
            }
            upload.contentType = contentType
            obj.uploadObject(upload)
        }
    }

    @Override
    long consumedSpace(String uuid) {
        walkPath(storagePathStrategy().createPathFromUUID(uuid)) { StoredObject so -> so.contentLength }.sum() ?: 0l
    }

    @Override
    boolean deleteStored(String uuid) {
        return walkPath(storagePathStrategy().createPathFromUUID(uuid)) { StoredObject so ->
            try {
                so.delete()
                AuditService.submitLog(uuid, "Image deleted from store", "N/A")
                true
            } catch (e) {
                log.error("Couldn't delete {} from {}", so.path, this, e)
                false
            }
        }

    }

    private <T> List<T> walkPath(String prefix, Closure<T> closure) {
        def paginationMap = container.getPaginationMap(prefix, 100)
        List<T> results = []
        for (int i= 0; i < paginationMap.numberOfPages; ++i) {
            Collection<StoredObject> children = container.list(paginationMap, i)
            results.addAll(children.collect(closure))
        }
        return results
    }

    @Override
    StoragePathStrategy storagePathStrategy() {
        return new DefaultStoragePathStrategy([], true, false)
    }

    @Override
    ByteSinkFactory thumbnailByteSinkFactory(String uuid) {
        return new SwiftByteSinkFactory(uuid)
    }

    @Override
    ByteSinkFactory tilerByteSinkFactory(String uuid) {
        return new SwiftByteSinkFactory(uuid, 'tms')
    }

    @Override
    protected storeAnywhere(String uuid, InputStream inputStream, String relativePath, String contentType, String contentDisposition, Long length) {
        def path = storagePathStrategy().createPathFromUUID(uuid, relativePath)
        storeInternal(inputStream, path, contentType, contentDisposition, length)
    }

    @Override
    void migrateTo(String uuid, String contentType, StorageLocation destination) {
        def basePath = createBasePathFromUUID(uuid)
        walkPath(basePath) { StoredObject so ->
            def relativePath = so.name - basePath
            log.error('migrate from {} {} {} {} to {}', this, uuid, relativePath, so.contentLength, destination)
            destination.storeAnywhere(uuid, so.downloadObjectAsInputStream(), so.name - basePath, so.contentType, null, so.contentLength)
        }
    }

    void storeInternal(InputStream inputStream, String path, String contentType, String contentDisposition, long length) {
        def obj = container.getObject(path)
//        obj.contentLength = length
        UploadInstructions upload = new UploadInstructions(inputStream, length)
        upload.contentType = contentType
        obj.uploadObject(upload)
    }

    class SwiftByteSinkFactory implements ByteSinkFactory {
        String uuid
        String[] path

        SwiftByteSinkFactory(String uuid, String... path) {
            this.uuid = uuid
            this.path = path
        }

        @Override
        void prepare() throws IOException {

        }

        @Override
        ByteSink getByteSinkForNames(String... names) {
            def obj = container.getObject(storagePathStrategy().createPathFromUUID(uuid, *(path + names)))
            return new ByteSink() {

                @Override
                OutputStream openStream() throws IOException {
                    def tempPath = Files.createTempFile("thumbnail-$uuid-${names.join('-')}", ".jpg")
                    def file = tempPath.toFile()
                    file.deleteOnExit()
                    return new FilterOutputStream(new BufferedOutputStream(Files.newOutputStream(tempPath))) {
                        @Override
                        void close() throws IOException {
                            super.close()
                            // once the file output is closed we can send it to Swift and then delete the temp file
                            UploadInstructions upload = new UploadInstructions(file)
                            upload.contentType = Files.probeContentType(file.toPath())
                            obj.uploadObject(upload)
                            Files.deleteIfExists(tempPath)
                        }
                    }
                }
            }
        }
    }

    @Override
    String toString() {
        return "SwiftStorageLocation $id $authUrl $tenantId $tenantName $containerName"
    }
}
