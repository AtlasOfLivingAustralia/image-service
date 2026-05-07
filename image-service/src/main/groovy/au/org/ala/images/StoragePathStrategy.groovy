package au.org.ala.images

trait StoragePathStrategy {

    String createThumbPathFromUUID(String uuid) {
        createPathFromUUID(uuid, 'thumbnail')
    }

    String createThumbLargePathFromUUID(String uuid, String type) {
        if (type && type.toLowerCase().startsWith('thumbnail_')) {
            type = type.substring(10)
        }
        createPathFromUUID(uuid, type && type != 'thumbnail' ? "thumbnail_${type ?: 'large'}" : 'thumbnail')
    }

    String createTilesPathFromUUID(String uuid) {
        createPathFromUUID(uuid, 'tms')
    }

    String createTilesPathFromUUID(String uuid, int x, int y, int z) {
        createPathFromUUID(uuid, 'tms', Integer.toString(z), Integer.toString(x), "${y}.png")
    }

    String createOriginalPathFromUUID(String uuid) {
        createPathFromUUID(uuid, 'original')
    }

    abstract String basePath()

    abstract String createPathFromUUID(String uuid, String... postfix)

}