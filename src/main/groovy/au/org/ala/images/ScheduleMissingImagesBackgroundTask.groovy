package au.org.ala.images

import com.opencsv.CSVWriter
import groovy.util.logging.Slf4j

//import org.apache.log4j.Logger
import org.apache.tomcat.util.http.fileupload.FileUtils
import org.hibernate.ScrollableResults

/**
 * Cycles through the database outputting details of images that are referenced in the database
 * but are missing on the file system
 */
@Slf4j
class ScheduleMissingImagesBackgroundTask extends BackgroundTask {

    private ImageStoreService _imageStoreService
    String _exportDirectory
//    private Logger log = Logger.getLogger(ScheduleMissingImagesBackgroundTask.class)
    boolean requiresSession = true

    ScheduleMissingImagesBackgroundTask(ImageStoreService imageStoreService, String exportDirectory) {
        _imageStoreService = imageStoreService
        _exportDirectory = exportDirectory
    }

    @Override
    void execute() {

        def exportDir = new File(_exportDirectory)
        if (!exportDir.exists()) {
            FileUtils.forceMkdir(new File(_exportDirectory))
        }

        def writer = new CSVWriter(new FileWriter(new File(exportDir, "/missing-images.csv")))
        writer.writeNext((String[])["imageIdentifier", "directory", "status"].toArray())
        def c = Image.createCriteria()
        ScrollableResults images = c.scroll { }
        def counter = 0
        while (images.next()) {
            def image = (Image) images.get(0)
            if (!image.stored()) {
                writer.writeNext((String[])[image.imageIdentifier, image.storageLocation.createOriginalPathFromUUID(image.imageIdentifier), "original-missing"].toArray())
            }
            // TODO directory missing for S3 buckets?
//            else {
//                writer.writeNext((String[])[imageId, imageDirectory.getAbsolutePath(), "directory-missing"].toArray())
//            }
            counter += 1
        }
        if (counter % 1000 == 0){
            log.info("Missing image check: " + counter)
        }

        writer.flush()
        writer.close()
        log.info("Missing images check complete. Total checked: " + counter)
    }

    @Override
    boolean isRequiresSession() {
        return true
    }
}
