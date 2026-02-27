package au.org.ala.images

class BatchFile {

    String filePath
    Date dateCreated
    Date lastUpdated
    String status
    Long recordCount
    Long newImages
    Long metadataUpdates
    Long errorCount
    Long processedCount
    Date dateCompleted
    Long timeTakenToLoad

    String md5Hash

    static belongsTo = [ batchFileUpload: BatchFileUpload ]

    static constraints = {
        newImages nullable: true
        metadataUpdates nullable: true
        errorCount nullable: true
        processedCount nullable: true
        dateCompleted nullable: true
        timeTakenToLoad nullable: true
    }

    static mapping = {
        version false
    }
}
