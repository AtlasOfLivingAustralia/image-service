package au.org.ala.images

class BatchFileUpload {

    String dataResourceUid
    String md5Hash
    String filePath
    Date dateCreated
    Date lastUpdated
    String status
    String message
    Date dateCompleted

    Integer getRecordCount(){
        batchFiles.collect {it.getRecordCount()?:0 }.sum()
    }

    Integer getNewImagesCount(){
        batchFiles.collect {it.getNewImages()?:0 }.sum()
    }

    Integer getMetadataUpdateCount(){
        batchFiles.collect {it.getMetadataUpdates()?:0 }.sum()
    }

    Integer getErrorCount(){
        batchFiles.collect {it.getErrorCount()?:0 }.sum()
    }

    static hasMany = [batchFiles:BatchFile]
    static constraints = {
        dateCompleted nullable: true
        message nullable: true
    }
    static mapping = {
        version false
        batchFiles lazy: false
    }
}
