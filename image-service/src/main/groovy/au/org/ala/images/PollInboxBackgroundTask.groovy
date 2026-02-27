package au.org.ala.images

import java.text.SimpleDateFormat

class PollInboxBackgroundTask extends BackgroundTask {

    ImageService imageService
    String batchId
    String userId

    PollInboxBackgroundTask(ImageService imageService, String userId) {
        this.imageService = imageService
        this.userId = userId
        def sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        this.batchId =  sdf.format(new Date())
    }

    @Override
    void execute() {
        imageService.pollInbox(batchId, userId)
    }

}
