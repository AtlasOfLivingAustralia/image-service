package au.org.ala.images

class BatchStatus {
    String batchId = UUID.randomUUID().toString()
    int taskCount = 0
    int tasksCompleted = 0
    Date timeStarted = new Date()
    Date timeFinished = null
    Map<Integer, Object> results = [:]
}
