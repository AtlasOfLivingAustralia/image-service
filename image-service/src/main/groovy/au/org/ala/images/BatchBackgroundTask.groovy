package au.org.ala.images

/**
 * Created by bai159 on 7/18/14.
 *
 * A batch background task is a single task within the context of a batch. A batch is a series jobs with a common batch id. Jobs within batch are ordered by sequence number (insert order)
 */
class BatchBackgroundTask extends BackgroundTask implements BackgroundTaskObserver {

    String batchId
    int sequenceNumber
    BatchBackgroundTaskState state

    private BackgroundTask _task
    private BatchService _batchService
    private String _errorMessage
    private Object _result

    BatchBackgroundTask(String batchId, int sequenceNumber, BackgroundTask task, BatchService batchService) {
        this.batchId = batchId
        this.sequenceNumber = sequenceNumber
        this.state = BatchBackgroundTaskState.Pending
        _task = task
        _batchService = batchService
    }

    @Override
    void execute() {
        if (_task) {
            state = BatchBackgroundTaskState.Running
            try {
                _task.addObserver(this)
                _task.doExecute()
                state = BatchBackgroundTaskState.Success
            } catch (Exception ex) {
                state = BatchBackgroundTaskState.Error
                _errorMessage = ex.message
            } finally {
                _batchService.notifyBatchTaskComplete(batchId, sequenceNumber, _result)
            }
        }
    }

    @Override
    void onTaskResult(BackgroundTask task, Object result) {
        _result = result
    }
}

enum BatchBackgroundTaskState {
    Pending, Running, Success, Error
}
