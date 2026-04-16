package au.org.ala.images

abstract class BackgroundTask {

    public List<BackgroundTaskObserver> _observers;

    /** Override this to return true have the background task wrapped in a GORM Session */
    boolean isRequiresSession() {
        false
    }

    /**
     * Execute this BackgroundTask
     */
    void doExecute() {
        if (requiresSession) {
            Image.withNewSession { session ->
                execute()
            }
        } else {
            execute()
        }
    }

    /**
     * BackgroundTasks should override this method with their implementation.
     */
    protected abstract void execute();

    protected void yieldResult(Object result) {
        if (_observers != null) {
            _observers.each { observer ->
                try {
                    observer.onTaskResult(this, result)
                } catch (Exception ex) {
                    ex.printStackTrace()
                }
            }
        }
    }

    void addObserver(BackgroundTaskObserver observer) {
        if (_observers == null) {
            _observers = new ArrayList<BackgroundTaskObserver>()
        }

        if (!_observers.contains(observer)) {
            _observers.add(observer);
        }
    }

}

interface BackgroundTaskObserver {
    void onTaskResult(BackgroundTask task, Object result);
}
