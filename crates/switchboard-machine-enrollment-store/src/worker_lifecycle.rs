use std::{future::pending, sync::Arc};

use tokio::sync::{Mutex, mpsc, watch};

use crate::{
    EnrollmentRequest, EnrollmentStoreCloseError,
    enrollment_worker::{EnrollmentWorker, EnrollmentWorkerExit},
};

/// The enrollment store's retained result of reaping its worker and resources.
#[derive(Clone, Debug)]
pub struct EnrollmentJoin {
    pub outcome: Result<(), EnrollmentStoreCloseError>,
}

#[derive(Clone)]
pub(super) struct WorkerLifecycle {
    progress: watch::Receiver<WorkerProgress>,
    #[cfg(test)]
    abort: tokio::task::AbortHandle,
}

enum WorkerProgress {
    Running,
    Exited(EnrollmentWorkerExit),
    Joined(EnrollmentWorkerExit, EnrollmentJoin),
}

impl WorkerLifecycle {
    pub(super) fn start(
        worker: EnrollmentWorker,
        requests: mpsc::UnboundedReceiver<EnrollmentRequest>,
        shutdown: watch::Receiver<bool>,
        enrollment_changes: watch::Sender<u64>,
    ) -> Self {
        let (report, progress) = watch::channel(WorkerProgress::Running);
        let worker = Arc::new(Mutex::new(worker));
        // The task borrows the resources. The reaper retains ownership across
        // unwind and takes them back only after Tokio has dropped that borrow.
        let task_worker = Arc::clone(&worker);
        let task = tokio::spawn(async move {
            task_worker
                .lock()
                .await
                .run(requests, shutdown, enrollment_changes)
                .await
        });
        #[cfg(test)]
        let abort = task.abort_handle();
        tokio::spawn(async move {
            let worker_result = task.await;
            let exit = match &worker_result {
                Ok(exit) => *exit,
                Err(_) => EnrollmentWorkerExit::Autonomous,
            };
            report.send_replace(WorkerProgress::Exited(exit));

            let Ok(worker) = Arc::try_unwrap(worker) else {
                return;
            };
            let connection_result = worker.into_inner().close_resources().await;
            let outcome = match worker_result {
                Ok(_) => connection_result
                    .map_err(|error| EnrollmentStoreCloseError::Connection(Arc::new(error))),
                Err(_) => Err(EnrollmentStoreCloseError::WorkerStopped),
            };
            report.send_replace(WorkerProgress::Joined(exit, EnrollmentJoin { outcome }));
        });
        Self {
            progress,
            #[cfg(test)]
            abort,
        }
    }

    pub(super) async fn worker_exit(&self) -> EnrollmentWorkerExit {
        let mut progress = self.progress.clone();
        let observed = progress
            .wait_for(|progress| !matches!(progress, WorkerProgress::Running))
            .await;
        let exit = match observed.as_deref() {
            Ok(WorkerProgress::Exited(exit) | WorkerProgress::Joined(exit, _)) => Some(*exit),
            Ok(WorkerProgress::Running) | Err(_) => None,
        };
        drop(observed);
        match exit {
            Some(exit) => exit,
            None => pending().await,
        }
    }

    pub(super) async fn join(&self) -> EnrollmentJoin {
        let mut progress = self.progress.clone();
        let observed = progress
            .wait_for(|progress| matches!(progress, WorkerProgress::Joined(..)))
            .await;
        let receipt = match observed.as_deref() {
            Ok(WorkerProgress::Joined(_, receipt)) => Some(receipt.clone()),
            Ok(WorkerProgress::Running | WorkerProgress::Exited(_)) | Err(_) => None,
        };
        drop(observed);
        match receipt {
            Some(receipt) => receipt,
            // A lost reaper cannot witness that the resources have ended.
            None => pending().await,
        }
    }

    #[cfg(test)]
    pub(super) fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.abort.clone()
    }
}
