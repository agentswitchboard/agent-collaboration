use std::sync::Arc;

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

/// Why the enrollment store could not provide the requested lifecycle witness.
/// A `join()` error means ownership is unconfirmed. Keep the store outstanding
/// and withhold any replacement until physical termination is established.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentObservationError {
    /// The publisher ended before recording the requested witness.
    #[error("the enrollment reaper stopped before publishing the requested witness")]
    ReaperLost,
    /// The worker task ended, but the reaper could not recover its resources.
    #[error("the enrollment reaper could not confirm release of the worker's resources")]
    OwnershipUnconfirmed { exit: EnrollmentWorkerExit },
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
    Unconfirmed(EnrollmentWorkerExit),
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
                report.send_replace(WorkerProgress::Unconfirmed(exit));
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

    pub(super) async fn worker_exit(
        &self,
    ) -> Result<EnrollmentWorkerExit, EnrollmentObservationError> {
        let mut progress = self.progress.clone();
        let observed = progress
            .wait_for(|progress| !matches!(progress, WorkerProgress::Running))
            .await
            .map_err(|_| EnrollmentObservationError::ReaperLost)?;
        match &*observed {
            WorkerProgress::Exited(exit)
            | WorkerProgress::Joined(exit, _)
            | WorkerProgress::Unconfirmed(exit) => Ok(*exit),
            WorkerProgress::Running => Err(EnrollmentObservationError::ReaperLost),
        }
    }

    pub(super) async fn join(&self) -> Result<EnrollmentJoin, EnrollmentObservationError> {
        let mut progress = self.progress.clone();
        let observed = progress
            .wait_for(|progress| {
                matches!(
                    progress,
                    WorkerProgress::Joined(..) | WorkerProgress::Unconfirmed(_)
                )
            })
            .await
            .map_err(|_| EnrollmentObservationError::ReaperLost)?;
        match &*observed {
            WorkerProgress::Joined(_, receipt) => Ok(receipt.clone()),
            WorkerProgress::Unconfirmed(exit) => {
                Err(EnrollmentObservationError::OwnershipUnconfirmed { exit: *exit })
            }
            WorkerProgress::Running | WorkerProgress::Exited(_) => {
                Err(EnrollmentObservationError::ReaperLost)
            }
        }
    }

    #[cfg(test)]
    pub(super) fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.abort.clone()
    }
}
