use std::sync::Arc;

use switchboard_machine_core_store::MachineLocalStoreError;
use tokio::sync::{Mutex, mpsc, watch};

use crate::{
    EnrollmentRequest, EnrollmentStoreCloseError,
    enrollment_worker::{EnrollmentWorker, EnrollmentWorkerExit},
};

/// A physical join receipt published by the enrollment store after the worker
/// and its blocking tasks end and SQLite acknowledges the connection close.
#[derive(Clone, Debug)]
pub struct EnrollmentJoin {
    pub outcome: Result<(), EnrollmentStoreCloseError>,
}

/// Why the enrollment store could not provide the requested lifecycle witness.
/// Every `join()` error, including an unacknowledged SQLite close, leaves
/// ownership unconfirmed. Keep the store outstanding and withhold replacement
/// until physical termination is established.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentObservationError {
    /// The publisher ended before recording the requested witness.
    #[error("the enrollment reaper stopped before publishing the requested witness")]
    ReaperLost,
    /// The worker task ended, but the reaper could not recover its resources.
    #[error("the enrollment reaper could not confirm release of the worker's resources")]
    OwnershipUnconfirmed { exit: EnrollmentWorkerExit },
    /// SQLite close returned an error before confirming connection release.
    #[error("the enrollment reaper could not confirm the database connection closed")]
    ConnectionCloseUnconfirmed { exit: EnrollmentWorkerExit },
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
    Reported(ReaperReport),
}

#[derive(Clone)]
struct ReaperReport {
    exit: EnrollmentWorkerExit,
    completion: WorkerCompletion,
}

#[derive(Clone)]
enum WorkerCompletion {
    Joined(EnrollmentJoin),
    OwnershipUnconfirmed,
    ConnectionCloseUnconfirmed {
        error: Arc<MachineLocalStoreError>,
        worker_outcome: WorkerTaskOutcome,
    },
}

#[derive(Clone, Copy)]
enum WorkerTaskOutcome {
    Returned,
    Failed,
}

impl WorkerTaskOutcome {
    fn legacy_close_result(
        self,
        connection_result: Result<(), Arc<MachineLocalStoreError>>,
    ) -> Result<(), EnrollmentStoreCloseError> {
        match self {
            Self::Returned => connection_result.map_err(EnrollmentStoreCloseError::Connection),
            Self::Failed => Err(EnrollmentStoreCloseError::WorkerStopped),
        }
    }
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
            let (exit, worker_outcome) = match task.await {
                Ok(exit) => (exit, WorkerTaskOutcome::Returned),
                Err(_) => (EnrollmentWorkerExit::Autonomous, WorkerTaskOutcome::Failed),
            };
            report.send_replace(WorkerProgress::Exited(exit));

            let Ok(worker) = Arc::try_unwrap(worker) else {
                report.send_replace(WorkerProgress::Reported(ReaperReport {
                    exit,
                    completion: WorkerCompletion::OwnershipUnconfirmed,
                }));
                return;
            };
            let completion = match worker.into_inner().close_resources().await {
                Ok(()) => WorkerCompletion::Joined(EnrollmentJoin {
                    outcome: worker_outcome.legacy_close_result(Ok(())),
                }),
                Err(error) => WorkerCompletion::ConnectionCloseUnconfirmed {
                    error: Arc::new(error),
                    worker_outcome,
                },
            };
            report.send_replace(WorkerProgress::Reported(ReaperReport { exit, completion }));
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
            WorkerProgress::Exited(exit) | WorkerProgress::Reported(ReaperReport { exit, .. }) => {
                Ok(*exit)
            }
            WorkerProgress::Running => Err(EnrollmentObservationError::ReaperLost),
        }
    }

    pub(super) async fn join(&self) -> Result<EnrollmentJoin, EnrollmentObservationError> {
        let ReaperReport { exit, completion } = self.reaper_report().await?;
        match completion {
            WorkerCompletion::Joined(receipt) => Ok(receipt),
            WorkerCompletion::OwnershipUnconfirmed => {
                Err(EnrollmentObservationError::OwnershipUnconfirmed { exit })
            }
            WorkerCompletion::ConnectionCloseUnconfirmed { .. } => {
                Err(EnrollmentObservationError::ConnectionCloseUnconfirmed { exit })
            }
        }
    }

    pub(super) async fn close_outcome(&self) -> Result<(), EnrollmentStoreCloseError> {
        let report = self
            .reaper_report()
            .await
            .map_err(|_| EnrollmentStoreCloseError::WorkerStopped)?;
        match report.completion {
            WorkerCompletion::Joined(receipt) => receipt.outcome,
            WorkerCompletion::OwnershipUnconfirmed => Err(EnrollmentStoreCloseError::WorkerStopped),
            WorkerCompletion::ConnectionCloseUnconfirmed {
                error,
                worker_outcome,
            } => worker_outcome.legacy_close_result(Err(error)),
        }
    }

    async fn reaper_report(&self) -> Result<ReaperReport, EnrollmentObservationError> {
        let mut progress = self.progress.clone();
        let observed = progress
            .wait_for(|progress| matches!(progress, WorkerProgress::Reported(_)))
            .await
            .map_err(|_| EnrollmentObservationError::ReaperLost)?;
        match &*observed {
            WorkerProgress::Reported(report) => Ok(report.clone()),
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
