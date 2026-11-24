use std::{panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt;
#[cfg(test)]
use futures_util::future::{AbortHandle, Abortable};
use state_machine_core::StateMachine;
use tokio::sync::{mpsc, watch};

use crate::{
    EnrollmentJoin, EnrollmentObservationError, EnrollmentRequest, EnrollmentStoreCloseError,
    EnrollmentWorkerExit,
    enrollment_worker::EnrollmentWorker,
    enrollment_worker_machine::{
        EnrollmentWorkerMachine, WorkerAdmission, WorkerEvent, WorkerProgress, WorkerReport,
        WorkerState, WorkerTaskEnd,
    },
};

#[derive(Clone)]
pub(super) struct WorkerLifecycle {
    // Before exit, the shutdown watch owns the live admission state.
    progress: watch::Receiver<Option<WorkerProgress>>,
    #[cfg(test)]
    abort: AbortHandle,
}

impl WorkerLifecycle {
    pub(super) fn start(
        mut worker: EnrollmentWorker,
        requests: mpsc::UnboundedReceiver<EnrollmentRequest>,
        shutdown: watch::Receiver<WorkerAdmission>,
        enrollment_changes: watch::Sender<u64>,
    ) -> Self {
        let (publisher, progress) = watch::channel(None);
        #[cfg(test)]
        let (abort, registration) = AbortHandle::new_pair();
        tokio::spawn(async move {
            // The loop borrows the worker. Its future, including any outstanding
            // request, is dropped inside this scope before the owner reaps it.
            let loop_result = AssertUnwindSafe(async {
                let run = worker.run(requests, shutdown.clone(), enrollment_changes);
                #[cfg(test)]
                {
                    Abortable::new(run, registration).await.ok()
                }
                #[cfg(not(test))]
                {
                    Some(run.await)
                }
            })
            .catch_unwind()
            .await;
            let task_end = match loop_result {
                Ok(Some(exit)) => WorkerTaskEnd::Returned(exit),
                Ok(None) | Err(_) => WorkerTaskEnd::Failed,
            };
            let admission = *shutdown.borrow();
            let exited = publish_transition(
                &publisher,
                &WorkerState::Running(admission),
                WorkerEvent::TaskExited(task_end),
            );

            // Always reap the owned resources. The first publication is the
            // loop's exit. Only an acknowledged close proves physical join.
            let closed = worker.close_resources().await.map_err(Arc::new);
            if let Some(exited) = exited {
                publish_transition(
                    &publisher,
                    &exited,
                    WorkerEvent::SqliteCloseFinished(closed),
                );
            }
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
            .wait_for(Option::is_some)
            .await
            .map_err(|_| EnrollmentObservationError::ReaperLost)?;
        observed
            .as_ref()
            .map(WorkerProgress::exit)
            .ok_or(EnrollmentObservationError::ReaperLost)
    }

    pub(super) async fn join(&self) -> Result<EnrollmentJoin, EnrollmentObservationError> {
        self.worker_report().await?.join()
    }

    pub(super) async fn close_outcome(&self) -> Result<(), EnrollmentStoreCloseError> {
        self.worker_report()
            .await
            .map_err(|_| EnrollmentStoreCloseError::WorkerStopped)?
            .close_outcome()
    }

    async fn worker_report(&self) -> Result<WorkerReport, EnrollmentObservationError> {
        let mut progress = self.progress.clone();
        let observed = progress
            .wait_for(|progress| progress.as_ref().and_then(WorkerProgress::report).is_some())
            .await
            .map_err(|_| EnrollmentObservationError::ReaperLost)?;
        observed
            .as_ref()
            .and_then(WorkerProgress::report)
            .cloned()
            .ok_or(EnrollmentObservationError::ReaperLost)
    }

    #[cfg(test)]
    pub(super) fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }
}

fn publish_transition(
    publisher: &watch::Sender<Option<WorkerProgress>>,
    state: &WorkerState,
    event: WorkerEvent,
) -> Option<WorkerState> {
    let (next, _) = EnrollmentWorkerMachine::transition(state, event).ok()?;
    let WorkerState::Progress(progress) = &next else {
        return None;
    };
    publisher.send_replace(Some(progress.clone()));
    Some(next)
}
