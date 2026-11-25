//! Adapts one enrollment context to the Central driver's lease and retirement port.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, PoisonError},
};

use central_link_client::EnrollmentEnd;
use router_clock::{ClockInstant, RuntimeClock};
use switchboard_central_link::{
    lease_source::{ApprovalServiceTarget, LeaseFailure, LeaseFuture, LeaseSource},
    link_policy::LeaseVersion,
};
use switchboard_machine_enrollment::{
    CredentialStore, EnrollmentEnd as StoredEnding, EnrollmentId, EnrollmentKey, RefreshOwner,
    RefreshSource, RetirementFence,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

mod lease_worker;

use lease_worker::{LeaseCommand, run};

/// Lease adapter started and closed by its composition owner.
///
/// The executor calls `seal_ending_reports` after the driver physically joins
/// or when it proves that the driver never started. A timed-out driver stop
/// leaves reports open until the late driver join.
pub struct EnrollmentLeaseSource {
    commands: mpsc::UnboundedSender<LeaseCommand>,
    retirements: RetirementFence,
    closing: watch::Sender<Option<ClockInstant>>,
    admission: Arc<Mutex<Admission>>,
    ending_reports: watch::Sender<Admission>,
    worker_exit: watch::Receiver<Option<LeaseWorkerExit>>,
    excluded: Arc<BTreeSet<EnrollmentId>>,
    completion: AsyncMutex<Completion>,
}

/// Physical completion of the worker and its child tasks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseJoin {
    pub unsettled_retirements: Vec<EnrollmentKey>,
    pub worker: Result<(), LeaseWorkerFailure>,
}

enum Completion {
    Running(JoinHandle<Result<(), LeaseWorkerFailure>>),
    Reaped(Result<(), LeaseWorkerFailure>),
    Finished(Result<LeaseJoin, LeaseJoinUnconfirmed>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Admission {
    Open,
    Sealed,
}

/// The worker exited before the lease's physical join was necessarily ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseWorkerExit {
    Stopped,
    Failed(LeaseWorkerFailure),
    Unconfirmed,
}

struct WorkerExitPublication {
    admission: Arc<Mutex<Admission>>,
    observed: watch::Sender<Option<LeaseWorkerExit>>,
}

impl WorkerExitPublication {
    fn seal(&self) {
        *self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Admission::Sealed;
    }

    fn publish(&self, outcome: Result<(), LeaseWorkerFailure>) {
        self.seal();
        self.observed.send_replace(Some(match outcome {
            Ok(()) => LeaseWorkerExit::Stopped,
            Err(error) => LeaseWorkerExit::Failed(error),
        }));
    }
}

impl Drop for WorkerExitPublication {
    fn drop(&mut self) {
        self.seal();
    }
}

/// The reaper was lost before it certified the physical join.
///
/// The executor keeps this piece outstanding and starts no replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseJoinUnconfirmed {
    ReaperPanicked,
    ReaperCancelled,
}

impl From<tokio::task::JoinError> for LeaseJoinUnconfirmed {
    fn from(error: tokio::task::JoinError) -> Self {
        if error.is_panic() {
            Self::ReaperPanicked
        } else {
            Self::ReaperCancelled
        }
    }
}

impl std::fmt::Display for LeaseJoinUnconfirmed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReaperPanicked => {
                formatter.write_str("the lease reaper panicked before confirming the join")
            }
            Self::ReaperCancelled => {
                formatter.write_str("the lease reaper was cancelled before confirming the join")
            }
        }
    }
}

impl std::error::Error for LeaseJoinUnconfirmed {}

/// Diagnostic outcome of the lease worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseWorkerFailure {
    Panicked,
    Cancelled,
}

impl std::fmt::Display for LeaseWorkerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Panicked => formatter.write_str("the enrollment lease worker panicked"),
            Self::Cancelled => formatter.write_str("the enrollment lease worker was cancelled"),
        }
    }
}

impl std::error::Error for LeaseWorkerFailure {}

impl From<tokio::task::JoinError> for LeaseWorkerFailure {
    fn from(error: tokio::task::JoinError) -> Self {
        if error.is_panic() {
            Self::Panicked
        } else {
            Self::Cancelled
        }
    }
}

impl EnrollmentLeaseSource {
    /// Starts the lease supervisor in the caller's runtime.
    pub fn start<Store, Source>(
        owner: RefreshOwner<Store, Source>,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
        excluded: Arc<BTreeSet<EnrollmentId>>,
    ) -> Self
    where
        Store: CredentialStore + 'static,
        Source: RefreshSource + 'static,
    {
        let (commands, work) = mpsc::unbounded_channel();
        let (closing, stopped) = watch::channel(None);
        let (observed, worker_exit) = watch::channel(None);
        let admission = Arc::new(Mutex::new(Admission::Open));
        let publication = WorkerExitPublication {
            admission: admission.clone(),
            observed,
        };
        owner.supervise_retirement();
        let retirements = owner.retirement_fence();
        let task = tokio::spawn(run(
            owner,
            clock,
            work,
            stopped,
            excluded.clone(),
            publication,
        ));
        Self {
            commands,
            retirements,
            closing,
            admission,
            ending_reports: watch::channel(Admission::Open).0,
            worker_exit,
            excluded,
            completion: AsyncMutex::new(Completion::Running(task)),
        }
    }

    /// Closes admission and requests the earliest supplied shutdown cutoff.
    pub fn request_stop_by(&self, cutoff: ClockInstant) {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *admission = Admission::Sealed;
        self.closing.send_if_modified(|deadline| {
            if deadline.is_some_and(|previous| previous <= cutoff) {
                return false;
            }
            *deadline = Some(cutoff);
            true
        });
    }

    /// Closes definitive reports after the driver has joined or never started.
    pub fn seal_ending_reports(&self) {
        self.ending_reports.send_if_modified(|admission| {
            if *admission == Admission::Sealed {
                return false;
            }
            *admission = Admission::Sealed;
            true
        });
    }

    /// Observes the worker's retained exit before waiting for its children.
    pub async fn worker_exit(&self) -> LeaseWorkerExit {
        let mut observed = self.worker_exit.clone();
        match observed.wait_for(Option::is_some).await {
            Ok(outcome) => outcome.unwrap_or(LeaseWorkerExit::Unconfirmed),
            Err(_) => LeaseWorkerExit::Unconfirmed,
        }
    }

    /// Waits for reaping and the driver report seal. Cancelling a wait retains the receipt.
    pub async fn join(&self) -> Result<LeaseJoin, LeaseJoinUnconfirmed> {
        let mut completion = self.completion.lock().await;
        let worker = match &mut *completion {
            Completion::Running(task) => match task.await {
                Ok(worker) => {
                    *completion = Completion::Reaped(worker);
                    worker
                }
                Err(error) => {
                    let unconfirmed = LeaseJoinUnconfirmed::from(error);
                    *completion = Completion::Finished(Err(unconfirmed));
                    return Err(unconfirmed);
                }
            },
            Completion::Reaped(worker) => *worker,
            Completion::Finished(joined) => return joined.clone(),
        };
        // This adapter holds the sender for the entire wait.
        let mut reports = self.ending_reports.subscribe();
        let _ = reports
            .wait_for(|admission| *admission == Admission::Sealed)
            .await;

        // An admitted retirement write is store-owned and can finish
        // after its child stops waiting. Pending here stays fenced.
        let joined = LeaseJoin {
            unsettled_retirements: self.retirements.pending_keys(),
            worker,
        };
        *completion = Completion::Finished(Ok(joined.clone()));
        Ok(joined)
    }
}

impl LeaseSource<EnrollmentKey> for EnrollmentLeaseSource {
    fn registration_target(
        &self,
        key: &EnrollmentKey,
    ) -> Result<ApprovalServiceTarget, LeaseFailure> {
        let organization_id = key
            .organization_id
            .to_string()
            .try_into()
            .map_err(|_| LeaseFailure::Fault)?;
        Ok(ApprovalServiceTarget { organization_id })
    }

    fn acquire<'source>(
        &'source self,
        key: &'source EnrollmentKey,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, EnrollmentKey> {
        Box::pin(async move {
            let (reply, result) = oneshot::channel();
            {
                let admission = self
                    .admission
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if self.excluded.contains(&key.enrollment_id) || self.retirements.is_fenced(key) {
                    return Err(LeaseFailure::EnrollmentEnded);
                }
                if *admission == Admission::Sealed {
                    return Err(LeaseFailure::Fault);
                }
                self.commands
                    .send(LeaseCommand {
                        key: key.clone(),
                        newer_than,
                        reply,
                    })
                    .map_err(|_| LeaseFailure::Fault)?;
            }
            let result = result.await.map_err(|_| LeaseFailure::Fault)?;
            if self.excluded.contains(&key.enrollment_id) || self.retirements.is_fenced(key) {
                return Err(LeaseFailure::EnrollmentEnded);
            }
            result
        })
    }

    fn end(&self, key: &EnrollmentKey, cause: EnrollmentEnd) {
        let admission = self.ending_reports.borrow();
        if *admission == Admission::Sealed || self.excluded.contains(&key.enrollment_id) {
            return;
        }
        let why = match cause {
            EnrollmentEnd::LeaseRefused => return,
            EnrollmentEnd::Revoked => StoredEnding::Revoked,
            EnrollmentEnd::Inactive => StoredEnding::Inactive,
            EnrollmentEnd::ConnectionEnrollmentEnded => StoredEnding::EndedByCentral,
        };
        self.retirements.request_definitive(key, why);
    }
}

#[cfg(test)]
#[path = "lease_adapter_test.rs"]
mod tests;

mod link_settings;
pub use link_settings::{DiscoverySource, LinkSettingsSource};
