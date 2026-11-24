//! Adapts one enrollment context to the Central driver's lease and retirement port.

use std::{collections::BTreeSet, sync::Arc};

use central_link_client::EnrollmentEnd;
use router_clock::{ClockInstant, RuntimeClock};
use switchboard_central_link::{
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource},
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
pub struct EnrollmentLeaseSource {
    commands: mpsc::UnboundedSender<LeaseCommand>,
    retirements: RetirementFence,
    closing: watch::Sender<Option<ClockInstant>>,
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
    Joined(LeaseJoin),
}

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
        owner.supervise_retirement();
        let retirements = owner.retirement_fence();
        let task = tokio::spawn(run(owner, clock, work, stopped, excluded.clone()));
        Self {
            commands,
            retirements,
            closing,
            excluded,
            completion: AsyncMutex::new(Completion::Running(task)),
        }
    }

    /// Closes admission and requests the earliest supplied shutdown cutoff.
    pub fn request_stop_by(&self, cutoff: ClockInstant) {
        self.closing.send_if_modified(|deadline| {
            if deadline.is_some_and(|previous| previous <= cutoff) {
                return false;
            }
            *deadline = Some(cutoff);
            true
        });
    }

    /// Waits for physical completion. Cancelling a wait leaves the join available.
    pub async fn join(&self) -> LeaseJoin {
        let mut completion = self.completion.lock().await;
        match &mut *completion {
            Completion::Running(task) => {
                let worker = task
                    .await
                    .map_err(LeaseWorkerFailure::from)
                    .and_then(|result| result);
                // An admitted retirement write is store-owned and can finish after
                // its child stops waiting. A pending entry here stays fenced even
                // when the store has already written, or will write, the ending.
                let joined = LeaseJoin {
                    unsettled_retirements: self.retirements.pending_keys(),
                    worker,
                };
                *completion = Completion::Joined(joined.clone());
                joined
            }
            Completion::Joined(joined) => joined.clone(),
        }
    }
}

impl LeaseSource<EnrollmentKey> for EnrollmentLeaseSource {
    fn acquire<'source>(
        &'source self,
        key: &'source EnrollmentKey,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, EnrollmentKey> {
        Box::pin(async move {
            let (reply, result) = oneshot::channel();
            {
                let closing = self.closing.borrow();
                if self.excluded.contains(&key.enrollment_id) || self.retirements.is_fenced(key) {
                    return Err(LeaseFailure::EnrollmentEnded);
                }
                if closing.is_some() {
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
        let closing = self.closing.borrow();
        if closing.is_some() || self.excluded.contains(&key.enrollment_id) {
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
