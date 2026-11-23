//! Adapts one enrollment context to the Central driver's lease and retirement port.

use std::{fmt, future::Future, sync::Arc, time::Duration};

use central_link_client::EnrollmentEnd;
use router_clock::{ClockInstant, RuntimeClock};
use switchboard_central_link::{
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource},
    link_policy::LeaseVersion,
};
use switchboard_machine_enrollment::{
    CredentialStore, EnrollmentEnd as StoredEnding, EnrollmentKey, RefreshOwner, RefreshSource,
    RetirementFence,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

mod lease_worker;

use lease_worker::{LeaseCommand, run};

const CLOSE_BUDGET: Duration = Duration::from_secs(5);

/// Lease adapter started and closed by its composition owner.
pub struct EnrollmentLeaseSource {
    commands: mpsc::UnboundedSender<LeaseCommand>,
    retirements: RetirementFence,
    closing: watch::Sender<Option<ClockInstant>>,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    completion: AsyncMutex<Completion>,
}

/// Operations that did not finish on the closing adapter.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnfinishedLeaseWork {
    pub acquisitions: Vec<EnrollmentKey>,
    pub retirements: Vec<EnrollmentKey>,
}

enum Completion {
    Running(JoinHandle<UnfinishedLeaseWork>),
    Closed(Result<UnfinishedLeaseWork, LeaseWorkerStopped>),
}

/// The task ended without returning its shutdown report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseWorkerStopped;

impl fmt::Display for LeaseWorkerStopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the enrollment lease worker stopped without a shutdown report")
    }
}

impl std::error::Error for LeaseWorkerStopped {}

impl EnrollmentLeaseSource {
    /// Starts the lease supervisor in the caller's runtime.
    pub fn start<Store, Source>(
        owner: RefreshOwner<Store, Source>,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
    ) -> Self
    where
        Store: CredentialStore + 'static,
        Source: RefreshSource + 'static,
    {
        let (commands, work) = mpsc::unbounded_channel();
        let (closing, stopped) = watch::channel(None);
        owner.supervise_retirement();
        let retirements = owner.retirement_fence();
        let task = tokio::spawn(run(owner, clock.clone(), work, stopped));
        Self {
            commands,
            retirements,
            closing,
            clock,
            completion: AsyncMutex::new(Completion::Running(task)),
        }
    }

    /// Stops admission immediately and gives all close callers the same deadline.
    ///
    /// The five-second bound uses the injected clock. It requires the runtime to
    /// schedule the supervisor and injected futures to yield promptly. Cancellation
    /// is cooperative, so a store operation already admitted may still settle.
    pub fn close(
        &self,
    ) -> impl Future<Output = Result<UnfinishedLeaseWork, LeaseWorkerStopped>> + '_ {
        self.closing.send_if_modified(|deadline| {
            if deadline.is_some() {
                return false;
            }
            *deadline = Some(self.clock.monotonic_now().after(CLOSE_BUDGET));
            true
        });
        async {
            let mut completion = self.completion.lock().await;
            match &mut *completion {
                Completion::Running(task) => {
                    let result = task.await.map_err(|_| LeaseWorkerStopped);
                    *completion = Completion::Closed(result.clone());
                    result
                }
                Completion::Closed(result) => result.clone(),
            }
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
                if self.retirements.is_fenced(key) {
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
            if self.retirements.is_fenced(key) {
                return Err(LeaseFailure::EnrollmentEnded);
            }
            result
        })
    }

    fn end(&self, key: &EnrollmentKey, cause: EnrollmentEnd) {
        let closing = self.closing.borrow();
        if closing.is_some() {
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
