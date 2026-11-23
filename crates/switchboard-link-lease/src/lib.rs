//! Adapts one enrollment context to the Central driver's lease and retirement port.

use std::{fmt, sync::Arc};

use central_link_client::EnrollmentEnd;
use router_clock::RuntimeClock;
use switchboard_central_link::{
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource},
    link_policy::LeaseVersion,
};
use switchboard_machine_enrollment::{CredentialStore, EnrollmentKey, RefreshOwner, RefreshSource};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

mod lease_worker;

use lease_worker::{LeaseCommand, run};

/// Lease adapter started and closed by its composition owner.
pub struct EnrollmentLeaseSource {
    commands: mpsc::UnboundedSender<LeaseCommand>,
    state: watch::Sender<Admission>,
    closing: watch::Sender<bool>,
    completion: AsyncMutex<Completion>,
}

#[derive(Default)]
struct Admission {
    closed: bool,
    retirements: Vec<EnrollmentKey>,
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
        owner: Arc<RefreshOwner<Store, Source>>,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
    ) -> Self
    where
        Store: CredentialStore + 'static,
        Source: RefreshSource + 'static,
    {
        let (commands, work) = mpsc::unbounded_channel();
        let (closing, stopped) = watch::channel(false);
        let (state, _) = watch::channel(Admission::default());
        let task = tokio::spawn(run(owner, clock, state.clone(), work, stopped));
        Self {
            commands,
            state,
            closing,
            completion: AsyncMutex::new(Completion::Running(task)),
        }
    }

    /// Stops admission, drains started work within the injected budget and reports any remainder.
    pub async fn close(&self) -> Result<UnfinishedLeaseWork, LeaseWorkerStopped> {
        self.state.send_modify(|state| state.closed = true);
        self.closing.send_replace(true);
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

impl LeaseSource<EnrollmentKey> for EnrollmentLeaseSource {
    fn acquire<'source>(
        &'source self,
        key: &'source EnrollmentKey,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, EnrollmentKey> {
        Box::pin(async move {
            let (reply, result) = oneshot::channel();
            {
                let state = self.state.borrow();
                if state.retirements.contains(key) {
                    return Err(LeaseFailure::EnrollmentEnded);
                }
                if state.closed {
                    return Err(LeaseFailure::Fault);
                }
                self.commands
                    .send(LeaseCommand::Acquire {
                        key: key.clone(),
                        newer_than,
                        reply,
                    })
                    .map_err(|_| LeaseFailure::Fault)?;
            }
            let result = result.await.map_err(|_| LeaseFailure::Fault)?;
            let state = self.state.borrow();
            if state.retirements.contains(key) {
                return Err(LeaseFailure::EnrollmentEnded);
            }
            result
        })
    }

    fn end(&self, key: &EnrollmentKey, cause: EnrollmentEnd) {
        self.state.send_if_modified(|state| {
            if state.closed || state.retirements.contains(key) {
                return false;
            }
            if self
                .commands
                .send(LeaseCommand::Retire {
                    key: key.clone(),
                    cause,
                })
                .is_err()
            {
                return false;
            }
            state.retirements.push(key.clone());
            true
        });
    }
}

#[cfg(test)]
#[path = "lease_adapter_test.rs"]
mod tests;

mod link_settings;
pub use link_settings::{DiscoverySource, LinkSettingsSource};
