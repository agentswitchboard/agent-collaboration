use std::{
    collections::BTreeSet,
    future::{Future, pending},
    sync::Arc,
    time::Duration,
};

use router_clock::{ClockInstant, RuntimeClock};
use switchboard_central_link::{AddressError, DialAddress, link_driver::DriverSettings};
use switchboard_machine_enrollment::{
    CredentialStore, DiscoveryAnswer, EnrollmentContext, EnrollmentId, EnrollmentKey,
    EnrollmentMetadata, MachineId, OrganizationId, SignInClient, SignInDeployment, SignInFailure,
};
use tokio::sync::watch;

const METADATA_POLL: Duration = Duration::from_secs(15);
const METADATA_RETRY: Duration = Duration::from_secs(5);
const DISCOVERY_RETRY: Duration = Duration::from_secs(30);

/// Public discovery is the external boundary of link settings.
pub trait DiscoverySource: Send + Sync {
    fn discover(&self) -> impl Future<Output = Result<DiscoveryAnswer, SignInFailure>> + Send;
}

impl DiscoverySource for SignInClient {
    async fn discover(&self) -> Result<DiscoveryAnswer, SignInFailure> {
        self.discover().await
    }
}

/// Publishes the enrolled key and discovered address for one link context.
pub struct LinkSettingsSource<Store, Discovery = SignInClient> {
    store: Store,
    discovery: Discovery,
    context: EnrollmentContext,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    changes: watch::Receiver<u64>,
    excluded: Arc<BTreeSet<EnrollmentId>>,
    settings: watch::Sender<DriverSettings<EnrollmentKey>>,
}

#[derive(Clone, Eq, PartialEq)]
struct DiscoveryTarget {
    key: EnrollmentKey,
    deployment: SignInDeployment,
}

#[derive(Eq, PartialEq)]
enum MetadataTarget {
    Unbound,
    Foreign(EnrollmentKey),
    Current(DiscoveryTarget),
}

struct DiscoveryState {
    target: DiscoveryTarget,
    last_good: Option<DialAddress>,
    refresh_at: Option<ClockInstant>,
}

impl<Store: CredentialStore> LinkSettingsSource<Store, SignInClient> {
    /// Prepares settings for the supplied sign-in client and its one enrollment context.
    pub fn new(
        store: Store,
        client: SignInClient,
        machine_id: MachineId,
        organization_id: OrganizationId,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
        changes: watch::Receiver<u64>,
        excluded: Arc<BTreeSet<EnrollmentId>>,
    ) -> (Self, watch::Receiver<DriverSettings<EnrollmentKey>>) {
        let context = EnrollmentContext {
            machine_id,
            organization_id,
            deployment: client.deployment().clone(),
        };
        Self::with_discovery(store, client, context, clock, changes, excluded)
    }
}

impl<Store: CredentialStore, Discovery: DiscoverySource> LinkSettingsSource<Store, Discovery> {
    /// Supplies discovery at its external boundary. Composition owns driving and stopping the task.
    pub fn with_discovery(
        store: Store,
        discovery: Discovery,
        context: EnrollmentContext,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
        changes: watch::Receiver<u64>,
        excluded: Arc<BTreeSet<EnrollmentId>>,
    ) -> (Self, watch::Receiver<DriverSettings<EnrollmentKey>>) {
        let (settings, receiver) = watch::channel(no_settings());
        (
            Self {
                store,
                discovery,
                context,
                clock,
                changes,
                excluded,
                settings,
            },
            receiver,
        )
    }

    /// Runs until the owner requests shutdown or drops the shutdown sender.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        tokio::select! {
            biased;
            _ = shutdown.wait_for(|stop| *stop) => {}
            () = self.publish_settings() => {}
        }
    }

    async fn publish_settings(mut self) {
        let mut discovery_state: Option<DiscoveryState> = None;
        let mut in_flight = None;
        let mut discovery_answer = None;
        let mut read_at = self.clock.monotonic_now();
        let mut read_due = true;
        let mut metadata_ready = false;
        let mut signal_open = true;
        loop {
            if read_due {
                let read_revision = *self.changes.borrow_and_update();
                match self.store.read_metadata().await {
                    Ok(metadata) => {
                        read_at = self.clock.monotonic_now().after(METADATA_POLL);
                        if self.observe_target(
                            target_for(&self.context, &self.excluded, &metadata),
                            &mut discovery_state,
                        ) {
                            in_flight = None;
                            discovery_answer = None;
                        }
                        // Hold the revision through publication so a local change cannot
                        // land between confirming the snapshot and publishing its address.
                        let revision = self.changes.borrow();
                        if *revision != read_revision {
                            continue;
                        }
                        metadata_ready = true;
                        if let Some(state) = &mut discovery_state
                            && let Some(answer) = discovery_answer.take()
                        {
                            let (address, retry) =
                                project_discovery(&state.target, answer, &mut state.last_good);
                            state.refresh_at =
                                retry.map(|delay| self.clock.monotonic_now().after(delay));
                            self.publish(DriverSettings {
                                binding: Some(state.target.key.clone()),
                                address,
                            });
                        }
                    }
                    Err(_) => {
                        read_at = self.clock.monotonic_now().after(METADATA_RETRY);
                        metadata_ready = false;
                        if in_flight.take().is_some()
                            && let Some(state) = &mut discovery_state
                        {
                            state.refresh_at = Some(self.clock.monotonic_now());
                        }
                    }
                }
                read_due = false;
            }
            if metadata_ready
                && let Some(state) = &mut discovery_state
                && state
                    .refresh_at
                    .is_some_and(|at| self.clock.monotonic_now() >= at)
            {
                state.refresh_at = None;
                in_flight = Some(Box::pin(self.discovery.discover()));
            }
            let refresh_at = if metadata_ready {
                discovery_state.as_ref().and_then(|state| state.refresh_at)
            } else {
                None
            };
            tokio::select! {
                biased;
                result = self.changes.changed(), if signal_open => {
                    signal_open = result.is_ok();
                    read_due = signal_open;
                }
                () = self.clock.sleep_until(read_at) => read_due = true,
                () = wait_until(self.clock.as_ref(), refresh_at) => {}
                answer = async {
                    match &mut in_flight {
                        Some(discovery) => discovery.await,
                        None => pending().await,
                    }
                } => {
                    in_flight = None;
                    discovery_answer = Some(answer);
                    read_due = true;
                }
            }
        }
    }

    fn observe_target(
        &self,
        target: MetadataTarget,
        discovery: &mut Option<DiscoveryState>,
    ) -> bool {
        if matches!((&*discovery, &target),
            (Some(state), MetadataTarget::Current(target)) if &state.target == target)
        {
            return false;
        }
        *discovery = match target {
            MetadataTarget::Unbound => {
                self.publish(no_settings());
                None
            }
            MetadataTarget::Foreign(key) => {
                self.publish(unresolved(key));
                None
            }
            MetadataTarget::Current(target) => {
                self.publish(unresolved(target.key.clone()));
                Some(DiscoveryState {
                    target,
                    last_good: None,
                    refresh_at: Some(self.clock.monotonic_now()),
                })
            }
        };
        true
    }

    fn publish(&self, next: DriverSettings<EnrollmentKey>) {
        self.settings.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            *current = next;
            true
        });
    }
}

async fn wait_until(clock: &dyn RuntimeClock, deadline: Option<ClockInstant>) {
    if let Some(deadline) = deadline {
        clock.sleep_until(deadline).await;
    } else {
        pending().await
    }
}

fn target_for(
    context: &EnrollmentContext,
    excluded: &BTreeSet<EnrollmentId>,
    metadata: &EnrollmentMetadata,
) -> MetadataTarget {
    let EnrollmentMetadata::Active {
        key, deployment, ..
    } = metadata
    else {
        return MetadataTarget::Unbound;
    };
    if excluded.contains(&key.enrollment_id) {
        return MetadataTarget::Unbound;
    }
    if key.organization_id != context.organization_id
        || key.machine_id != context.machine_id
        || deployment != &context.deployment
    {
        return MetadataTarget::Foreign(key.clone());
    }
    MetadataTarget::Current(DiscoveryTarget {
        key: key.clone(),
        deployment: deployment.clone(),
    })
}

fn no_settings() -> DriverSettings<EnrollmentKey> {
    DriverSettings {
        binding: None,
        address: Err(AddressError::DiscoveryPending),
    }
}

fn unresolved(key: EnrollmentKey) -> DriverSettings<EnrollmentKey> {
    DriverSettings {
        binding: Some(key),
        address: Err(AddressError::DiscoveryPending),
    }
}

fn project_discovery(
    target: &DiscoveryTarget,
    answer: Result<DiscoveryAnswer, SignInFailure>,
    last_good: &mut Option<DialAddress>,
) -> (Result<DialAddress, AddressError>, Option<Duration>) {
    let address = match answer {
        Ok(answer) => secure_address(target, answer),
        Err(SignInFailure::Invalid { .. }) => Err(AddressError::InvalidLocation),
        Err(SignInFailure::Unavailable { retry_after }) => {
            let delay = retry_after
                .unwrap_or(DISCOVERY_RETRY)
                .max(Duration::from_secs(1));
            return (
                last_good.clone().ok_or(AddressError::DiscoveryUnavailable),
                Some(delay),
            );
        }
    };
    *last_good = address.as_ref().ok().cloned();
    let retry = address.is_err().then_some(DISCOVERY_RETRY);
    (address, retry)
}

fn secure_address(
    target: &DiscoveryTarget,
    answer: DiscoveryAnswer,
) -> Result<DialAddress, AddressError> {
    let mut url = answer
        .link_url
        .for_organization(target.key.organization_id)
        .map_err(|_| AddressError::InvalidLocation)?;
    url.set_scheme("wss")
        .map_err(|()| AddressError::InvalidLocation)?;
    DialAddress::secure(url.as_str())
}

#[cfg(test)]
#[path = "link_settings_test.rs"]
mod tests;
