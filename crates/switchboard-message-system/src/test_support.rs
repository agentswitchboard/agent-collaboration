//! External ports for sending through a real Central fixture with local credentials.

use std::{future::Future, sync::Arc};

use router_clock::RuntimeClock;
use switchboard_central_link::link_driver::LinkDriver;
use switchboard_link_lease::{DiscoverySource, EnrollmentLeaseSource};
use switchboard_machine_enrollment::{EnrollmentKey, MachineId, RefreshOwner, RefreshSource};
use switchboard_machine_enrollment_store::MachineEnrollmentStore;
use switchboard_message_custody::recipient_sessions::RecipientSessionDirectory;

use crate::{
    MessageRoots, MessageSystem, MessageSystemEvents,
    interpreter::{MessageInputs, OpenedEnrollment},
    message_unit::{MessageUnit, UnitLinkInputs},
};

type FixtureDriver = LinkDriver<EnrollmentKey, EnrollmentLeaseSource>;

pub struct LinkFixture<Discovery, Refresh, RunDriver> {
    pub discovery: Discovery,
    pub refresh: Refresh,
    pub run_driver: RunDriver,
}

/// Runs the production interpreter with fixture discovery and token issuance
/// at sign-in's boundary and with a caller-owned enrollment store.
pub fn start_with_link_fixture<Run: Future<Output = ()> + Send + 'static>(
    roots: MessageRoots,
    machine_id: MachineId,
    store: MachineEnrollmentStore,
    clock: Arc<dyn RuntimeClock>,
    recipients: Arc<dyn RecipientSessionDirectory>,
    events: MessageSystemEvents,
    fixture: LinkFixture<
        impl DiscoverySource + Clone + 'static,
        impl RefreshSource + Clone + 'static,
        impl Fn(FixtureDriver) -> Run + Send + Sync + 'static,
    >,
) -> MessageSystem {
    let fixture = Arc::new(fixture);
    MessageSystem::start_with_parts(
        MessageInputs {
            roots,
            clock,
            recipients,
            events,
        },
        Some(OpenedEnrollment::new(machine_id, store)),
        move |inputs| {
            let fixture = fixture.clone();
            async move {
                let refresh_owner = RefreshOwner::new(
                    inputs.store.clone(),
                    fixture.refresh.clone(),
                    inputs.clock.clone(),
                    inputs.context.clone(),
                );
                MessageUnit::assemble(
                    inputs,
                    UnitLinkInputs {
                        discovery: fixture.discovery.clone(),
                        refresh_owner,
                        run_driver: |driver| (fixture.run_driver)(driver),
                    },
                )
                .await
            }
        },
    )
}
