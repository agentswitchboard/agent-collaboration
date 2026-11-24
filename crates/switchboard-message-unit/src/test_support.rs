//! Test substitution at the unit's two external boundaries.

use std::{future::Future, path::Path, sync::Arc};

use router_clock::RuntimeClock;
use switchboard_central_link::link_driver::LinkDriver;
use switchboard_link_lease::{DiscoverySource, EnrollmentLeaseSource};
use switchboard_machine_enrollment::{
    EnrollmentContext, EnrollmentKey, RefreshOwner, RefreshSource,
};
use switchboard_machine_enrollment_store::MachineEnrollmentStore;
use switchboard_message_custody::recipient_sessions::RecipientSessionDirectory;

use crate::{MessageRuntime, MessageUnit, MessageUnitEvents, message_unit::UnitLinkInputs};

type FixtureDriver = LinkDriver<EnrollmentKey, EnrollmentLeaseSource>;

pub struct LinkFixture<Discovery, Refresh, RunDriver> {
    pub discovery: Discovery,
    pub refresh: Refresh,
    pub run_driver: RunDriver,
}

/// Runs the Host's one-shot enrollment decision using a fixture store and transport.
pub async fn start_with_link_fixture<Run: Future<Output = ()> + Send + 'static>(
    root: &Path,
    store: MachineEnrollmentStore,
    clock: Arc<dyn RuntimeClock>,
    recipients: Arc<dyn RecipientSessionDirectory>,
    events: MessageUnitEvents,
    fixture: LinkFixture<
        impl DiscoverySource + 'static,
        impl RefreshSource + 'static,
        impl FnOnce(FixtureDriver) -> Run,
    >,
) -> MessageRuntime {
    let unit_store = store.clone();
    MessageRuntime::start_for_enrollment(store, |enrollment| async move {
        let context = EnrollmentContext {
            machine_id: enrollment.key.machine_id,
            organization_id: enrollment.key.organization_id,
            deployment: enrollment.deployment,
        };
        let refresh_owner = RefreshOwner::new(
            unit_store.clone(),
            fixture.refresh,
            clock.clone(),
            context.clone(),
        );
        MessageUnit::assemble(
            root,
            &unit_store,
            clock,
            recipients,
            events,
            UnitLinkInputs {
                context,
                discovery: fixture.discovery,
                refresh_owner,
                run_driver: fixture.run_driver,
            },
        )
        .await
    })
    .await
}
