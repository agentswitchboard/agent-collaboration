use std::{future::Future, path::Path, sync::Arc};

use router_clock::RuntimeClock;
use switchboard_central_link::{
    link_driver::LinkDriver, link_lane::link_lane_channel, link_policy::LinkStatus,
};
use switchboard_link_lease::{DiscoverySource, EnrollmentLeaseSource, LinkSettingsSource};
use switchboard_machine_core_store::{
    InboxStore, MACHINE_LOCAL_DATABASE, MachineLocalOutbox, MachineLocalStore,
};
use switchboard_machine_enrollment::{
    EnrollmentContext, EnrollmentKey, MachineId, RefreshOwner, RefreshSource, SignInClient,
    SignInDeployment,
};
use switchboard_machine_enrollment_store::MachineEnrollmentStore;
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_task::{CustodyEventSink, CustodyRunGroup, custody_task},
    flush_clock::MachineFlushJitter,
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
    recipient_sessions::RecipientSessionDirectory,
};
use tokio::{sync::watch, task::JoinHandle};

use crate::{FrontRefusal, MessageFront, message_front::FrontPublication};

/// The metadata used to start a unit for an active enrollment.
pub struct ActiveEnrollment {
    pub key: EnrollmentKey,
    pub deployment: SignInDeployment,
}

/// Observations published by the driver's and custody's existing ports.
pub struct MessageUnitEvents {
    pub custody: Arc<dyn CustodyEventSink>,
    pub link_status: watch::Sender<LinkStatus>,
}

impl MessageUnitEvents {
    #[must_use]
    pub fn new(custody: Arc<dyn CustodyEventSink>) -> Self {
        Self {
            custody,
            link_status: watch::channel(LinkStatus::NotEnrolled).0,
        }
    }
}

/// Closed build failure classes, safe to publish without enrollment details.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MessageUnitBuildError {
    #[error("the machine identity could not be loaded")]
    MachineIdentity,
    #[error("the enrollment store could not be opened")]
    EnrollmentStore,
    #[error("the enrollment belongs to another machine")]
    EnrollmentMachine,
    #[error("the sign-in client could not be constructed")]
    SignInClient,
    #[error("the custody outbox could not be opened")]
    Outbox,
    #[error("the custody inbox could not be opened")]
    Inbox,
}

pub(crate) struct OwnedLinkTask {
    pub(crate) stop: watch::Sender<bool>,
    pub(crate) join: JoinHandle<()>,
}

/// Owns one unit's tasks. Call `stop` to collect their shutdown results.
#[must_use]
pub struct MessageUnit {
    pub(crate) clock: Arc<dyn RuntimeClock>,
    pub(crate) driver: OwnedLinkTask,
    pub(crate) custody: CustodyRunGroup,
    pub(crate) settings: OwnedLinkTask,
    pub(crate) lease: Arc<EnrollmentLeaseSource>,
    pub(crate) publication: FrontPublication,
    front: MessageFront,
}

type EnrollmentLinkDriver = LinkDriver<EnrollmentKey, EnrollmentLeaseSource>;

pub(crate) struct UnitLinkInputs<Discovery, Refresh, RunDriver> {
    pub(crate) context: EnrollmentContext,
    pub(crate) discovery: Discovery,
    pub(crate) refresh_owner: RefreshOwner<MachineEnrollmentStore, Refresh>,
    pub(crate) run_driver: RunDriver,
}

impl MessageUnit {
    pub async fn build(
        state_root: &Path,
        machine_id: MachineId,
        enrollment_store: &MachineEnrollmentStore,
        enrollment: ActiveEnrollment,
        clock: Arc<dyn RuntimeClock>,
        recipient_directory: Arc<dyn RecipientSessionDirectory>,
        events: MessageUnitEvents,
    ) -> Result<Self, MessageUnitBuildError> {
        if enrollment.key.machine_id != machine_id {
            return Err(MessageUnitBuildError::EnrollmentMachine);
        }
        let sign_in = SignInClient::new(enrollment.deployment.origin().clone(), clock.clone())
            .map_err(|_| MessageUnitBuildError::SignInClient)?;
        let organization_id = enrollment.key.organization_id;
        let refresh_owner = sign_in.refresh_owner(
            enrollment_store.clone(),
            machine_id.clone(),
            organization_id,
        );
        Self::assemble(
            state_root,
            enrollment_store,
            clock,
            recipient_directory,
            events,
            UnitLinkInputs {
                context: EnrollmentContext {
                    machine_id,
                    organization_id,
                    deployment: enrollment.deployment,
                },
                discovery: sign_in,
                refresh_owner,
                run_driver: LinkDriver::run,
            },
        )
        .await
    }

    pub(crate) async fn assemble<Run: Future<Output = ()> + Send + 'static>(
        state_root: &Path,
        enrollment_store: &MachineEnrollmentStore,
        clock: Arc<dyn RuntimeClock>,
        recipient_directory: Arc<dyn RecipientSessionDirectory>,
        events: MessageUnitEvents,
        link: UnitLinkInputs<
            impl DiscoverySource + 'static,
            impl RefreshSource + 'static,
            impl FnOnce(EnrollmentLinkDriver) -> Run,
        >,
    ) -> Result<Self, MessageUnitBuildError> {
        let UnitLinkInputs {
            context,
            discovery,
            refresh_owner,
            run_driver,
        } = link;
        let machine_id = context.machine_id.clone();
        let database = state_root.join(MACHINE_LOCAL_DATABASE);
        // Prepare every fallible resource before starting any of the tasks.
        let outbox_store = MachineLocalStore::open(&database)
            .await
            .map_err(|_| MessageUnitBuildError::Outbox)?;
        let inbox_store = match MachineLocalStore::open(&database).await {
            Ok(store) => store,
            Err(_) => {
                let _ = outbox_store.close().await;
                return Err(MessageUnitBuildError::Inbox);
            }
        };
        let (lane, driver_lane) = link_lane_channel();
        let custody = CustodyLink::new(
            Arc::new(lane),
            clock.clone(),
            Arc::new(MachineFlushJitter::new(&machine_id)),
            OutboxWork::new(
                Box::new(MachineLocalOutbox::new(outbox_store)),
                clock.clone(),
            ),
            RecipientInbox::new(
                InboxStore::new(inbox_store),
                machine_id.clone(),
                recipient_directory,
                clock.clone(),
            ),
        );
        let excluded = Arc::new(Default::default());
        let (settings_source, settings_updates) = LinkSettingsSource::with_discovery(
            enrollment_store.clone(),
            discovery,
            context,
            clock.clone(),
            enrollment_store.subscribe_enrollment_changes(),
            Arc::clone(&excluded),
        );
        let lease = Arc::new(EnrollmentLeaseSource::start(
            refresh_owner,
            clock.clone(),
            excluded,
        ));
        let (settings_stop, settings_shutdown) = watch::channel(false);
        let settings = OwnedLinkTask {
            stop: settings_stop,
            join: tokio::spawn(settings_source.run(settings_shutdown)),
        };
        let (driver_stop, driver_shutdown) = watch::channel(false);
        let driver = OwnedLinkTask {
            stop: driver_stop,
            join: tokio::spawn(run_driver(LinkDriver {
                machine_id,
                clock: clock.clone(),
                lease_source: lease.clone(),
                settings: settings_updates,
                shutdown: driver_shutdown,
                status: events.link_status,
                lane: driver_lane,
            })),
        };
        let (handle, custody) = custody_task(custody, events.custody);
        let (publication, front) = MessageFront::forwarding(handle);
        Ok(Self {
            clock,
            driver,
            custody,
            settings,
            lease,
            publication,
            front,
        })
    }

    #[must_use]
    pub fn front(&self) -> MessageFront {
        self.front.clone()
    }
}

impl Drop for MessageUnit {
    fn drop(&mut self) {
        let _ = self
            .publication
            .send_replace(Err(FrontRefusal::ShuttingDown));
    }
}
