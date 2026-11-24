use std::{collections::BTreeSet, future::Future, path::PathBuf, sync::Arc};

use futures_util::{
    FutureExt as _,
    future::{BoxFuture, Shared},
};
use router_clock::RuntimeClock;
use switchboard_central_link::{
    link_driver::LinkDriver, link_lane::link_lane_channel, link_policy::LinkStatus,
};
use switchboard_link_lease::{DiscoverySource, EnrollmentLeaseSource, LinkSettingsSource};
use switchboard_machine_core_store::{
    InboxStore, MACHINE_LOCAL_DATABASE, MachineLocalOutbox, MachineLocalStore,
};
use switchboard_machine_enrollment::{
    EnrollmentContext, EnrollmentId, EnrollmentKey, RefreshOwner, RefreshSource, SignInClient,
};
use switchboard_machine_enrollment_store::MachineEnrollmentStore;
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_task::{CustodyEventSink, CustodyRunGroup, CustodyTaskHandle, custody_task},
    flush_clock::MachineFlushJitter,
    handover_notice::ManagedClaudeNoticePort,
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
    recipient_sessions::RecipientSessionDirectory,
};
use tokio::{
    sync::watch,
    task::{AbortHandle, JoinHandle},
};

pub(crate) struct UnitBuildInputs {
    pub(crate) root: PathBuf,
    pub(crate) store: MachineEnrollmentStore,
    pub(crate) context: EnrollmentContext,
    pub(crate) excluded: BTreeSet<EnrollmentId>,
    pub(crate) clock: Arc<dyn RuntimeClock>,
    pub(crate) recipients: Arc<dyn RecipientSessionDirectory>,
    pub(crate) custody_events: Arc<dyn CustodyEventSink>,
    pub(crate) notice_port: Option<Arc<dyn ManagedClaudeNoticePort>>,
    pub(crate) link_status: watch::Sender<LinkStatus>,
}

/// Closed build failure classes, safe to publish without enrollment details.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum MessageUnitBuildError {
    #[error("the sign-in client could not be constructed")]
    SignInClient,
    #[error("the custody outbox could not be opened")]
    Outbox,
    #[error("the custody inbox could not be opened")]
    Inbox,
}

pub(crate) struct OwnedLinkTask {
    pub(crate) stop: watch::Sender<bool>,
    pub(crate) abort: AbortHandle,
    pub(crate) joined: Shared<BoxFuture<'static, ()>>,
}

impl OwnedLinkTask {
    fn new(
        stop: watch::Sender<bool>,
        task: JoinHandle<()>,
        on_join: impl FnOnce() + Send + 'static,
    ) -> Self {
        let abort = task.abort_handle();
        let joined = async move {
            let _joined = task.await;
            on_join();
        }
        .boxed()
        .shared();
        Self {
            stop,
            abort,
            joined,
        }
    }
}

/// Handles to one generation's real parts.
pub(crate) struct MessageUnit {
    pub(crate) driver: OwnedLinkTask,
    pub(crate) custody: Arc<CustodyRunGroup>,
    pub(crate) settings: OwnedLinkTask,
    pub(crate) lease: Arc<EnrollmentLeaseSource>,
    pub(crate) front: CustodyTaskHandle,
}

type EnrollmentLinkDriver = LinkDriver<EnrollmentKey, EnrollmentLeaseSource>;

pub(crate) struct UnitLinkInputs<Discovery, Refresh, RunDriver> {
    pub(crate) discovery: Discovery,
    pub(crate) refresh_owner: RefreshOwner<MachineEnrollmentStore, Refresh>,
    pub(crate) run_driver: RunDriver,
}

impl MessageUnit {
    pub(crate) async fn build(inputs: UnitBuildInputs) -> Result<Self, MessageUnitBuildError> {
        let sign_in = SignInClient::new(
            inputs.context.deployment.origin().clone(),
            inputs.clock.clone(),
        )
        .map_err(|_| MessageUnitBuildError::SignInClient)?;
        let refresh_owner = sign_in.refresh_owner(
            inputs.store.clone(),
            inputs.context.machine_id.clone(),
            inputs.context.organization_id,
        );
        Self::assemble(
            inputs,
            UnitLinkInputs {
                discovery: sign_in,
                refresh_owner,
                run_driver: LinkDriver::run,
            },
        )
        .await
    }

    pub(crate) async fn assemble<Run: Future<Output = ()> + Send + 'static>(
        inputs: UnitBuildInputs,
        link: UnitLinkInputs<
            impl DiscoverySource + 'static,
            impl RefreshSource + 'static,
            impl FnOnce(EnrollmentLinkDriver) -> Run,
        >,
    ) -> Result<Self, MessageUnitBuildError> {
        let UnitBuildInputs {
            root,
            store,
            context,
            excluded,
            clock,
            recipients,
            custody_events,
            notice_port,
            link_status,
        } = inputs;
        let UnitLinkInputs {
            discovery,
            refresh_owner,
            run_driver,
        } = link;
        let machine_id = context.machine_id.clone();
        let database = root.join(MACHINE_LOCAL_DATABASE);
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
                recipients,
                clock.clone(),
            ),
        );
        let custody = match notice_port {
            Some(port) => custody.with_notice_port(port),
            None => custody,
        };
        let excluded = Arc::new(excluded);
        let (settings_source, settings_updates) = LinkSettingsSource::with_discovery(
            store.clone(),
            discovery,
            context,
            clock.clone(),
            store.subscribe_enrollment_changes(),
            Arc::clone(&excluded),
        );
        let lease = Arc::new(EnrollmentLeaseSource::start(
            refresh_owner,
            clock.clone(),
            excluded,
        ));
        let (settings_stop, settings_shutdown) = watch::channel(false);
        let settings = OwnedLinkTask::new(
            settings_stop,
            tokio::spawn(settings_source.run(settings_shutdown)),
            || {},
        );
        let (driver_stop, driver_shutdown) = watch::channel(false);
        let driver_lease = lease.clone();
        let driver = OwnedLinkTask::new(
            driver_stop,
            tokio::spawn(run_driver(LinkDriver {
                machine_id,
                clock: clock.clone(),
                lease_source: lease.clone(),
                settings: settings_updates,
                shutdown: driver_shutdown,
                status: link_status,
                lane: driver_lane,
            })),
            move || driver_lease.seal_ending_reports(),
        );
        let (front, custody) = custody_task(custody, custody_events);
        Ok(Self {
            driver,
            custody: Arc::new(custody),
            settings,
            lease,
            front,
        })
    }
}
