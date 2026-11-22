//! Supply the existing Mail operations to the notice planners.
use central_storage::{GuardedStatement, StoreFailure};
use fleet_contracts::MailDeliveryReady;
use fleet_runtime::FleetRuntime;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use workspace_ids::SessionRef;
use workspace_model::{
    Timestamp, WorkspaceId,
    central_mail::{NoticeItem, NoticeSource},
};

pub(crate) struct NoticeMailbox<'runtime, Host: CentralStore, Stores: WorkspaceStores>(
    pub(crate) &'runtime FleetRuntime<Host, Stores>,
);

impl<Host: CentralStore, Stores: WorkspaceStores> fleet_notices::NoticeMailbox
    for NoticeMailbox<'_, Host, Stores>
{
    async fn plan_cancel_queued_notices_held<'a>(
        &'a self,
        workspace: &'a WorkspaceId,
        cancellations: &'a [fleet_notices::NoticeCancellation],
        now: Timestamp,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        fleet_mail::plan_cancel_queued_notices_held(
            self.0.statement_host(),
            workspace,
            cancellations
                .iter()
                .map(|event| (&event.thread, event.recipient.as_ref())),
            now,
        )
        .await
    }

    fn plan_sourced_notice(
        &self,
        target: SessionRef,
        items: Vec<NoticeItem>,
        source: NoticeSource,
        now: Timestamp,
    ) -> Result<(Vec<GuardedStatement>, MailDeliveryReady), StoreFailure> {
        fleet_mail::plan_sourced_notice(target, items, source, now, || self.0.mint())
    }
}
