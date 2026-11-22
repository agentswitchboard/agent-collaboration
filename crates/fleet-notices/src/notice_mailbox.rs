//! The queue operations Core supplies to Notices from Mail.
use central_storage::{GuardedStatement, StoreFailure};
use fleet_contracts::MailDeliveryReady;
use std::future::Future;
use workspace_ids::SessionRef;
use workspace_model::{
    ActorIdentityId, ThreadId, Timestamp, WorkspaceId,
    central_mail::{NoticeItem, NoticeSource},
};

/// A cancellation in the order the workspace recorded it.
pub struct NoticeCancellation {
    pub thread: ThreadId,
    pub recipient: Option<ActorIdentityId>,
}

/// The caller applies Mail plans under its Central turn.
pub trait NoticeMailbox: Sync {
    /// The caller holds the Central turn through the applying batch.
    fn plan_cancel_queued_notices_held<'a>(
        &'a self,
        workspace: &'a WorkspaceId,
        cancellations: &'a [NoticeCancellation],
        now: Timestamp,
    ) -> impl Future<Output = Result<Vec<GuardedStatement>, StoreFailure>> + Send + 'a;

    /// Plan a sourced notice INSERT and return its paired delivery hint.
    fn plan_sourced_notice(
        &self,
        target: SessionRef,
        items: Vec<NoticeItem>,
        source: NoticeSource,
        now: Timestamp,
    ) -> Result<(Vec<GuardedStatement>, MailDeliveryReady), StoreFailure>;
}
