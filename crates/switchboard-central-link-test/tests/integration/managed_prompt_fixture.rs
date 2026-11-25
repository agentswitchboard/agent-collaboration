use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use router_clock::RuntimeClock;
use switchboard_message_custody::{
    handover::NoticeWriteOutcome,
    handover_notice::{
        ClaimedNotice, ManagedNoticePort, ManagedNoticeRun, NoticeDeferred, NoticeFuture,
    },
};
use workspace_ids::SessionRef;

#[derive(Clone, Copy)]
/// The notice fixture picks whether the taken notice gets an immediate writer result.
pub(super) enum NoticeResultScript {
    ReturnWriterTake,
    #[cfg(debug_assertions)]
    HoldAfterWriterTake,
}

#[derive(Clone)]
/// The link tests supply this scripted managed writer and watch its captured notices.
pub(super) struct ManagedPromptPort {
    pub(super) clock: Arc<dyn RuntimeClock>,
    pub(super) enabled: Arc<AtomicBool>,
    pub(super) calls: Arc<AtomicUsize>,
    pub(super) offers: tokio::sync::mpsc::UnboundedSender<ClaimedNotice>,
    pub(super) result: NoticeResultScript,
}

impl ManagedNoticePort for ManagedPromptPort {
    fn ready<'a>(
        &'a self,
        _target: &'a SessionRef,
    ) -> NoticeFuture<'a, Result<Box<dyn ManagedNoticeRun>, NoticeDeferred>> {
        Box::pin(async {
            if self.enabled.load(Ordering::SeqCst) {
                Ok(Box::new(self.clone()) as Box<dyn ManagedNoticeRun>)
            } else {
                Err(NoticeDeferred::WaitingForSession)
            }
        })
    }
}

impl ManagedNoticeRun for ManagedPromptPort {
    fn offer(self: Box<Self>, claimed: ClaimedNotice) -> NoticeFuture<'static, NoticeWriteOutcome> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let taken_at = self.clock.utc_now();
            assert!(self.offers.send(claimed).is_ok());
            match self.result {
                NoticeResultScript::ReturnWriterTake => NoticeWriteOutcome::Written { taken_at },
                #[cfg(debug_assertions)]
                NoticeResultScript::HoldAfterWriterTake => std::future::pending().await,
            }
        })
    }
}
