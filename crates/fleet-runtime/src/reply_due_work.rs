//! One response owns the due work discovered during its event.
use std::{cell::RefCell, future::Future, pin::Pin};

pub type DeferredDueTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type DueBatchYield =
    Pin<Box<dyn Future<Output = Result<(), DueBatchYieldFailure>> + Send + 'static>>;

#[derive(Debug, thiserror::Error)]
#[error("the host could not schedule a turn between due batches")]
pub struct DueBatchYieldFailure;

pub trait ReplyDueWorkOwner: Send + Sync {
    /// Take work after the transport has staged the reply for delivery.
    fn run_after_reply(&self, work: ReplyDueWork);

    /// Give the host a turn outside of any applying transaction.
    fn yield_due_batches(&self) -> DueBatchYield;
}

#[must_use]
pub struct ReplyDueWork(Vec<DeferredDueTask>);

impl ReplyDueWork {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn handoff(self, owner: &impl ReplyDueWorkOwner) {
        if !self.0.is_empty() {
            owner.run_after_reply(self);
        }
    }

    pub async fn run(self) {
        for task in self.0 {
            task.await;
        }
    }
}

tokio::task_local! {
    static REPLY_DUE_WORK: RefCell<Vec<DeferredDueTask>>;
}

pub fn collect_reply_due_work<Reply>(
    reply: impl Future<Output = Reply>,
) -> impl Future<Output = (Reply, ReplyDueWork)> {
    // The reply may contain a whole protocol dispatch. Keep the scoped future
    // small so each transport can retain it while handling independent events.
    let reply = Box::pin(reply);
    REPLY_DUE_WORK.scope(RefCell::default(), async move {
        let reply = reply.await;
        (reply, ReplyDueWork(REPLY_DUE_WORK.with(|work| work.take())))
    })
}

pub fn has_reply_due_scope() -> bool {
    REPLY_DUE_WORK.try_with(|_| ()).is_ok()
}

pub fn queue_reply_due_work(work: DeferredDueTask) {
    let _ = REPLY_DUE_WORK.try_with(|pending| pending.borrow_mut().push(work));
}

pub fn yield_shared_due_batches() -> DueBatchYield {
    Box::pin(async {
        tokio::task::yield_now().await;
        Ok(())
    })
}
