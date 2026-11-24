//! One response owns the due work discovered during its event.
use std::{
    cell::RefCell,
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
};

pub type DeferredDueTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type DueBatchYield =
    Pin<Box<dyn Future<Output = Result<(), DueBatchYieldFailure>> + Send + 'static>>;
type AcceptReplyDueWork = Box<dyn FnOnce(Arc<dyn ReplyDueWorkOwner>) + Send>;

#[derive(Debug, thiserror::Error)]
#[error("the host could not schedule a turn between due batches")]
pub struct DueBatchYieldFailure;

pub trait ReplyDueWorkOwner: Send + Sync {
    /// Schedule a claim once the transport's writer has accepted its reply.
    fn run_after_reply(&self, task: DeferredDueTask);
}

#[must_use]
pub struct ReplyDueWork(Vec<AcceptReplyDueWork>);

impl ReplyDueWork {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn handoff(self, owner: Arc<dyn ReplyDueWorkOwner>) {
        for accept in self.0 {
            accept(owner.clone());
        }
    }
}

thread_local! {
    static REPLY_DUE_WORK: RefCell<Option<Vec<AcceptReplyDueWork>>> = const { RefCell::new(None) };
}

/// The dynamic scope exists only during each poll. Pending futures carry their
/// own queue when they leave the thread, including when resumed on another one.
struct ReplyPollScope<'a> {
    queued: &'a mut Vec<AcceptReplyDueWork>,
    previous: Option<Vec<AcceptReplyDueWork>>,
}

impl<'a> ReplyPollScope<'a> {
    fn enter(queued: &'a mut Vec<AcceptReplyDueWork>) -> Self {
        let previous = REPLY_DUE_WORK.replace(Some(std::mem::take(queued)));
        Self { queued, previous }
    }
}

impl Drop for ReplyPollScope<'_> {
    fn drop(&mut self) {
        if let Some(queued) = REPLY_DUE_WORK.replace(self.previous.take()) {
            *self.queued = queued;
        }
    }
}

pub fn collect_reply_due_work<Reply>(
    reply: impl Future<Output = Reply>,
) -> impl Future<Output = (Reply, ReplyDueWork)> {
    // A protocol dispatch can be large. Box it once in the shared scope.
    let mut reply = Box::pin(reply);
    let mut queued = Vec::new();
    poll_fn(move |context| {
        let scope = ReplyPollScope::enter(&mut queued);
        let response = reply.as_mut().poll(context);
        drop(scope);
        response.map(|reply| (reply, ReplyDueWork(std::mem::take(&mut queued))))
    })
}

pub fn has_reply_due_scope() -> bool {
    REPLY_DUE_WORK.with(|pending| pending.borrow().is_some())
}

pub fn queue_reply_due_work(accept: impl FnOnce(Arc<dyn ReplyDueWorkOwner>) + Send + 'static) {
    REPLY_DUE_WORK.with(|pending| {
        if let Some(queued) = pending.borrow_mut().as_mut() {
            queued.push(Box::new(accept));
        }
    });
}
