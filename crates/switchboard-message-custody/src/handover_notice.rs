//! Local authority carried from a committed inbox claim to a managed writer.

pub use crate::handover::EmptyNoticeClaim;
use crate::handover::NoticeWriteOutcome;
use router_clock::{ClockInstant, RuntimeClock};
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{future::Future, pin::Pin, sync::Arc};
pub use switchboard_machine_core_store::NoticeAttemptId;
use tokio::sync::watch;
pub use workspace_ids::{MachineId, MessageId, SessionRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticeDeferred {
    NotOpen,
    UnprovedRecipient,
    WaitingForSession,
    RunChanged,
}

pub type NoticeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A writer for an exact managed session run and a claimed notice.
pub trait ManagedNoticeRun: Send {
    fn offer(self: Box<Self>, notice: ClaimedNotice) -> NoticeFuture<'static, NoticeWriteOutcome>;
}

pub trait ManagedNoticePort: Send + Sync {
    fn ready<'a>(
        &'a self,
        target: &'a SessionRef,
    ) -> NoticeFuture<'a, Result<Box<dyn ManagedNoticeRun>, NoticeDeferred>>;
}

/// The custody unit holds the close side. A writer holds a read guard through
/// the synchronous take, so retirement and writer take have one order.
pub struct NoticeAuthority(watch::Sender<bool>, #[cfg(debug_assertions)] AtomicUsize);

impl NoticeAuthority {
    pub fn new() -> Self {
        Self(
            watch::Sender::new(true),
            #[cfg(debug_assertions)]
            AtomicUsize::new(0),
        )
    }

    pub fn close(&self) {
        self.0
            .send_if_modified(|live| std::mem::replace(live, false));
    }

    #[cfg(debug_assertions)]
    #[must_use]
    pub fn read_guard_held_for_test(&self) -> bool {
        self.1.load(Ordering::Acquire) != 0
    }
}

/// Borrows the actual watch guard, so this witness cannot outlive the lock.
#[cfg(debug_assertions)]
struct HeldNoticeRead<'guard, 'watch> {
    _held: &'guard watch::Ref<'watch, bool>,
    count: &'guard AtomicUsize,
}

#[cfg(debug_assertions)]
impl<'guard, 'watch> HeldNoticeRead<'guard, 'watch> {
    fn new(held: &'guard watch::Ref<'watch, bool>, count: &'guard AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self { _held: held, count }
    }
}

#[cfg(debug_assertions)]
impl Drop for HeldNoticeRead<'_, '_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Default for NoticeAuthority {
    fn default() -> Self {
        Self::new()
    }
}

/// One durable claim for one session. The notice derives only from its count.
pub struct ClaimedNotice {
    target: SessionRef,
    attempt: NoticeAttemptId,
    messages: Vec<MessageId>,
    not_after: ClockInstant,
    expires_at: workspace_model::Timestamp,
    authority: Arc<NoticeAuthority>,
}

impl ClaimedNotice {
    pub fn new(
        target: SessionRef,
        attempt: NoticeAttemptId,
        messages: Vec<MessageId>,
        not_after: ClockInstant,
        expires_at: workspace_model::Timestamp,
        authority: Arc<NoticeAuthority>,
    ) -> Result<Self, EmptyNoticeClaim> {
        if messages.is_empty() {
            return Err(EmptyNoticeClaim);
        }
        Ok(Self {
            target,
            attempt,
            messages,
            not_after,
            expires_at,
            authority,
        })
    }

    pub fn target(&self) -> &SessionRef {
        &self.target
    }

    pub fn attempt(&self) -> NoticeAttemptId {
        self.attempt
    }

    pub fn count(&self) -> usize {
        self.messages.len()
    }

    pub fn text(&self) -> String {
        let count = self.count();
        let noun = if count == 1 { "message" } else { "messages" };
        format!("You have {count} new {noun}. Read them with message_inbox.")
    }

    pub fn not_after(&self) -> ClockInstant {
        self.not_after
    }

    pub fn can_take_at(
        &self,
        clock: &dyn RuntimeClock,
        taken_at: workspace_model::Timestamp,
    ) -> bool {
        clock.monotonic_now() < self.not_after && taken_at < self.expires_at
    }

    /// Run the synchronous writer operation only while the custody unit
    /// retains authority. The closure must not perform async work.
    pub fn while_live<T>(&self, take: impl FnOnce() -> T) -> Option<T> {
        let live = self.authority.0.borrow();
        #[cfg(debug_assertions)]
        let _guard_witness = HeldNoticeRead::new(&live, &self.authority.1);
        if !*live {
            return None;
        }
        Some(take())
    }
}
