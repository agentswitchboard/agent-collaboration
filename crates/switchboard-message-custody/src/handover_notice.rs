//! Local authority carried from a committed inbox claim to a managed writer.

use crate::handover::NoticeWriteOutcome;
use router_clock::ClockInstant;
use std::{future::Future, pin::Pin, sync::Arc};
pub use switchboard_machine_core_store::NoticeAttemptId;
use tokio::sync::watch;
pub use workspace_ids::{MachineId, MessageId, SessionRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticeDeferred {
    NotOpen,
    Busy,
    UnprovedRecipient,
    UnsupportedProgram,
    RunChanged,
}

pub type NoticeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An adapter can issue this only for a managed Claude run it found idle.
pub trait IdleClaudeRun: Send {
    fn offer(self: Box<Self>, notice: ClaimedNotice) -> NoticeFuture<'static, NoticeWriteOutcome>;
}

pub trait ManagedClaudeNoticePort: Send + Sync {
    fn ready<'a>(
        &'a self,
        target: &'a SessionRef,
    ) -> NoticeFuture<'a, Result<Box<dyn IdleClaudeRun>, NoticeDeferred>>;
}

/// The custody unit holds the close side. A writer holds a read guard through
/// the synchronous take, so retirement and writer take have one order.
pub struct NoticeAuthority(watch::Sender<bool>);

impl NoticeAuthority {
    pub fn new() -> Self {
        Self(watch::Sender::new(true))
    }

    pub fn close(&self) {
        self.0.send_replace(false);
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
    authority: Arc<NoticeAuthority>,
}

impl ClaimedNotice {
    pub fn new(
        target: SessionRef,
        attempt: NoticeAttemptId,
        messages: Vec<MessageId>,
        not_after: ClockInstant,
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

    /// Run the synchronous writer operation only while the custody unit
    /// retains authority. The closure must not perform async work.
    pub fn while_live<T>(&self, take: impl FnOnce() -> T) -> Option<T> {
        let live = self.authority.0.borrow();
        if !*live {
            return None;
        }
        Some(take())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("a notice claim must cover at least one inbox message")]
pub struct EmptyNoticeClaim;
