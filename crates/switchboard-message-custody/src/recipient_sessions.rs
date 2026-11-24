use router_clock::{ClockInstant, RuntimeClock};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};
pub use workspace_ids::SessionId;

pub const RECIPIENT_LOOKUP_BUDGET: Duration = Duration::from_secs(30);

/// The session directory's answer about one addressed session on this Mac.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecipientSessionAnswer {
    KnownOnce,
    UnknownSession,
    AmbiguousSession,
    CouldNotTell,
}

/// The Host supplies this lookup so custody can check a local recipient before it makes an inbox decision.
pub trait RecipientSessionDirectory: Send + Sync {
    /// Check whether exactly one provider knows this session, including when it is idle.
    fn query_recipient_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>>;

    /// Check using the offer's deadline. A composite lookup keeps the evidence
    /// supplied by any authority that answered before the budget ran out.
    fn query_until<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
        clock: &'lookup dyn RuntimeClock,
        deadline: ClockInstant,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                () = clock.sleep_until(deadline) => RecipientSessionAnswer::CouldNotTell,
                answer = self.query_recipient_session(session_id) => answer,
            }
        })
    }
}

/// What one authority established about the addressed session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionInventoryAnswer {
    Complete(usize),
    /// The authority may be hiding more matches.
    AtLeast(usize),
    CompleteWithManagedClaude {
        independent: usize,
    },
    AtLeastWithManagedClaude {
        independent: usize,
    },
}

impl SessionInventoryAnswer {
    const fn evidence(self) -> (usize, bool, bool) {
        match self {
            Self::Complete(count) => (count, false, true),
            Self::AtLeast(count) => (count, false, false),
            Self::CompleteWithManagedClaude { independent } => (independent, true, true),
            Self::AtLeastWithManagedClaude { independent } => (independent, true, false),
        }
    }

    /// A positive claim for the exact managed Claude owner. Both inventories
    /// may carry it when the terminal record proved its managed ancestry.
    #[must_use]
    pub const fn with_managed_claude(self) -> Self {
        let (independent, _, complete) = self.evidence();
        if complete {
            Self::CompleteWithManagedClaude { independent }
        } else {
            Self::AtLeastWithManagedClaude { independent }
        }
    }

    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        let (left, left_managed, left_complete) = self.evidence();
        let (right, right_managed, right_complete) = other.evidence();
        let independent = left.saturating_add(right);
        let merged = if left_complete && right_complete {
            Self::Complete(independent)
        } else {
            Self::AtLeast(independent)
        };
        if left_managed || right_managed {
            merged.with_managed_claude()
        } else {
            merged
        }
    }
}

const EMPTY_INVENTORY: SessionInventoryAnswer = SessionInventoryAnswer::Complete(0);
const UNAVAILABLE_INVENTORY: SessionInventoryAnswer = SessionInventoryAnswer::AtLeast(0);

pub type InventoryRead<'lookup> =
    Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>>;

/// A disjoint part of an authority. Its completed answer replaces any evidence
/// the authority could already prove before starting the read.
pub struct SessionInventoryPart<'lookup> {
    pub observed: SessionInventoryAnswer,
    pub read: InventoryRead<'lookup>,
}

/// One source of session existence evidence on this Mac.
pub trait SessionInventory: Send + Sync {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> InventoryRead<'lookup>;

    /// The deadline, when supplied by custody, uses the Host's injected clock.
    fn lookup_parts<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
        _deadline: Option<ClockInstant>,
    ) -> Vec<SessionInventoryPart<'lookup>> {
        vec![SessionInventoryPart {
            observed: UNAVAILABLE_INVENTORY,
            read: self.lookup_session(session_id),
        }]
    }
}

/// An authority that has yet to become readable during startup.
pub struct UnavailableSessionInventory;

impl SessionInventory for UnavailableSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>> {
        Box::pin(std::future::ready(UNAVAILABLE_INVENTORY))
    }
}

/// Defer while fewer than two matches are proved and any authority is
/// incomplete. Two proved claims already establish ambiguity.
pub fn combine_inventory_answers(
    answers: impl IntoIterator<Item = SessionInventoryAnswer>,
) -> RecipientSessionAnswer {
    let summary = answers
        .into_iter()
        .fold(EMPTY_INVENTORY, SessionInventoryAnswer::merge);
    let (independent, managed, complete) = summary.evidence();
    let proved = independent.saturating_add(if managed { 1 } else { 0 });
    match (proved, complete) {
        (2.., _) => RecipientSessionAnswer::AmbiguousSession,
        (_, false) => RecipientSessionAnswer::CouldNotTell,
        (1, true) => RecipientSessionAnswer::KnownOnce,
        (0, true) => RecipientSessionAnswer::UnknownSession,
    }
}

/// Resolves endpoint-free addresses against the Mac's session sources.
pub struct CombinedRecipientSessionDirectory {
    sources: RecipientSessionSources,
}

/// All of the authorities that can hold a recipient on this Mac.
pub struct RecipientSessionSources {
    pub codex: Arc<dyn SessionInventory>,
    pub managed_providers: Arc<dyn SessionInventory>,
    pub claude_terminal: Arc<dyn SessionInventory>,
}

impl CombinedRecipientSessionDirectory {
    #[must_use]
    pub fn new(sources: RecipientSessionSources) -> Self {
        Self { sources }
    }

    async fn query(
        &self,
        session_id: &SessionId,
        cutoff: Option<ClockInstant>,
        deadline: impl Future<Output = ()>,
    ) -> RecipientSessionAnswer {
        let mut parts: Vec<_> = [
            &self.sources.codex,
            &self.sources.managed_providers,
            &self.sources.claude_terminal,
        ]
        .into_iter()
        .flat_map(|source| source.lookup_parts(session_id, cutoff))
        .map(|part| (part.observed, Some(part.read)))
        .collect();
        let mut summary = combine_inventory_answers(parts.iter().map(|(answer, _)| *answer));
        let lookups = poll_fn(|context| {
            for (answer, pending) in &mut parts {
                if let Some(read) = pending
                    && let Poll::Ready(completed) = read.as_mut().poll(context)
                {
                    *answer = completed;
                    *pending = None;
                }
            }
            summary = combine_inventory_answers(parts.iter().map(|(answer, _)| *answer));
            if summary == RecipientSessionAnswer::AmbiguousSession
                || parts.iter().all(|(_, pending)| pending.is_none())
            {
                Poll::Ready(summary)
            } else {
                Poll::Pending
            }
        });
        tokio::select! {
            biased;
            answer = lookups => answer,
            () = deadline => summary,
        }
    }
}

impl RecipientSessionDirectory for CombinedRecipientSessionDirectory {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(self.query(session_id, None, std::future::pending()))
    }

    fn query_until<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
        clock: &'lookup dyn RuntimeClock,
        deadline: ClockInstant,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(self.query(session_id, Some(deadline), clock.sleep_until(deadline)))
    }
}

#[cfg(test)]
#[path = "recipient_sessions_test.rs"]
mod tests;
