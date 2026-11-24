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
}

impl SessionInventoryAnswer {
    const fn proved(self) -> usize {
        match self {
            Self::Complete(count) | Self::AtLeast(count) => count,
        }
    }

    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        let proved = self.proved().saturating_add(other.proved());
        match (self, other) {
            (Self::AtLeast(_), _) | (_, Self::AtLeast(_)) => Self::AtLeast(proved),
            _ => Self::Complete(proved),
        }
    }
}

const EMPTY_INVENTORY: SessionInventoryAnswer = SessionInventoryAnswer::Complete(0);
const UNAVAILABLE_INVENTORY: SessionInventoryAnswer = SessionInventoryAnswer::AtLeast(0);

pub type InventoryRead<'lookup> =
    Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>>;

/// Which role this lookup plays in resolving the addressed session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionInventorySource {
    Independent,
    ManagedClaude,
    ClaudeTerminal,
}

/// A disjoint part of an authority. Its completed answer replaces any evidence
/// the authority could already prove before starting the read.
pub struct SessionInventoryPart<'lookup> {
    pub source: SessionInventorySource,
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
            source: SessionInventorySource::Independent,
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
    match summary {
        SessionInventoryAnswer::Complete(2..) | SessionInventoryAnswer::AtLeast(2..) => {
            RecipientSessionAnswer::AmbiguousSession
        }
        SessionInventoryAnswer::AtLeast(_) => RecipientSessionAnswer::CouldNotTell,
        SessionInventoryAnswer::Complete(1) => RecipientSessionAnswer::KnownOnce,
        SessionInventoryAnswer::Complete(0) => RecipientSessionAnswer::UnknownSession,
    }
}

fn resolve_recipient_parts(
    parts: impl IntoIterator<Item = (SessionInventorySource, SessionInventoryAnswer)>,
) -> RecipientSessionAnswer {
    let mut independent = EMPTY_INVENTORY;
    let mut managed_claude = EMPTY_INVENTORY;
    let mut claude_terminal = EMPTY_INVENTORY;
    for (source, answer) in parts {
        let slot = match source {
            SessionInventorySource::Independent => &mut independent,
            SessionInventorySource::ManagedClaude => &mut managed_claude,
            SessionInventorySource::ClaudeTerminal => &mut claude_terminal,
        };
        *slot = slot.merge(answer);
    }
    combine_inventory_answers([
        independent,
        prefer_managed_claude(managed_claude, claude_terminal),
    ])
}

/// A managed Claude claim owns this Mac's address. A terminal sighting of the
/// same ID has no separate address, even when someone explicitly reuses the ID
/// in another project (Claude Code 2.1.331), or opens the pre-clear conversation
/// after a context reset kept the ACP ID but minted a new native ID
/// (claude-agent-acp 0.81.0). The Lead accepted both cases.
fn prefer_managed_claude(
    managed: SessionInventoryAnswer,
    terminal: SessionInventoryAnswer,
) -> SessionInventoryAnswer {
    let terminal_count = match managed {
        SessionInventoryAnswer::Complete(0) => return terminal,
        // Until the managed lookup can prove absence, multiple terminal sightings
        // establish only one guaranteed Claude claim.
        SessionInventoryAnswer::AtLeast(0) => terminal.proved().min(1),
        SessionInventoryAnswer::Complete(_) | SessionInventoryAnswer::AtLeast(_) => 0,
    };
    let terminal = match terminal {
        SessionInventoryAnswer::Complete(_) => SessionInventoryAnswer::Complete(terminal_count),
        SessionInventoryAnswer::AtLeast(_) => SessionInventoryAnswer::AtLeast(terminal_count),
    };
    managed.merge(terminal)
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
        .map(|part| (part.source, part.observed, Some(part.read)))
        .collect();
        let mut summary =
            resolve_recipient_parts(parts.iter().map(|(source, answer, _)| (*source, *answer)));
        let lookups = poll_fn(|context| {
            for (_, answer, pending) in &mut parts {
                if let Some(read) = pending
                    && let Poll::Ready(completed) = read.as_mut().poll(context)
                {
                    *answer = completed;
                    *pending = None;
                }
            }
            summary =
                resolve_recipient_parts(parts.iter().map(|(source, answer, _)| (*source, *answer)));
            if summary == RecipientSessionAnswer::AmbiguousSession
                || parts.iter().all(|(_, _, pending)| pending.is_none())
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
