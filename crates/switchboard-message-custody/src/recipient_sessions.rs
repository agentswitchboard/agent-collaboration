use std::{future::Future, pin::Pin, sync::Arc};
use workspace_ids::SessionId;

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
}

/// The number of distinct sessions a source can prove and whether it finished
/// checking the places it owns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionInventoryAnswer {
    pub at_least: usize,
    pub complete: bool,
}

impl SessionInventoryAnswer {
    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        Self {
            at_least: self.at_least.saturating_add(other.at_least),
            complete: self.complete && other.complete,
        }
    }
}

const EMPTY_INVENTORY: SessionInventoryAnswer = SessionInventoryAnswer {
    at_least: 0,
    complete: true,
};

/// One source of session existence evidence on this Mac.
pub trait SessionInventory: Send + Sync {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = SessionInventoryAnswer> + Send + 'lookup>>;
}

pub fn combine_inventory_answers(
    answers: impl IntoIterator<Item = SessionInventoryAnswer>,
) -> RecipientSessionAnswer {
    let summary = answers
        .into_iter()
        .fold(EMPTY_INVENTORY, SessionInventoryAnswer::merge);
    match (summary.at_least, summary.complete) {
        (2.., _) => RecipientSessionAnswer::AmbiguousSession,
        (_, false) => RecipientSessionAnswer::CouldNotTell,
        (1, true) => RecipientSessionAnswer::KnownOnce,
        (0, true) => RecipientSessionAnswer::UnknownSession,
    }
}

/// Resolves endpoint-free addresses against the Mac's session sources.
pub struct CombinedRecipientSessionDirectory {
    sources: Vec<Arc<dyn SessionInventory>>,
}

impl CombinedRecipientSessionDirectory {
    #[must_use]
    pub fn new(sources: Vec<Arc<dyn SessionInventory>>) -> Self {
        Self { sources }
    }
}

impl RecipientSessionDirectory for CombinedRecipientSessionDirectory {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            let mut summary = EMPTY_INVENTORY;
            for source in &self.sources {
                summary = summary.merge(source.lookup_session(session_id).await);
            }
            combine_inventory_answers([summary])
        })
    }
}

#[cfg(test)]
#[path = "recipient_sessions_test.rs"]
mod tests;
