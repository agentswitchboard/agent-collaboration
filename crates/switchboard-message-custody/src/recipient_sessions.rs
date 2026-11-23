use std::{future::Future, pin::Pin};
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
