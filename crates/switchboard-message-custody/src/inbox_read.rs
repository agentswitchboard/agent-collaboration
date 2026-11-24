//! The recipient page and its exact, single-use read acknowledgment.

use workspace_ids::{MessageId, SessionRef};
use workspace_model::central_mail::MessageRecord;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedLocalSession(SessionRef);

impl AdmittedLocalSession {
    /// Only the session-admission seam may call this constructor. D29's v0
    /// admission stub is today's only caller.
    pub fn new(session: SessionRef) -> Self {
        Self(session)
    }

    pub(crate) fn session(&self) -> &SessionRef {
        &self.0
    }
}

pub struct RecipientInboxPage {
    messages: Vec<MessageRecord>,
    acknowledgment: RecipientInboxRead,
}

pub struct RecipientInboxRead {
    pub(crate) target: SessionRef,
    pub(crate) returned: Vec<MessageId>,
}

impl RecipientInboxPage {
    pub(crate) fn new(target: SessionRef, messages: Vec<MessageRecord>) -> Self {
        let returned = messages
            .iter()
            .map(|message| message.message_id.clone())
            .collect();
        Self {
            messages,
            acknowledgment: RecipientInboxRead { target, returned },
        }
    }

    pub fn messages(&self) -> &[MessageRecord] {
        &self.messages
    }

    /// Return the record for the exact page after its tool reply completes.
    pub fn into_acknowledgment(self) -> RecipientInboxRead {
        self.acknowledgment
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InboxReadError {
    #[error("the custody task is closed")]
    Closed,
    #[error("the read acknowledgment belongs to a different admitted session")]
    RecipientChanged,
    #[error("the inbox state changed before the read could be acknowledged")]
    ChangedSinceRead,
    #[error("the inbox read could not be applied")]
    Handover(#[from] crate::handover::HandoverError),
    #[error("the inbox could not be read")]
    Inbox(#[from] crate::inbox_error::InboxCustodyError),
}
