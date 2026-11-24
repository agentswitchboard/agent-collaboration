//! The inbox worker's recipient page and exact acknowledgment.

pub use workspace_ids::AdmittedLocalSession;
use workspace_ids::{MessageId, SessionRef};
use workspace_model::central_mail::MessageRecord;

/// The inbox worker returns these messages to one admitted recipient.
pub struct RecipientInboxPage {
    messages: Vec<MessageRecord>,
    acknowledgment: RecipientInboxRead,
}

/// The inbox adapter reconstructs the exact IDs that an admitted recipient acknowledges.
pub struct RecipientInboxRead {
    pub(crate) target: SessionRef,
    pub(crate) returned: Vec<MessageId>,
}

impl RecipientInboxRead {
    pub fn for_session(session: &AdmittedLocalSession, returned: Vec<MessageId>) -> Self {
        Self {
            target: session.address().clone(),
            returned,
        }
    }
}

/// The inbox worker reports the number of acknowledged IDs whose handover state changed.
pub struct InboxReadSettlement {
    pub changed: usize,
    pub unchanged: usize,
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

    pub fn into_messages(self) -> Vec<MessageRecord> {
        self.messages
    }

    /// Return the record for the exact page after its tool reply completes.
    pub fn into_acknowledgment(self) -> RecipientInboxRead {
        self.acknowledgment
    }
}

/// The inbox worker reports why it could not read a page or acknowledge its exact IDs.
#[derive(Debug, thiserror::Error)]
pub enum InboxReadError {
    #[error("the custody task is closed")]
    Closed,
    #[error("the read acknowledgment belongs to a different admitted session")]
    RecipientChanged,
    #[error("an acknowledgment id is duplicated or is not a kept direct message of this session")]
    MessageNotKept,
    #[error("the inbox state changed before the read could be acknowledged")]
    ChangedSinceRead,
    #[error("the inbox read could not be applied")]
    Handover(#[from] crate::handover::HandoverError),
    #[error("the inbox could not be read")]
    Inbox(#[from] crate::inbox_error::InboxCustodyError),
}
