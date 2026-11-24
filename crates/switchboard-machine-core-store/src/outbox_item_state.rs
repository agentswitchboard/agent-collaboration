//! The outbox row's `item_state`, validated in Rust (no SQL enum constraint):
//! where a write stands, and for an ended write why it ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboxItemState {
    InOutbox,
    /// Went out in a batch whose answer was lost; back in the outbox to be
    /// resent with the same id, and may already be in central.
    SentAnswerLost,
    /// Claimed for a first transmission from InOutbox.
    Sent,
    /// On the wire again: claimed from SentAnswerLost, so an earlier attempt
    /// may already be in central even if this attempt never left the Mac.
    Resent,
    Answered,
    Refused,
    /// Ended at its deadline (outbox_deadline) with its text gone: it never
    /// left the Mac within 15 minutes.
    NotSent,
    /// Ended at its deadline with its text gone: it left the Mac but was not
    /// confirmed within 15 minutes, so it may have been applied.
    Unconfirmed,
}

impl OutboxItemState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InOutbox => "inOutbox",
            Self::SentAnswerLost => "sentAnswerLost",
            Self::Sent => "sent",
            Self::Resent => "resent",
            Self::Answered => "answered",
            Self::Refused => "refused",
            Self::NotSent => "notSent",
            Self::Unconfirmed => "unconfirmed",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "inOutbox" => Some(Self::InOutbox),
            "sentAnswerLost" => Some(Self::SentAnswerLost),
            "sent" => Some(Self::Sent),
            "resent" => Some(Self::Resent),
            "answered" => Some(Self::Answered),
            "refused" => Some(Self::Refused),
            "notSent" => Some(Self::NotSent),
            "unconfirmed" => Some(Self::Unconfirmed),
            _ => None,
        }
    }
}
