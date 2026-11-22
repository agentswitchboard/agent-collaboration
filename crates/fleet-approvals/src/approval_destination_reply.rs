use std::fmt;

use approval_request_protocol::DecidedBy;

use crate::ApprovalRequestKey;

/// A destination payload can be released only for the request and recipient it was built for.
#[derive(Clone, Eq, PartialEq)]
pub struct DestinationReply<Payload> {
    request: ApprovalRequestKey,
    recipient: DecidedBy,
    payload: Payload,
}

impl<Payload> DestinationReply<Payload> {
    pub(crate) fn new(
        request: &ApprovalRequestKey,
        recipient: DecidedBy,
        payload: Payload,
    ) -> Self {
        Self {
            request: request.clone(),
            recipient,
            payload,
        }
    }

    pub fn request(&self) -> &ApprovalRequestKey {
        &self.request
    }

    pub fn recipient(&self) -> &DecidedBy {
        &self.recipient
    }

    pub fn for_recipient(
        &self,
        request: &ApprovalRequestKey,
        recipient: &DecidedBy,
    ) -> Result<&Payload, DestinationReplyMismatch> {
        if &self.request != request {
            return Err(DestinationReplyMismatch::Request);
        }
        if &self.recipient != recipient {
            return Err(DestinationReplyMismatch::Recipient);
        }
        Ok(&self.payload)
    }
}

impl<Payload> fmt::Debug for DestinationReply<Payload> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DestinationReply")
            .field("request", &self.request)
            .field("recipient", &self.recipient)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DestinationReplyMismatch {
    Request,
    Recipient,
}
