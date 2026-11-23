//! Typed custody outcomes delivered to the command worker's sink.

use central_link_client::LaneFrameSizeError;
use central_link_protocol::{ProtocolError, ReplyPayload};
use switchboard_machine_core_store::{EndedWrite, OutboxStoreError, StoredSenderOutcome};
use workspace_ids::RequestId;
use workspace_model::Caller;

use crate::outbox_work::OutboxWorkError;

/// A checked answer custody persisted for one member of its own flush.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlushedWriteAnswer {
    pub request: RequestId,
    pub caller: Caller,
    pub reply: ReplyPayload,
}

/// Custody reports the original caller's durably applied Central delivery notice to its sink.
#[derive(Debug)]
pub struct AppliedSenderNotice {
    pub caller: Caller,
    pub notice: StoredSenderOutcome,
}

/// A problem custody observed while settling a batch or returning a claim.
#[derive(Debug, thiserror::Error)]
pub enum BatchDispatchError {
    #[error("the outbox batch could not be prepared")]
    Work(#[from] OutboxWorkError),
    #[error("the outbox batch failed wire validation")]
    Wire(#[from] ProtocolError),
    #[error("the outbox batch could not be measured")]
    WireSize(#[from] LaneFrameSizeError),
    #[error("a stored write does not fit alone in a link frame")]
    OversizeClaim,
    #[error("the lane returned results outside the current batch's order or identity")]
    ResultsMismatch,
    #[error("the lane's reply does not settle request {0}")]
    UnsettledReply(RequestId),
    #[error("the store did not update request {request}")]
    StoreMember {
        request: RequestId,
        #[source]
        error: OutboxStoreError,
    },
    #[error("batch preparation failed after completing local work")]
    Interrupted {
        completed: Box<FlushReport>,
        #[source]
        error: Box<Self>,
    },
}

/// One batch's results, kept by custody for the outcome sink.
#[derive(Debug, Default)]
pub struct FlushReport {
    pub answered: Vec<FlushedWriteAnswer>,
    pub sender_notices: Vec<AppliedSenderNotice>,
    pub ended: Vec<EndedWrite>,
    pub issues: Vec<BatchDispatchError>,
}

impl FlushReport {
    pub(crate) fn is_empty(&self) -> bool {
        self.answered.is_empty()
            && self.sender_notices.is_empty()
            && self.ended.is_empty()
            && self.issues.is_empty()
    }

    pub(crate) fn append(&mut self, other: Self) {
        self.answered.extend(other.answered);
        self.sender_notices.extend(other.sender_notices);
        self.ended.extend(other.ended);
        self.issues.extend(other.issues);
    }
}
