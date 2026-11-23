//! Caller-scoped status above the Mac store and the central request replay read.

use central_link_client::{CorrelatedStatusReply, LinkLaneDisposition, RequestStatusCommand};
use central_link_protocol::{
    CorrelationId, ReplyPayload, RequestScope, RequestStatusAnswer, RequestStatusQuery,
};
use runtime_primitives::SyncTime;
use switchboard_machine_core_store::{
    OutboxCancellation, OutboxRefusal, OutboxRequestKey, OutboxRequestStatus, OutboxScope,
    StoredSenderOutcome,
};
use workspace_ids::MessageId;
use workspace_model::{
    Caller, ListCursor, ListOrder, ListQuery, RefusalReason, WorkspaceAnswer, WorkspaceOperation,
    WorkspaceReply, WorkspaceRequest, WorkspaceResult,
    central_mail::{MessageBody, MessageRecord, MessageSender, MessageStatus},
};

use crate::custody_submission::{Submission, SubmissionPayload, accepted_reply};

/// Custody's answer to the original caller's request-status query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CustodyRequestStatus {
    InOutbox { flush_at: SyncTime },
    Sent,
    Answered(ReplyPayload),
    Refused(OutboxRefusal),
    Ended { reason: OutboxCancellation },
    NotFound,
    StatusUnavailable,
}

/// Custody reports this error when central cannot give a checked sender-scoped message status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MessageStatusError {
    #[error("message status is unavailable")]
    StatusUnavailable,
}

pub(crate) enum PreparedRequestStatus {
    Complete(CustodyRequestStatus),
    Remote(RequestStatusCommand),
}

pub(crate) fn status_key(query: &RequestStatusQuery) -> OutboxRequestKey {
    let scope = match &query.scope {
        RequestScope::Organization => OutboxScope::Organization,
        RequestScope::Workspace(workspace) => OutboxScope::Workspace(workspace.clone()),
    };
    OutboxRequestKey {
        request_id: query.request_id.clone(),
        scope,
    }
}

pub(crate) fn local_status(status: OutboxRequestStatus) -> Option<CustodyRequestStatus> {
    match status {
        OutboxRequestStatus::NotFound => None,
        OutboxRequestStatus::InOutbox { flush_at_ms } => Some(CustodyRequestStatus::InOutbox {
            flush_at: SyncTime::from_unix_millis(flush_at_ms),
        }),
        OutboxRequestStatus::Sent => Some(CustodyRequestStatus::Sent),
        OutboxRequestStatus::Answered(accepted) => {
            Some(CustodyRequestStatus::Answered(accepted_reply(accepted)))
        }
        OutboxRequestStatus::Refused(reason) => Some(CustodyRequestStatus::Refused(reason)),
        OutboxRequestStatus::Ended { reason } => Some(CustodyRequestStatus::Ended { reason }),
    }
}

pub(crate) fn remote_status(
    correlation: &CorrelationId,
    disposition: LinkLaneDisposition<CorrelatedStatusReply>,
) -> CustodyRequestStatus {
    let LinkLaneDisposition::Answered {
        reply: CorrelatedStatusReply {
            correlation_id,
            result,
        },
    } = disposition
    else {
        return CustodyRequestStatus::StatusUnavailable;
    };
    if &correlation_id != correlation {
        return CustodyRequestStatus::StatusUnavailable;
    }
    match *result {
        RequestStatusAnswer::Answered(reply) => CustodyRequestStatus::Answered(reply),
        RequestStatusAnswer::Refused(reason) => {
            CustodyRequestStatus::Refused(OutboxRefusal::Central(reason))
        }
        RequestStatusAnswer::NotFound => CustodyRequestStatus::NotFound,
    }
}

pub(crate) fn message_query(
    message_id: MessageId,
) -> Result<SubmissionPayload, MessageStatusError> {
    Ok(SubmissionPayload::Workspace(WorkspaceRequest {
        request_id: None,
        workspace_id: None,
        operation: WorkspaceOperation::List {
            query: ListQuery::MessageStatus { message_id },
            cursor: ListCursor::start(ListOrder::OldestFirst),
            limit: 1_u16
                .try_into()
                .map_err(|_| MessageStatusError::StatusUnavailable)?,
        },
    }))
}

pub(crate) fn message_status_reply(
    submission: Submission,
) -> Result<Option<MessageStatus>, MessageStatusError> {
    match submission {
        Submission::Answered(ReplyPayload::Workspace(WorkspaceReply {
            result: WorkspaceResult::Done(WorkspaceAnswer::MessageStatus { status }),
            ..
        })) => Ok(Some(status)),
        Submission::Answered(ReplyPayload::Workspace(WorkspaceReply {
            result:
                WorkspaceResult::NotFound { .. }
                | WorkspaceResult::Refused(RefusalReason::NotFound { .. }),
            ..
        })) => Ok(None),
        _ => Err(MessageStatusError::StatusUnavailable),
    }
}

pub(crate) fn retained_delivery_notice(
    record: &MessageRecord,
) -> Option<(Caller, StoredSenderOutcome)> {
    let MessageBody::DeliveryOutcome {
        message_id,
        request_id,
        outcome,
    } = &record.body
    else {
        return None;
    };
    (record.sender == MessageSender::Central).then(|| {
        (
            Caller::Session(record.target.clone()),
            StoredSenderOutcome {
                inbox_message_id: record.message_id.clone(),
                message_id: message_id.clone(),
                request_id: request_id.clone(),
                outcome: *outcome,
                reported_at: record.created_at,
            },
        )
    })
}
