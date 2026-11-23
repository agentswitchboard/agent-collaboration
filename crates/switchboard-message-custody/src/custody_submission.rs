//! The tool-facing submission seam above the outbox.

use central_link_client::{CorrelatedRequestReply, LaneFrameSizeError, LinkLaneDisposition};
use central_link_protocol::{
    FreshCorrelationId, LinkRequest, ProtocolError, ReplyPayload, RequestPayload, RequestScope,
};
use runtime_primitives::SyncTime;
use switchboard_machine_core_store::{
    NewOutboxItem, OutboxAccepted, OutboxInsert, OutboxQueuedWrite, OutboxRefusal,
    OutboxRequestStatus, OutboxScope, OutboxStoreError,
};
use workspace_ids::RequestId;
use workspace_model::{
    Caller, ItemRef, Timestamp, WorkspaceAnswer, WorkspaceOperation, WorkspaceReply,
    WorkspaceRequest, WorkspaceResult,
    central_mail::{
        CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
        SendMessageRequest,
    },
};

use crate::{
    outbox_wire::{fits_one_member, request_for_write},
    outbox_work::OutboxWork,
};

/// A tool supplies an unstamped send, a workspace operation or an immediate mail operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmissionPayload {
    SendMessage {
        request: RequestId,
        message: SendMessageRequest,
    },
    Workspace(WorkspaceRequest),
    Mail(CentralMailRequest),
}

/// Custody gives this receipt after an outbox insert, with the write's first eligible minute mark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxReceipt {
    pub id: RequestId,
    pub flush_at: SyncTime,
}

/// Custody's immediate answer to a tool submission.
#[derive(Debug)]
pub enum Submission {
    Answered(ReplyPayload),
    InOutbox(OutboxReceipt),
    Refused(SubmitRefusal),
    Uncertain {
        request: RequestId,
        scope: RequestScope,
    },
    ReadAnswerLost,
}

/// A definite refusal returned by the tool submission seam.
#[derive(Debug)]
pub enum SubmitRefusal {
    CentralUnavailable,
    OutboxUnavailable,
    RequestIdConflict,
    UnstampedMessageRequired,
    TooLargeToSend,
    InvalidSendTime,
    UnmeasurableWrite(LaneFrameSizeError),
    InvalidRequest(ProtocolError),
}

pub(crate) enum SubmissionRoute {
    Outbox {
        request: RequestId,
        payload: OutboxQueuedWrite,
    },
    Immediate(RequestPayload),
}

pub(crate) fn choose_submission_route(
    payload: SubmissionPayload,
) -> Result<SubmissionRoute, SubmitRefusal> {
    match payload {
        SubmissionPayload::SendMessage { request, message } => Ok(SubmissionRoute::Outbox {
            payload: OutboxQueuedWrite::SendMessage {
                request_id: request.clone(),
                message,
            },
            request,
        }),
        SubmissionPayload::Workspace(request)
            if matches!(request.operation, WorkspaceOperation::ThreadPost { .. }) =>
        {
            let Some(request_id) = request.request_id.clone() else {
                return Err(SubmitRefusal::InvalidRequest(
                    ProtocolError::RequestIdPolicy { field: "requestId" },
                ));
            };
            Ok(SubmissionRoute::Outbox {
                request: request_id,
                payload: OutboxQueuedWrite::Workspace(request),
            })
        }
        SubmissionPayload::Workspace(request) => Ok(SubmissionRoute::Immediate(
            RequestPayload::Workspace(request),
        )),
        SubmissionPayload::Mail(request)
            if matches!(request.operation, CentralMailOperation::SendMessage { .. }) =>
        {
            Err(SubmitRefusal::UnstampedMessageRequired)
        }
        SubmissionPayload::Mail(request) => {
            Ok(SubmissionRoute::Immediate(RequestPayload::Mail(request)))
        }
    }
}

pub(crate) async fn submit_to_outbox(
    work: &mut OutboxWork,
    caller: Caller,
    request: RequestId,
    payload: OutboxQueuedWrite,
    flush_at: SyncTime,
) -> Submission {
    let scope = wire_scope(payload.request_scope());
    let now_ms = work.local_now_ms();
    let Some(sent_at) = Timestamp::from_timestamp_millis(now_ms) else {
        return Submission::Refused(SubmitRefusal::InvalidSendTime);
    };
    let wire = match request_for_write(&caller, &payload, sent_at, now_ms) {
        Ok(wire) => wire,
        Err(error) => return Submission::Refused(SubmitRefusal::InvalidRequest(error)),
    };
    match fits_one_member(&wire) {
        Ok(true) => {}
        Ok(false) => return Submission::Refused(SubmitRefusal::TooLargeToSend),
        Err(error) => return Submission::Refused(SubmitRefusal::UnmeasurableWrite(error)),
    }
    let item = NewOutboxItem {
        request: request.clone(),
        caller,
        payload,
        flush_at_ms: flush_at.unix_millis(),
    };
    match work.outbox_storage().insert(&item, now_ms).await {
        Ok(OutboxInsert::Stored) => Submission::InOutbox(OutboxReceipt {
            id: request,
            flush_at,
        }),
        Ok(OutboxInsert::AlreadyStored(OutboxRequestStatus::InOutbox { flush_at_ms })) => {
            Submission::InOutbox(OutboxReceipt {
                id: request,
                flush_at: SyncTime::from_unix_millis(flush_at_ms),
            })
        }
        Ok(OutboxInsert::AlreadyStored(OutboxRequestStatus::Answered(accepted))) => {
            Submission::Answered(accepted_reply(accepted))
        }
        Ok(OutboxInsert::Refused(_))
        | Ok(OutboxInsert::AlreadyStored(OutboxRequestStatus::Refused(OutboxRefusal::Local(_)))) => {
            Submission::Refused(SubmitRefusal::RequestIdConflict)
        }
        Err(OutboxStoreError::InsertNotStarted(_)) => {
            Submission::Refused(SubmitRefusal::OutboxUnavailable)
        }
        Ok(OutboxInsert::AlreadyStored(
            OutboxRequestStatus::NotFound
            | OutboxRequestStatus::Sent
            | OutboxRequestStatus::Refused(_)
            | OutboxRequestStatus::Ended { .. },
        ))
        | Err(_) => Submission::Uncertain { request, scope },
    }
}

pub(crate) fn accepted_reply(accepted: OutboxAccepted) -> ReplyPayload {
    match accepted {
        OutboxAccepted::ThreadPost {
            post_id,
            position,
            actors,
        } => ReplyPayload::Workspace(WorkspaceReply {
            result: WorkspaceResult::Done(WorkspaceAnswer::Created {
                item: ItemRef::Post(post_id),
                position,
            }),
            actors,
        }),
        OutboxAccepted::SendMessage { message_id } => {
            ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued {
                message_id,
            }))
        }
    }
}

pub(crate) fn wire_scope(scope: OutboxScope) -> RequestScope {
    match scope {
        OutboxScope::Organization => RequestScope::Organization,
        OutboxScope::Workspace(workspace) => RequestScope::Workspace(workspace),
    }
}

pub(crate) fn prepare_immediate_request(
    caller: Caller,
    payload: RequestPayload,
    now_ms: i64,
) -> Result<LinkRequest, SubmitRefusal> {
    let correlation = FreshCorrelationId::mint_at(now_ms).wire_id().clone();
    LinkRequest::new(correlation, caller, payload).map_err(SubmitRefusal::InvalidRequest)
}

pub(crate) fn settle_immediate_request(
    request: &LinkRequest,
    disposition: LinkLaneDisposition<CorrelatedRequestReply>,
) -> Submission {
    match disposition {
        LinkLaneDisposition::NotReached { .. } => {
            Submission::Refused(SubmitRefusal::CentralUnavailable)
        }
        LinkLaneDisposition::Answered { reply }
            if reply.correlation_id == *request.correlation_id()
                && reply.result.matches_request(request.payload()) =>
        {
            Submission::Answered(*reply.result)
        }
        LinkLaneDisposition::Answered { .. } | LinkLaneDisposition::AnswerLost => {
            match request.payload().write_request_id() {
                Some(request_id) => Submission::Uncertain {
                    request: request_id.clone(),
                    scope: request.payload().request_scope(),
                },
                None => Submission::ReadAnswerLost,
            }
        }
    }
}
