//! Adapts the link's tool submission seam to the collaboration application's one Central port.
//!
//! The adapter builds no tool-specific request: it forwards the application's payload to the
//! current generation's custody and projects custody's answer into the port's public types.
use central_link_protocol::ProtocolError;
use collaboration_protocol::central_tool_port::{
    AdmittedLocalSession, CentralSubmission, CentralToolFuture, CentralToolPayload,
    CentralToolPort, Timestamp,
};
use collaboration_protocol::link_tool_result::{InvalidRequestReason, LinkRefusal};
use switchboard_message_custody::{
    custody_submission::{Submission, SubmissionPayload, SubmitRefusal},
    custody_task::CustodyTaskHandle,
};
use workspace_model::Caller;

use crate::{FrontRefusal, MessageFront, message_tool_port::front_refusal};

impl CentralToolPort for MessageFront {
    fn check_available(&self) -> Result<(), LinkRefusal> {
        self.ready_custody().map(drop)
    }

    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        payload: CentralToolPayload,
    ) -> CentralToolFuture<'call> {
        Box::pin(async move {
            let custody = self.ready_custody()?;
            let admission = custody.reserve().await.map_err(|_| self.link_stopped())?;
            let submission = admission
                .submit(
                    Caller::Session(caller.address().clone()),
                    submission_payload(payload),
                )
                .resolve()
                .await;
            match central_submission(submission) {
                Err(LinkRefusal::OutboxUnavailable) => Err(self
                    .ready_custody()
                    .err()
                    .unwrap_or(LinkRefusal::OutboxUnavailable)),
                answer => answer,
            }
        })
    }
}

impl MessageFront {
    /// The current generation's custody, or the Router's readiness refusal: the one check
    /// behind both `check_available` and every submission.
    fn ready_custody(&self) -> Result<CustodyTaskHandle, LinkRefusal> {
        self.custody().map_err(router_not_ready)
    }

    /// The generation's refusal when custody stopped between reservation attempts.
    fn link_stopped(&self) -> LinkRefusal {
        self.ready_custody()
            .err()
            .unwrap_or_else(|| router_not_ready(FrontRefusal::ShuttingDown))
    }
}

fn router_not_ready(reason: FrontRefusal) -> LinkRefusal {
    LinkRefusal::RouterNotReady {
        state: front_refusal(reason),
    }
}

fn submission_payload(payload: CentralToolPayload) -> SubmissionPayload {
    match payload {
        CentralToolPayload::SendMessage { request, message } => {
            SubmissionPayload::SendMessage { request, message }
        }
        CentralToolPayload::Workspace(request) => SubmissionPayload::Workspace(request),
        CentralToolPayload::Mail(request) => SubmissionPayload::Mail(request),
    }
}

fn central_submission(submission: Submission) -> Result<CentralSubmission, LinkRefusal> {
    match submission {
        Submission::Answered(reply) => Ok(CentralSubmission::Answered(reply)),
        Submission::InOutbox(receipt) => Ok(
            match Timestamp::from_timestamp_millis(receipt.flush_at.unix_millis()) {
                Some(flush_at) => CentralSubmission::InOutbox {
                    request_id: receipt.id,
                    flush_at,
                },
                // The write is stored, but its mark cannot be shown; its status can.
                None => CentralSubmission::Uncertain {
                    request_id: receipt.id,
                },
            },
        ),
        Submission::Uncertain { request, .. } => Ok(CentralSubmission::Uncertain {
            request_id: request,
        }),
        Submission::ReadAnswerLost => Ok(CentralSubmission::ReadAnswerLost),
        Submission::Refused(reason) => Err(link_refusal(reason)),
    }
}

fn link_refusal(reason: SubmitRefusal) -> LinkRefusal {
    let invalid = |reason| LinkRefusal::InvalidRequest { reason };
    match reason {
        SubmitRefusal::CentralUnavailable => LinkRefusal::CentralUnavailable,
        SubmitRefusal::OutboxUnavailable => LinkRefusal::OutboxUnavailable,
        SubmitRefusal::RequestIdConflict => LinkRefusal::RequestIdConflict,
        SubmitRefusal::UnstampedMessageRequired => {
            invalid(InvalidRequestReason::UnstampedMessageRequired)
        }
        SubmitRefusal::TooLargeToSend => invalid(InvalidRequestReason::TooLargeToSend),
        SubmitRefusal::InvalidSendTime => invalid(InvalidRequestReason::InvalidSendTime),
        SubmitRefusal::UnmeasurableWrite(_) => invalid(InvalidRequestReason::UnmeasurableWrite),
        SubmitRefusal::InvalidRequest(error) => invalid(invalid_request_reason(&error)),
    }
}

/// `ProtocolError` holds a JSON error whose text is not public-safe, so only its code crosses.
fn invalid_request_reason(error: &ProtocolError) -> InvalidRequestReason {
    match error {
        ProtocolError::Json(_) => InvalidRequestReason::MalformedFrame,
        ProtocolError::RequestIdPolicy { field } => InvalidRequestReason::RequestIdPolicy {
            field: (*field).to_owned(),
        },
        ProtocolError::InvalidBatch => InvalidRequestReason::InvalidBatch,
        ProtocolError::InvalidReceiptBatch => InvalidRequestReason::InvalidReceiptBatch,
        ProtocolError::InvalidOffer => InvalidRequestReason::InvalidOffer,
        ProtocolError::WrongDirection => InvalidRequestReason::WrongDirection,
        ProtocolError::EgressTooBig { .. } => InvalidRequestReason::TooLargeToSend,
        ProtocolError::UnknownTypedCall { .. } => InvalidRequestReason::UnknownTypedCall,
    }
}

#[cfg(test)]
#[path = "central_tool_port_test.rs"]
mod tests;
