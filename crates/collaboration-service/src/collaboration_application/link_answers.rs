//! All agent-facing wording for the Central-backed tools.
//!
//! Every non-success answer names the state and a valid next step (R10). Each match is
//! exhaustive, so a new Central or Router variant fails to compile until it is worded here.
use collaboration_protocol::central_tool_port::{
    CentralMailResult, ReplyPayload, WorkspaceReply, WorkspaceResult,
};
use collaboration_protocol::link_tool_result::{
    InvalidRequestReason, LinkRefusal, LinkToolOutput, LinkToolResult, MessageCallerRefusal,
    MessageFrontRefusal, RemovedField,
};
use workspace_refusal_wording::{mail_refusal, workspace_refusal};

#[path = "link_answers_refusals.rs"]
mod workspace_refusal_wording;

/// One answer's state and the next step an agent can take from it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Wording {
    pub state: String,
    pub next_step: &'static str,
}

impl Wording {
    fn new(state: impl Into<String>, next_step: &'static str) -> Self {
        Self {
            state: state.into(),
            next_step,
        }
    }

    fn sentence(&self) -> String {
        format!("{}. {}", self.state, self.next_step)
    }
}

/// Pairs a tool's result with its wording.
#[must_use]
pub fn link_tool_output(result: LinkToolResult) -> LinkToolOutput {
    let text = link_tool_text(&result);
    LinkToolOutput { result, text }
}

fn link_tool_text(result: &LinkToolResult) -> String {
    match result {
        LinkToolResult::Answered { reply } => match reply {
            ReplyPayload::Workspace(WorkspaceReply { result, .. }) => {
                workspace_result_wording(result).map_or_else(done, |wording| wording.sentence())
            }
            ReplyPayload::Mail(result) => {
                mail_result_wording(result).map_or_else(done, |wording| wording.sentence())
            }
        },
        LinkToolResult::InOutbox {
            request_id,
            flush_at,
        } => format!(
            "In outbox as request {request_id}; it leaves this Mac at {}.",
            flush_at.to_rfc3339()
        ),
        LinkToolResult::Uncertain { request_id } => Wording::new(
            format!("Uncertain: request {request_id} left this Mac and its answer was lost, so it may have applied"),
            "Check its status, or resend with the same request ID within 24 hours.",
        )
        .sentence(),
        LinkToolResult::ReadAnswerLost => Wording::new(
            "Read failed: the request left this Mac and its answer was lost",
            "Reading again is safe.",
        )
        .sentence(),
        LinkToolResult::Refused { code } => refusal_wording(code).sentence(),
    }
}

fn done() -> String {
    "Central answered.".into()
}

/// The wording of a Central workspace answer; a done answer needs none.
#[must_use]
pub fn workspace_result_wording(result: &WorkspaceResult) -> Option<Wording> {
    Some(match result {
        WorkspaceResult::Done(_) => return None,
        WorkspaceResult::Refused(reason) => workspace_refusal(reason),
        WorkspaceResult::NotFound { workspace_id, .. } => Wording::new(
            format!("Not found: Central has no such item in workspace {workspace_id}"),
            "Check the workspaceId and the item's ID, then try again.",
        ),
        WorkspaceResult::Conflict => Wording::new(
            "Conflict: Central found the request at odds with the item's current state",
            "Read the item again, then decide whether to retry.",
        ),
        WorkspaceResult::OutcomeUnknown => Wording::new(
            "Outcome unknown: Central could not say whether the request applied",
            "Read the item again before retrying.",
        ),
        WorkspaceResult::CentralUnavailable => Wording::new(
            "Central unavailable: it answered without deciding",
            "Try again shortly, or check the link.",
        ),
    })
}

/// The wording of a Central mail answer; a done answer needs none.
#[must_use]
pub fn mail_result_wording(result: &CentralMailResult) -> Option<Wording> {
    Some(match result {
        CentralMailResult::Done(_) => return None,
        CentralMailResult::Refused(reason) => mail_refusal(reason),
        CentralMailResult::Conflict => Wording::new(
            "Conflict: Central found the request at odds with what it holds",
            "Check the request's status before retrying.",
        ),
        CentralMailResult::OutcomeUnknown => Wording::new(
            "Outcome unknown: Central could not say whether the request applied",
            "Check its status before retrying.",
        ),
        CentralMailResult::CentralUnavailable => Wording::new(
            "Central unavailable: it answered without deciding",
            "Try again shortly, or check the link.",
        ),
    })
}

/// The wording of a refusal made before Central decided anything.
#[must_use]
pub fn refusal_wording(code: &LinkRefusal) -> Wording {
    match code {
        LinkRefusal::CentralUnavailable => Wording::new(
            "Central unavailable: nothing was sent",
            "Try again, or check the link.",
        ),
        LinkRefusal::OutboxUnavailable => Wording::new(
            "Outbox unavailable: nothing was kept or sent",
            "Try again shortly; if it persists, check the Router.",
        ),
        LinkRefusal::RequestIdConflict => Wording::new(
            "Request ID conflict: this ID already names a different write",
            "Use a new request ID for a new write, or resend the original content with this ID.",
        ),
        LinkRefusal::InvalidRequest { reason } => invalid_request(reason),
        LinkRefusal::RemovedInput { field } => removed_input(*field),
        LinkRefusal::InvalidArguments => Wording::new(
            "Invalid arguments: the input does not match the tool's schema",
            "Correct the input to the listed fields, then call again.",
        ),
        LinkRefusal::Caller { reason } => caller(*reason),
        LinkRefusal::RouterNotReady { state } => router_not_ready(*state),
    }
}

fn invalid_request(reason: &InvalidRequestReason) -> Wording {
    let next = "Correct the request; resending it unchanged will be refused again.";
    let state = match reason {
        InvalidRequestReason::MalformedFrame => "the link could not encode the request".into(),
        InvalidRequestReason::RequestIdPolicy { field } => {
            format!("{field} does not match the operation's request-ID rule")
        }
        InvalidRequestReason::InvalidBatch => "the write batch is malformed".into(),
        InvalidRequestReason::InvalidReceiptBatch => "the receipt batch is malformed".into(),
        InvalidRequestReason::InvalidOffer => "the offer lacks the time it requires".into(),
        InvalidRequestReason::WrongDirection => {
            "the request is not allowed from a Mac to Central".into()
        }
        InvalidRequestReason::TooLargeToSend => "the request is too large to send".into(),
        InvalidRequestReason::UnknownTypedCall => "the link does not know this call".into(),
        InvalidRequestReason::UnmeasurableWrite => "the link could not measure the write".into(),
        InvalidRequestReason::UnstampedMessageRequired => {
            "a message must reach the outbox unstamped".into()
        }
        InvalidRequestReason::InvalidSendTime => "the write's send time is invalid".into(),
    };
    Wording::new(format!("Invalid request: {state}; nothing was sent"), next)
}

fn removed_input(field: RemovedField) -> Wording {
    match field {
        RemovedField::IncludeRedacted => Wording::new(
            "includeRedacted is no longer accepted: redaction deletes the text, so there is nothing to include",
            "Omit includeRedacted and call again.",
        ),
        RemovedField::ProjectQualifier => Wording::new(
            "The project qualifier (projectId or project) is no longer accepted: a thread ID alone names the thread",
            "Omit it and identify the thread with workspaceId and threadId.",
        ),
        RemovedField::ProtocolVersion => Wording::new(
            "protocolVersion is no longer accepted: the link versions itself",
            "Omit protocolVersion and call again.",
        ),
    }
}

fn caller(reason: MessageCallerRefusal) -> Wording {
    match reason {
        MessageCallerRefusal::CallerRequired => Wording::new(
            "Caller required: no calling session was reported",
            "Report your harness session ID as callerSessionId.",
        ),
        MessageCallerRefusal::UnknownSession => Wording::new(
            "Unknown caller: this Mac has no session with that ID",
            "Report your current harness session ID as callerSessionId.",
        ),
        MessageCallerRefusal::AmbiguousSession => Wording::new(
            "Ambiguous caller: more than one session on this Mac has that ID",
            "Call from a session whose ID is unique on this Mac.",
        ),
        MessageCallerRefusal::CouldNotTell => Wording::new(
            "Caller not admitted: the Router could not tell which session is calling",
            "Try again shortly.",
        ),
    }
}

fn router_not_ready(state: MessageFrontRefusal) -> Wording {
    let (state, next) = match state {
        MessageFrontRefusal::AwaitingFirstRead => (
            "the Router has not yet read this Mac's enrollment",
            "Try again shortly.",
        ),
        MessageFrontRefusal::NotEnrolled => (
            "this Mac is not enrolled",
            "Enroll this Mac, then try again.",
        ),
        MessageFrontRefusal::EnrollmentEnded => (
            "this Mac's enrollment has ended",
            "Enroll this Mac again, then try again.",
        ),
        MessageFrontRefusal::MetadataUnreadable => (
            "the Router cannot read this Mac's enrollment",
            "Try again shortly; if it persists, check the Router.",
        ),
        MessageFrontRefusal::Starting => ("the link is starting", "Try again shortly."),
        MessageFrontRefusal::Restarting => ("the link is restarting", "Try again shortly."),
        MessageFrontRefusal::EnrollmentMismatch => (
            "this Mac's enrollment does not match the running link",
            "Enroll this Mac again, then try again.",
        ),
        MessageFrontRefusal::ShuttingDown => (
            "the Router is shutting down",
            "Try again after the Router restarts.",
        ),
        MessageFrontRefusal::CouldNotStart => (
            "the link could not start",
            "Check the Router, then try again.",
        ),
    };
    Wording::new(format!("Router not ready: {state}; nothing was sent"), next)
}

#[cfg(test)]
#[path = "link_answers_test.rs"]
mod tests;
