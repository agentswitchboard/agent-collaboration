//! The link family: each Central-backed tool becomes exactly one request on the shared port.
//!
//! The family is built from the caller admission and the port only, so it can reach Central
//! and nothing else. It maps a tool's inputs to its one request, submits it, and returns one
//! closed result with its wording.
use std::sync::Arc;

use collaboration_protocol::central_tool_port::{
    CentralSubmission, CentralToolPayload, CentralToolPort, ItemRef, WorkspaceOperation,
    WorkspaceRequest,
};
use collaboration_protocol::link_tool_result::{
    LinkRefusal, LinkToolOutput, LinkToolResult, MessageCallerRefusal,
};
use collaboration_protocol::message_tools::{MessageToolRefusal, SessionId};
use collaboration_protocol::workspace_tools::{ThreadId, ThreadShowArguments, WorkspaceId};

use super::link_answers::link_tool_output;
use super::message_operations::MessageCallerAdmission;

struct LinkToolServices {
    admission: Arc<dyn MessageCallerAdmission>,
    central: Arc<dyn CentralToolPort>,
}

/// The collaboration application's Central-backed tools, configured by the Host's composition.
#[derive(Clone, Default)]
pub struct LinkToolOperations(Option<Arc<LinkToolServices>>);

impl LinkToolOperations {
    pub fn new(
        admission: Arc<dyn MessageCallerAdmission>,
        central: Arc<dyn CentralToolPort>,
    ) -> Self {
        Self(Some(Arc::new(LinkToolServices { admission, central })))
    }

    /// `thread_show`: one immediate workspace Show of the thread itself (R1, R9).
    pub async fn show_thread(&self, input: ThreadShowArguments) -> LinkToolOutput {
        let request = thread_show_request(input.workspace_id, input.thread_id);
        link_tool_output(
            self.submit(
                input.caller_session_id,
                CentralToolPayload::Workspace(request),
            )
            .await,
        )
    }

    async fn submit(&self, caller: SessionId, payload: CentralToolPayload) -> LinkToolResult {
        let Some(services) = self.0.as_deref() else {
            return refused(LinkRefusal::CentralUnavailable);
        };
        // The Router's readiness comes first: a Router that is not enrolled or not ready cannot
        // judge the caller, so it must not answer "unknown caller".
        if let Err(code) = services.central.check_available() {
            return refused(code);
        }
        let caller = match services.admission.admit(Some(caller)).await {
            Ok(caller) => caller,
            Err(refusal) => return refused(admission_refusal(refusal)),
        };
        match services.central.submit(&caller, payload).await {
            Ok(submission) => answered(submission),
            Err(code) => refused(code),
        }
    }
}

/// A read carries the workspace it names and no write ID; the thread ID alone names the thread.
fn thread_show_request(workspace_id: WorkspaceId, thread_id: ThreadId) -> WorkspaceRequest {
    WorkspaceRequest {
        request_id: None,
        workspace_id: Some(workspace_id),
        operation: WorkspaceOperation::Show {
            item: ItemRef::Thread(thread_id),
        },
    }
}

fn answered(submission: CentralSubmission) -> LinkToolResult {
    match submission {
        CentralSubmission::Answered(reply) => LinkToolResult::Answered { reply },
        CentralSubmission::InOutbox {
            request_id,
            flush_at,
        } => LinkToolResult::InOutbox {
            request_id,
            flush_at,
        },
        CentralSubmission::Uncertain { request_id } => LinkToolResult::Uncertain { request_id },
        CentralSubmission::ReadAnswerLost => LinkToolResult::ReadAnswerLost,
    }
}

const fn refused(code: LinkRefusal) -> LinkToolResult {
    LinkToolResult::Refused { code }
}

/// Caller admission reports the caller's own refusal or the link's state. It shares the message
/// family's refusal type, so every other variant is listed: none is produced by an admission,
/// and each means the caller could not be admitted.
fn admission_refusal(refusal: MessageToolRefusal) -> LinkRefusal {
    match refusal {
        MessageToolRefusal::Caller(reason) => LinkRefusal::Caller { reason },
        MessageToolRefusal::Front(state) => LinkRefusal::RouterNotReady { state },
        MessageToolRefusal::NoMessageLink | MessageToolRefusal::TransportUnavailable => {
            LinkRefusal::CentralUnavailable
        }
        MessageToolRefusal::InvalidCaller
        | MessageToolRefusal::InvalidAddress
        | MessageToolRefusal::InvalidBody
        | MessageToolRefusal::InvalidRequestId
        | MessageToolRefusal::InvalidArguments
        | MessageToolRefusal::InvalidInboxLimit
        | MessageToolRefusal::InvalidInboxReadToken
        | MessageToolRefusal::InboxAcknowledgmentNotKept
        | MessageToolRefusal::InboxChanged
        | MessageToolRefusal::InboxUnavailable
        | MessageToolRefusal::Submission(_)
        | MessageToolRefusal::Recorded(_)
        | MessageToolRefusal::RequestIdUnavailable
        | MessageToolRefusal::UnexpectedReply
        | MessageToolRefusal::ReadAnswerLost => LinkRefusal::Caller {
            reason: MessageCallerRefusal::CouldNotTell,
        },
    }
}

#[cfg(test)]
#[path = "link_tool_operations_test.rs"]
mod tests;
