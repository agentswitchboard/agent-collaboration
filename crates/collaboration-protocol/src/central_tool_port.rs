//! The one port every Central-backed tool submits through.
//!
//! The collaboration application maps a tool's inputs to one Central request and hands it to
//! this port; the Mac's message unit implements it over its link. Local mailbox reads stay on
//! the message unit's own port, so a local tool never gains a path to Central.
use std::{future::Future, pin::Pin};

pub use central_link_protocol::ReplyPayload;
pub use workspace_ids::{AdmittedLocalSession, RequestId};
pub use workspace_model::{
    ItemRef, ItemView, MemberRole, NotificationSetting, RefusalReason, Thread, ThreadReaderView,
    ThreadState, Timestamp, WorkspaceAnswer, WorkspaceOperation, WorkspaceReply, WorkspaceRequest,
    WorkspaceResult,
    central_mail::{
        CentralMailRefusal, CentralMailRequest, CentralMailResult, SendMessageRequest,
        TimingRequirement,
    },
};

use crate::link_tool_result::LinkRefusal;

/// The request a tool hands to Central, one variant per path the link carries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CentralToolPayload {
    /// An unstamped direct message; the link stamps it from its outbox row.
    SendMessage {
        request: RequestId,
        message: SendMessageRequest,
    },
    Workspace(WorkspaceRequest),
    Mail(CentralMailRequest),
}

/// What the link reports for one submitted request when it did not refuse it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CentralSubmission {
    /// Central's own answer, unchanged, including its domain refusals.
    Answered(ReplyPayload),
    /// The write is stored in this Mac's outbox and leaves at `flush_at`.
    InOutbox {
        request_id: RequestId,
        flush_at: Timestamp,
    },
    /// An immediate write left this Mac and its answer was lost.
    Uncertain { request_id: RequestId },
    /// A read left this Mac and its answer was lost.
    ReadAnswerLost,
}

/// The port resolves one submission without exposing the link's queue or frame types.
pub type CentralToolFuture<'call> =
    Pin<Box<dyn Future<Output = Result<CentralSubmission, LinkRefusal>> + Send + 'call>>;

/// Submits one request for an admitted local session; the only way a tool reaches Central.
pub trait CentralToolPort: Send + Sync {
    /// Whether the link can take a request now; checked before the caller is admitted, so a
    /// Router that is not enrolled or not ready says so instead of judging the caller.
    fn check_available(&self) -> Result<(), LinkRefusal>;

    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        payload: CentralToolPayload,
    ) -> CentralToolFuture<'call>;
}
