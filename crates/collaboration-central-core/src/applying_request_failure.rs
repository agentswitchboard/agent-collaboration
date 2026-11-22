use central_link_protocol::MachineLinkCloseCode;
use central_storage::StoreFailure;
use fleet_workspace_access::WorkspaceAdmissionRefusal;
use workspace_model::RefusalReason;

/// A request's later admission can lose authority after the link admitted its frame.
#[derive(Debug)]
pub enum ApplyingRequestFailure {
    Store(StoreFailure),
    Authority(MachineLinkCloseCode),
}

impl From<StoreFailure> for ApplyingRequestFailure {
    fn from(error: StoreFailure) -> Self {
        Self::Store(error)
    }
}

pub(crate) fn applying_refusal<Reply>(
    refusal: WorkspaceAdmissionRefusal,
    workspace_reply: impl FnOnce(RefusalReason) -> Reply,
) -> Result<Reply, ApplyingRequestFailure> {
    match refusal {
        WorkspaceAdmissionRefusal::Authority(cause) => {
            Err(ApplyingRequestFailure::Authority(cause))
        }
        WorkspaceAdmissionRefusal::Workspace(reason) => Ok(workspace_reply(reason)),
    }
}

#[cfg(feature = "test-support")]
pub(crate) fn probe_refusal(
    refusal: WorkspaceAdmissionRefusal,
) -> Result<RefusalReason, StoreFailure> {
    use workspace_model::WorkspaceResult;
    match refusal {
        WorkspaceAdmissionRefusal::Workspace(reason) => Ok(reason),
        WorkspaceAdmissionRefusal::Authority(cause) => {
            let reply = central_link_protocol::ReplyPayload::workspace_authority_refused(cause);
            if let WorkspaceResult::Refused(reason) = reply.result {
                Ok(reason)
            } else {
                Err(StoreFailure::Unavailable)
            }
        }
    }
}
