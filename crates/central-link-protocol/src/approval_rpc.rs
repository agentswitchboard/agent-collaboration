//! Approval RPC markers and addresses carried by the authenticated machine link.
pub use approval_request_protocol::RegisterRunReply;
use approval_request_protocol::{
    ApprovalNotice, ApprovalNoticeReply, ApprovalSessionCall, DecideApprovalAsSessionReply,
    DecideApprovalAsSessionRequest, DecideReply, DecideRequest, DeliverEndingReply,
    DeliverEndingRequest, ListApprovalsForSessionReply, ListApprovalsForSessionRequest,
    PassOnApprovalReply, PassOnApprovalRequest, PassOnReply, PassOnRequest, Raise, RaiseReply,
    ReadApprovalForSessionReply, ReadApprovalForSessionRequest, RegisterRunRequest, RouterRunId,
    StopReply, StopRequest, decode_named_map,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId, SessionSecret};
use fleet_rpc::{IndependentReplyRpc, Rpc};
use serde::{Deserialize, Serialize};
use workspace_ids::{CentralSessionId, MachineId};

/// Central's approvals service for one organization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalServiceTarget {
    pub organization_id: OrganizationId,
}

impl<'de> Deserialize<'de> for ApprovalServiceTarget {
    fn deserialize<Input: serde::Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct NamedTarget {
            organization_id: OrganizationId,
        }
        let target: NamedTarget = decode_named_map(input)?;
        Ok(Self {
            organization_id: target.organization_id,
        })
    }
}

/// Exact Mac authority allowed to receive a private ending.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalHolderTarget {
    pub organization_id: OrganizationId,
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
    pub router_run: RouterRunId,
}

impl<'de> Deserialize<'de> for ApprovalHolderTarget {
    fn deserialize<Input: serde::Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct NamedTarget {
            organization_id: OrganizationId,
            machine_id: MachineId,
            enrollment_id: EnrollmentId,
            router_run: RouterRunId,
        }
        let target: NamedTarget = decode_named_map(input)?;
        Ok(Self {
            organization_id: target.organization_id,
            machine_id: target.machine_id,
            enrollment_id: target.enrollment_id,
            router_run: target.router_run,
        })
    }
}

/// The exact physical enrollment and destination session for a pending request notice.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalNoticeTarget {
    pub organization_id: OrganizationId,
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
    pub session_id: CentralSessionId,
}

impl<'de> Deserialize<'de> for ApprovalNoticeTarget {
    fn deserialize<Input: serde::Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct NamedTarget {
            organization_id: OrganizationId,
            machine_id: MachineId,
            enrollment_id: EnrollmentId,
            session_id: CentralSessionId,
        }
        let target: NamedTarget = decode_named_map(input)?;
        Ok(Self {
            organization_id: target.organization_id,
            machine_id: target.machine_id,
            enrollment_id: target.enrollment_id,
            session_id: target.session_id,
        })
    }
}

macro_rules! bind_approval_rpc {
    ($marker:ident, $target:ty, $request:ty, $reply:ty) => {
        pub struct $marker;

        impl Rpc for $marker {
            type Target = $target;
            type Request = $request;
            type Reply = $reply;
        }

        impl IndependentReplyRpc for $marker {}
    };
}

bind_approval_rpc!(
    RegisterRunRpc,
    ApprovalServiceTarget,
    RegisterRunRequest,
    RegisterRunReply
);
bind_approval_rpc!(RaiseApprovalRpc, ApprovalServiceTarget, Raise, RaiseReply);
bind_approval_rpc!(
    StopApprovalRpc,
    ApprovalServiceTarget,
    StopRequest,
    StopReply
);
bind_approval_rpc!(
    DeliverEndingRpc,
    ApprovalHolderTarget,
    DeliverEndingRequest,
    DeliverEndingReply
);
bind_approval_rpc!(
    DecideApprovalRpc,
    ApprovalServiceTarget,
    DecideRequest,
    DecideReply
);
bind_approval_rpc!(
    PassOnApprovalRpc,
    ApprovalServiceTarget,
    PassOnRequest,
    PassOnReply
);

bind_approval_rpc!(
    ListApprovalsForSessionRpc,
    ApprovalServiceTarget,
    ApprovalSessionCall<SessionSecret, ListApprovalsForSessionRequest>,
    ListApprovalsForSessionReply
);
bind_approval_rpc!(
    ReadApprovalForSessionRpc,
    ApprovalServiceTarget,
    ApprovalSessionCall<SessionSecret, ReadApprovalForSessionRequest>,
    ReadApprovalForSessionReply
);
bind_approval_rpc!(
    DecideApprovalAsSessionRpc,
    ApprovalServiceTarget,
    ApprovalSessionCall<SessionSecret, DecideApprovalAsSessionRequest>,
    DecideApprovalAsSessionReply
);
bind_approval_rpc!(
    PassOnApprovalForSessionRpc,
    ApprovalServiceTarget,
    ApprovalSessionCall<SessionSecret, PassOnApprovalRequest>,
    PassOnApprovalReply
);
bind_approval_rpc!(
    ApprovalNoticeRpc,
    ApprovalNoticeTarget,
    ApprovalNotice,
    ApprovalNoticeReply
);

#[cfg(test)]
#[path = "approval_rpc_test.rs"]
mod tests;
