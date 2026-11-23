//! Approval RPC markers and addresses carried by the authenticated machine link.
pub use approval_request_protocol::RegisterRunReply;
use approval_request_protocol::{
    DecideReply, DecideRequest, DeliverEndingReply, DeliverEndingRequest, PassOnReply,
    PassOnRequest, Raise, RaiseReply, RegisterRunRequest, RouterRunId, StopReply, StopRequest,
    decode_named_map,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_rpc::{IndependentReplyRpc, Rpc};
use serde::{Deserialize, Serialize};
use workspace_ids::MachineId;

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

#[cfg(test)]
#[path = "approval_rpc_test.rs"]
mod tests;
