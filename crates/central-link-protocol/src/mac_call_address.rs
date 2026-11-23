//! Expected receiver and reply for an outbound machine-link call.

use crate::{
    MacCall, MacCallReply, TypedCallResult,
    approval_rpc::{ApprovalHolderTarget, ApprovalNoticeTarget},
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use workspace_ids::{MachineId, RouterRunId};

/// The destination comes from the call envelope and stays attached to its waiter.
#[derive(Clone)]
pub enum MacCallAddress {
    DeliverEnding(ApprovalHolderTarget),
    ApprovalNotice(ApprovalNoticeTarget),
    #[cfg(any(test, feature = "test-support"))]
    MacProbe(MachineId),
}

impl From<&MacCall> for MacCallAddress {
    fn from(call: &MacCall) -> Self {
        match call {
            MacCall::DeliverEnding(call) => Self::DeliverEnding(call.target.clone()),
            MacCall::ApprovalNotice(call) => Self::ApprovalNotice(call.target.clone()),
            #[cfg(any(test, feature = "test-support"))]
            MacCall::MacProbe(call) => Self::MacProbe(call.target.clone()),
        }
    }
}

impl MacCallAddress {
    pub fn organization_id(&self) -> Option<&OrganizationId> {
        match self {
            Self::DeliverEnding(target) => Some(&target.organization_id),
            Self::ApprovalNotice(target) => Some(&target.organization_id),
            #[cfg(any(test, feature = "test-support"))]
            Self::MacProbe(_) => None,
        }
    }

    pub fn machine_id(&self) -> &MachineId {
        match self {
            Self::DeliverEnding(target) => &target.machine_id,
            Self::ApprovalNotice(target) => &target.machine_id,
            #[cfg(any(test, feature = "test-support"))]
            Self::MacProbe(machine_id) => machine_id,
        }
    }

    /// Revalidate this address against the current physical socket before writing.
    pub fn permits(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        enrollment: &EnrollmentId,
        run: Option<&RouterRunId>,
    ) -> bool {
        match self {
            Self::DeliverEnding(target) => {
                &target.organization_id == organization
                    && &target.machine_id == machine
                    && &target.enrollment_id == enrollment
                    && run == Some(&target.router_run)
            }
            #[cfg(any(test, feature = "test-support"))]
            Self::MacProbe(target) => target == machine,
            Self::ApprovalNotice(target) => {
                &target.organization_id == organization
                    && &target.machine_id == machine
                    && &target.enrollment_id == enrollment
            }
        }
    }

    /// Link refusals identify the attempt by correlation and the owning socket.
    /// Service answers must also match the call marker and exact address.
    pub fn matches_reply(&self, result: &TypedCallResult<MacCallReply>) -> bool {
        let TypedCallResult::Answered { reply } = result else {
            return true;
        };
        match (self, reply) {
            (Self::DeliverEnding(expected), MacCallReply::DeliverEnding(envelope)) => {
                &envelope.target == expected
            }
            (Self::ApprovalNotice(expected), MacCallReply::ApprovalNotice(envelope)) => {
                &envelope.target == expected
            }
            (Self::ApprovalNotice(_), MacCallReply::DeliverEnding(_))
            | (Self::DeliverEnding(_), MacCallReply::ApprovalNotice(_)) => false,
            #[cfg(any(test, feature = "test-support"))]
            (Self::MacProbe(expected), MacCallReply::MacProbe(envelope)) => {
                &envelope.target == expected
            }
            #[cfg(any(test, feature = "test-support"))]
            (Self::MacProbe(_), MacCallReply::DeliverEnding(_))
            | (Self::MacProbe(_), MacCallReply::ApprovalNotice(_))
            | (Self::DeliverEnding(_) | Self::ApprovalNotice(_), MacCallReply::MacProbe(_)) => {
                false
            }
        }
    }
}
