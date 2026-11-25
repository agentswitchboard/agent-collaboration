//! Read organization data over an admitted person link.

mod change_records;
pub use change_records::{
    AuditChangeKind, AuditChangeOrigin, AuditRecord, ChangeRecordPageLimit, MachineAllowanceState,
    ReadOrganizationChangeRecords, ReadOrganizationChangeRecordsResult,
    ReadOrganizationChangeRecordsRpc, TargetedAuditRecord,
};

use approval_request_protocol::ApprovalRequestId;
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::{IndependentReplyRpc, Rpc};
use serde::{Deserialize, Serialize};
use workspace_ids::{RequestId, WorkspaceId};
use workspace_model::{Description, ResourceName};

pub const PERSON_WORKSPACE_PAGE_LIMIT: u16 = 100;

pub struct ListOrganizationWorkspacesRpc;

impl IndependentReplyRpc for ListOrganizationWorkspacesRpc {}

impl Rpc for ListOrganizationWorkspacesRpc {
    type Target = OrganizationId;
    type Request = ListOrganizationWorkspaces;
    type Reply = OrganizationWorkspacePage;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListOrganizationWorkspaces {
    pub operation_id: RequestId,
    pub after: Option<WorkspaceId>,
    pub limit: PersonWorkspacePageLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "u16", into = "u16")]
pub struct PersonWorkspacePageLimit(
    #[schemars(range(min = 1, max = PERSON_WORKSPACE_PAGE_LIMIT))] u16,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("workspace page limit is outside 1..=100")]
pub struct InvalidPersonWorkspacePageLimit;

impl TryFrom<u16> for PersonWorkspacePageLimit {
    type Error = InvalidPersonWorkspacePageLimit;

    fn try_from(limit: u16) -> Result<Self, Self::Error> {
        (1..=PERSON_WORKSPACE_PAGE_LIMIT)
            .contains(&limit)
            .then_some(Self(limit))
            .ok_or(InvalidPersonWorkspacePageLimit)
    }
}

impl From<PersonWorkspacePageLimit> for u16 {
    fn from(limit: PersonWorkspacePageLimit) -> Self {
        limit.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrganizationWorkspacePage {
    pub workspaces: Vec<OrganizationWorkspace>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub next: Option<WorkspaceId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrganizationWorkspace {
    pub workspace_id: WorkspaceId,
    pub name: ResourceName,
    pub description: Description,
    pub state: OrganizationWorkspaceState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum OrganizationWorkspaceState {
    Active,
    Archiving,
    Archived,
}

pub struct LookupOrganizationWorkspaceRpc;

impl IndependentReplyRpc for LookupOrganizationWorkspaceRpc {}

impl Rpc for LookupOrganizationWorkspaceRpc {
    type Target = OrganizationId;
    type Request = LookupOrganizationWorkspace;
    type Reply = LookupOrganizationWorkspaceResult;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LookupOrganizationWorkspace {
    pub workspace_id: WorkspaceId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "result", rename_all = "camelCase", deny_unknown_fields)]
pub enum LookupOrganizationWorkspaceResult {
    Found { workspace: OrganizationWorkspace },
    Absent {},
    Refused { reason: OrganizationReadRefusal },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum OrganizationReadRefusal {
    NotPermitted {},
}

pub struct ListPersonApprovalsRpc;

impl IndependentReplyRpc for ListPersonApprovalsRpc {}

impl Rpc for ListPersonApprovalsRpc {
    type Target = OrganizationId;
    type Request = ListPersonApprovals;
    type Reply = approval_request_protocol::ApprovalListReply;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListPersonApprovals {
    pub filter: approval_request_protocol::ApprovalListFilter,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ApprovalRequestId>,
}

pub struct ReadPersonApprovalRpc;

impl IndependentReplyRpc for ReadPersonApprovalRpc {}

impl Rpc for ReadPersonApprovalRpc {
    type Target = OrganizationId;
    type Request = ReadPersonApproval;
    type Reply = Box<approval_request_protocol::ApprovalReadReply>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadPersonApproval {
    pub request_id: ApprovalRequestId,
}

pub struct DecidePersonApprovalRpc;

impl IndependentReplyRpc for DecidePersonApprovalRpc {}

impl Rpc for DecidePersonApprovalRpc {
    type Target = OrganizationId;
    type Request = DecidePersonApproval;
    type Reply = approval_request_protocol::ApprovalDecideReply;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DecidePersonApproval {
    pub request_id: ApprovalRequestId,
    pub operation_id: RequestId,
    pub answer: approval_request_protocol::Answer,
}

// The bodies use the same strict named-map decoding as the approval records.
impl<'de> Deserialize<'de> for ListPersonApprovals {
    fn deserialize<D: serde::Deserializer<'de>>(input: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Fields {
            filter: approval_request_protocol::ApprovalListFilter,
            cursor: Option<ApprovalRequestId>,
        }
        let Fields { filter, cursor } = approval_request_protocol::decode_named_map(input)?;
        Ok(Self { filter, cursor })
    }
}

impl<'de> Deserialize<'de> for ReadPersonApproval {
    fn deserialize<D: serde::Deserializer<'de>>(input: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Fields {
            request_id: ApprovalRequestId,
        }
        let Fields { request_id } = approval_request_protocol::decode_named_map(input)?;
        Ok(Self { request_id })
    }
}

impl<'de> Deserialize<'de> for DecidePersonApproval {
    fn deserialize<D: serde::Deserializer<'de>>(input: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Fields {
            request_id: ApprovalRequestId,
            operation_id: RequestId,
            answer: approval_request_protocol::Answer,
        }
        let Fields {
            request_id,
            operation_id,
            answer,
        } = approval_request_protocol::decode_named_map(input)?;
        Ok(Self {
            request_id,
            operation_id,
            answer,
        })
    }
}
