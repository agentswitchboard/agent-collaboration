//! Read the organization's workspace directory over an admitted person link.

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
