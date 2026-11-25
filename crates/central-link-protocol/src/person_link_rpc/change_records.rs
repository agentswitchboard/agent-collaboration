//! Change-record paging on the person link.

use super::OrganizationReadRefusal;
use fleet_identity_protocol::{OrganizationId, UnixMillis};
use fleet_rpc::{IndependentReplyRpc, Rpc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use workspace_ids::{MachineId, WorkspaceId};

const MAXIMUM_CHANGE_RECORD_PAGE: u16 = 100;

pub struct ReadOrganizationChangeRecordsRpc;

impl IndependentReplyRpc for ReadOrganizationChangeRecordsRpc {}

impl Rpc for ReadOrganizationChangeRecordsRpc {
    type Target = OrganizationId;
    type Request = ReadOrganizationChangeRecords;
    type Reply = ReadOrganizationChangeRecordsResult;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadOrganizationChangeRecords {
    #[schemars(with = "Option<String>", extend("format" = "uuid"))]
    pub after: Option<Uuid>,
    pub limit: ChangeRecordPageLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "u16", into = "u16")]
pub struct ChangeRecordPageLimit(#[schemars(range(min = 1, max = MAXIMUM_CHANGE_RECORD_PAGE))] u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("change record page limit is outside 1..=100")]
pub struct InvalidChangeRecordPageLimit;

impl TryFrom<u16> for ChangeRecordPageLimit {
    type Error = InvalidChangeRecordPageLimit;

    fn try_from(limit: u16) -> Result<Self, Self::Error> {
        (1..=MAXIMUM_CHANGE_RECORD_PAGE)
            .contains(&limit)
            .then_some(Self(limit))
            .ok_or(InvalidChangeRecordPageLimit)
    }
}

impl From<ChangeRecordPageLimit> for u16 {
    fn from(limit: ChangeRecordPageLimit) -> Self {
        limit.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "camelCase", deny_unknown_fields)]
pub enum ReadOrganizationChangeRecordsResult {
    Records {
        records: Vec<AuditRecord>,
        #[serde(deserialize_with = "Option::deserialize")]
        #[schemars(with = "Option<String>", extend("format" = "uuid"))]
        next: Option<Uuid>,
    },
    Refused {
        reason: OrganizationReadRefusal,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum AuditRecord {
    MachineAllowanceChanged {
        #[schemars(with = "String", extend("format" = "uuid"))]
        id: Uuid,
        request_id: String,
        origin: AuditChangeOrigin,
        machine_id: MachineId,
        workspace_id: WorkspaceId,
        state: MachineAllowanceState,
        created_at_millis: UnixMillis,
    },
    #[serde(untagged)]
    TargetedChange(TargetedAuditRecord),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TargetedAuditRecord {
    #[schemars(with = "String", extend("format" = "uuid"))]
    pub id: Uuid,
    pub request_id: String,
    pub kind: AuditChangeKind,
    pub origin: AuditChangeOrigin,
    pub target_id: String,
    pub created_at_millis: UnixMillis,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AuditChangeKind {
    OrganizationProvisioned,
    StampRenewed,
    WorkspaceCreated,
    MachineEnrolled,
    MachineRevoked,
    MachineExpired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum AuditChangeOrigin {
    SignInProvisioning {},
    Automatic {},
    Person {
        person_id: String,
        permission_checked: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MachineAllowanceState {
    Active,
    Withdrawn,
}
