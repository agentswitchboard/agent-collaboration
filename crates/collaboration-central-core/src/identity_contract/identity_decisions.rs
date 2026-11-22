//! Sign-in service decisions and machine-token extension claims.
use super::{AllowedWorkspaces, DeviceLoginRequestId, EnrollPermission, PersonId};
use fleet_contracts::ClientId;
use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixSeconds};
use message_board::MachineLabel;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::num::NonZeroU32;
use workspace_ids::MachineId;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct DecideDeviceLogin {
    pub organization_id: OrganizationId,
    pub approver: PersonId,
    pub permission_checked: EnrollPermission,
    pub request_id: DeviceLoginRequestId,
    pub machine_id: MachineId,
    pub label: MachineLabel,
    pub allowed_workspaces: AllowedWorkspaces,
    #[serde(deserialize_with = "required_nullable_approval_time")]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = NullableApprovalTime))]
    #[schemars(required, schema_with = "nullable_approval_time_schema")]
    pub approver_second_factor_at: NullableApprovalTime,
    pub code_expires_at: UnixSeconds,
}
type NullableApprovalTime = Option<UnixSeconds>;

#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
const _: fn(&DecideDeviceLogin) -> &NullableApprovalTime =
    |request| &request.approver_second_factor_at;

fn nullable_approval_time_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let seconds = UnixSeconds::json_schema(generator);
    schemars::json_schema!({"anyOf": [seconds, {"type": "null"}]})
}

fn required_nullable_approval_time<'de, Decoder: Deserializer<'de>>(
    decoder: Decoder,
) -> Result<NullableApprovalTime, Decoder::Error> {
    NullableApprovalTime::deserialize(decoder)
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "result",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DeviceLoginDecision {
    Approved {
        enrollment_id: EnrollmentId,
        label: MachineLabel,
        allowed_workspaces: AllowedWorkspaces,
    },
    Refused {
        reason: DeviceLoginRefusal,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DeviceLoginRefusal {
    NotPermitted {},
    MachineInAnotherOrganization {},
    UnknownWorkspace {},
    SecondFactorRequired { within_seconds: NonZeroU32 },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct DecideMachineToken {
    pub organization_id: OrganizationId,
    pub enrollment_id: EnrollmentId,
    pub client_id: ClientId,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct EndedMachineClient {
    pub enrollment_id: EnrollmentId,
    pub client_id: ClientId,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "result",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum MachineTokenDecision {
    Grant {
        machine_id: MachineId,
        enrollment_id: EnrollmentId,
        decided_at: UnixSeconds,
        grant_expires_at: UnixSeconds,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        superseded: Option<EndedMachineClient>,
    },
    Refuse {
        reason: MachineTokenRefusal,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum MachineTokenRefusal {
    EnrollmentNotActive {},
    Superseded {},
    WrongClient {},
    IdleExpired {},
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct PresentedMachine {
    pub machine_id: MachineId,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DisableMachineClient {
    pub organization_id: OrganizationId,
    pub enrollment_id: EnrollmentId,
    pub client_id: ClientId,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "result",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DisabledMachineClient {
    Disabled {},
    AlreadyDisabled {},
    NotThisEnrollment {},
}
#[cfg(test)]
#[path = "identity_decisions_test.rs"]
mod tests;
