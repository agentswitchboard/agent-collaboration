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
    #[serde(deserialize_with = "nullable_nonnegative_unix_seconds")]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = Option<UnixSeconds>))]
    #[schemars(required, schema_with = "nullable_nonnegative_unix_seconds_schema")]
    pub approver_second_factor_at: Option<UnixSeconds>,
    #[serde(deserialize_with = "nonnegative_unix_seconds")]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = UnixSeconds))]
    #[schemars(with = "i64", range(min = 0))]
    pub code_expires_at: UnixSeconds,
}
fn nullable_nonnegative_unix_seconds_schema(
    generator: &mut schemars::SchemaGenerator,
) -> schemars::Schema {
    let mut seconds = i64::json_schema(generator);
    seconds.insert("minimum".into(), 0.into());
    schemars::json_schema!({"anyOf": [seconds, {"type": "null"}]})
}

fn nonnegative_unix_seconds<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<UnixSeconds, D::Error> {
    let seconds = UnixSeconds::deserialize(deserializer)?;
    if seconds.0 < 0 {
        return Err(serde::de::Error::custom("Unix seconds must be nonnegative"));
    }
    Ok(seconds)
}
fn nullable_nonnegative_unix_seconds<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<UnixSeconds>, D::Error> {
    let seconds = Option::<UnixSeconds>::deserialize(deserializer)?;
    if seconds.is_some_and(|seconds| seconds.0 < 0) {
        return Err(serde::de::Error::custom("Unix seconds must be nonnegative"));
    }
    Ok(seconds)
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
        #[serde(deserialize_with = "nonnegative_unix_seconds")]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = UnixSeconds))]
        #[schemars(with = "i64", range(min = 0))]
        decided_at: UnixSeconds,
        #[serde(deserialize_with = "nonnegative_unix_seconds")]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = UnixSeconds))]
        #[schemars(with = "i64", range(min = 0))]
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
