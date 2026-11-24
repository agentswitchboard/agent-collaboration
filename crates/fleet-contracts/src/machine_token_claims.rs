use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixSeconds};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use workspace_ids::MachineId;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "snake_case",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum MachinePrincipal {
    Machine {
        machine_id: MachineId,
        enrollment_id: EnrollmentId,
    },
}
/// Only extension claims live here; the token checker validates the registered claims separately.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct MachineTokenClaims {
    pub principal: MachinePrincipal,
    pub organization_id: OrganizationId,
    pub grant_expires_at: UnixSeconds,
}
