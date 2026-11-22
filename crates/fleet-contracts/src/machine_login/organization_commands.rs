use super::PersonId;
use message_board::RecordId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Permission required for the private machine revocation command.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum RevokePermission {
    #[serde(rename = "machine:revoke")]
    MachineRevoke,
}

impl RevokePermission {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MachineRevoke => "machine:revoke",
        }
    }
}

/// The actor and proof already admitted for the owning revoke operation.
pub struct AdmittedRevoke {
    pub actor: PersonId,
    pub proof_request_id: RecordId,
    pub permission: RevokePermission,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AllowanceState {
    Active,
    Withdrawn,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum EnrollmentRevocationReason {
    Owner {},
    Superseded {},
}

/// Maximum time since second factor for a machine authorization change.
pub const PERSON_SECOND_FACTOR_WINDOW_SECONDS: i64 = 900;
