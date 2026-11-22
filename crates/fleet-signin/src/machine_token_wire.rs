//! Serde headers and claims for the Central machine access-token profile.
use fleet_contracts::MachineTokenClaims;
use fleet_identity_protocol::UnixSeconds;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct MachineAccessTokenHeader {
    pub(super) alg: String,
    pub(super) typ: String,
    pub(super) kid: String,
}

/// The complete payload Central inspects. Unrecognized issuer claims are ignored.
#[derive(Deserialize, Serialize)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct MachineAccessTokenClaims {
    pub(super) iss: String,
    pub(super) aud: String,
    pub(super) iat: UnixSeconds,
    pub(super) exp: UnixSeconds,
    pub(super) client_id: String,
    pub(super) azp: String,
    #[serde(flatten)]
    pub(super) machine: MachineTokenClaims,
    #[serde(default, rename = "sid", skip_serializing)]
    _sid: Option<()>,
    #[serde(
        default,
        rename = "workspace_id",
        deserialize_with = "reject_workspace_claim",
        skip_serializing_if = "workspace_claim_absent"
    )]
    #[cfg_attr(
        all(feature = "wire-export", not(target_arch = "wasm32")),
        specta(type = specta_typescript::Never<ForbiddenWorkspaceClaim>)
    )]
    _workspace_id: ForbiddenWorkspaceClaim,
}

type ForbiddenWorkspaceClaim = ();

fn reject_workspace_claim<'de, D: serde::Deserializer<'de>>(
    _: D,
) -> Result<ForbiddenWorkspaceClaim, D::Error> {
    Err(serde::de::Error::custom("workspace_id is forbidden"))
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde calls skip_serializing_if with a field reference"
)]
fn workspace_claim_absent(_: &ForbiddenWorkspaceClaim) -> bool {
    true
}
