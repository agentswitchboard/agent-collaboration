//! JSON inside a person's signed access token and Central-specific DPoP proof.
use crate::identity_contract::FenceRevision;
use fleet_identity_protocol::{UnixMillis, UnixSeconds};
use fleet_runtime_ports::ProofJwk;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[cfg_attr(
    all(test, feature = "wire-export", not(target_arch = "wasm32")),
    derive(serde::Serialize)
)]
pub(crate) struct PersonTokenHeader {
    pub(super) alg: String,
    pub(super) kid: String,
    pub(super) typ: Option<String>,
}

/// RFC 7519 allows one audience string or an array of them.
#[derive(Deserialize)]
#[serde(untagged)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[cfg_attr(
    all(test, feature = "wire-export", not(target_arch = "wasm32")),
    derive(serde::Serialize)
)]
pub(super) enum PersonTokenAudience {
    One(String),
    Many(Vec<String>),
}

impl PersonTokenAudience {
    pub(super) fn names(&self, expected: &str) -> bool {
        match self {
            Self::One(one) => one == expected,
            Self::Many(many) => many.iter().any(|one| one == expected),
        }
    }
}

#[derive(Deserialize)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[cfg_attr(
    all(test, feature = "wire-export", not(target_arch = "wasm32")),
    derive(serde::Serialize)
)]
pub(super) struct PersonTokenConfirmation {
    pub(super) jkt: String,
}

#[derive(Deserialize)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[cfg_attr(
    all(test, feature = "wire-export", not(target_arch = "wasm32")),
    derive(serde::Serialize)
)]
pub(crate) struct PersonClaims {
    pub(super) iss: String,
    pub(super) aud: PersonTokenAudience,
    pub(super) sub: String,
    pub(super) sid: String,
    pub(super) iat: UnixSeconds,
    pub(super) exp: UnixSeconds,
    pub(super) cnf: PersonTokenConfirmation,
    pub(super) token_profile: String,
    pub(super) organization_id: String,
    pub(super) authorization_subject_id: String,
    pub(super) person_revision: FenceRevision,
    pub(super) organization_revision: FenceRevision,
    pub(super) permission_policy_digest: String,
    pub(super) permissions: Vec<String>,
    pub(super) second_factor_at: Option<UnixSeconds>,
    #[serde(default)]
    pub(super) second_factor_method: Option<String>,
    pub(super) session_expires_at_ms: UnixMillis,
    pub(super) authorization_snapshot_at_ms: UnixMillis,
    pub(super) authorization_snapshot_valid_until_ms: UnixMillis,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[cfg_attr(
    all(test, feature = "wire-export", not(target_arch = "wasm32")),
    derive(serde::Serialize)
)]
pub(crate) struct ProofHeader {
    pub(super) typ: String,
    pub(super) alg: String,
    pub(super) jwk: ProofJwk,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[cfg_attr(
    all(test, feature = "wire-export", not(target_arch = "wasm32")),
    derive(serde::Serialize)
)]
pub(crate) struct ProofClaims {
    pub(super) jti: String,
    pub(super) htm: String,
    pub(super) htu: String,
    pub(super) iat: UnixSeconds,
    pub(super) ath: String,
    pub(super) intent: String,
}

#[cfg(test)]
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
#[path = "signed_claims_test.rs"]
mod tests;
#[cfg(all(test, feature = "wire-export", not(target_arch = "wasm32")))]
pub(crate) use tests::signed_claims_specimens;
