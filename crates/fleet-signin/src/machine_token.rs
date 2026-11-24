//! Verify the issuer's machine access-token profile before live admission.
use crate::IssuerSignatureVerifier;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use fleet_contracts::ClientId;
use fleet_contracts::{MachinePrincipal, MachineTokenClaims};
use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixMillis, UnixSeconds};
use fleet_runtime_ports::Clock;
use fleet_runtime_ports::{Ed25519Verification, IssuerKeySource, SignatureValidity};
use std::sync::Arc;
use workspace_ids::MachineId;

#[path = "machine_token_wire.rs"]
mod machine_token_wire;
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
pub use machine_token_wire::{MachineAccessTokenClaims, MachineAccessTokenHeader};
#[cfg(not(all(feature = "wire-export", not(target_arch = "wasm32"))))]
use machine_token_wire::{MachineAccessTokenClaims, MachineAccessTokenHeader};

pub const MACHINE_TOKEN_LIFETIME_SECONDS: i64 = 600;

#[derive(Clone, Debug)]
pub struct MachineTokenPolicy {
    pub issuer: String,
    pub audience: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedMachineToken {
    organization_id: OrganizationId,
    machine_id: MachineId,
    enrollment_id: EnrollmentId,
    client_id: ClientId,
    effective_expires_at: UnixSeconds,
}

impl VerifiedMachineToken {
    #[must_use]
    pub fn organization_id(&self) -> &OrganizationId {
        &self.organization_id
    }
    #[must_use]
    pub fn machine_id(&self) -> &MachineId {
        &self.machine_id
    }
    #[must_use]
    pub fn enrollment_id(&self) -> &EnrollmentId {
        &self.enrollment_id
    }
    #[must_use]
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }
    #[must_use]
    pub fn effective_expires_at(&self) -> UnixSeconds {
        self.effective_expires_at
    }
}

/// Machine verification exposes a rejection or an unavailable check.
///
/// ```compile_fail,E0599
/// use fleet_signin::machine_token::MachineTokenRefusal;
/// let _ = MachineTokenRefusal::UnknownKey;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MachineTokenRefusal {
    #[error("machine token rejected")]
    Rejected,
    #[error("machine token verification unavailable")]
    Unavailable,
}

pub struct MachineTokenVerifier<Ed, Keys> {
    signatures: IssuerSignatureVerifier<Ed, Keys>,
    clock: Arc<dyn Clock>,
    policy: MachineTokenPolicy,
}

impl<Ed: Ed25519Verification, Keys: IssuerKeySource> MachineTokenVerifier<Ed, Keys> {
    #[must_use]
    pub fn new(
        signatures: IssuerSignatureVerifier<Ed, Keys>,
        clock: Arc<dyn Clock>,
        policy: MachineTokenPolicy,
    ) -> Self {
        Self {
            signatures,
            clock,
            policy,
        }
    }

    pub async fn verify(&self, token: &str) -> Result<VerifiedMachineToken, MachineTokenRefusal> {
        if self.policy.issuer.is_empty() || self.policy.audience.is_empty() {
            return Err(MachineTokenRefusal::Unavailable);
        }
        let parsed = CompactMachineToken::parse(token)?;
        let header: MachineAccessTokenHeader =
            serde_json::from_slice(&parsed.header).map_err(|_| MachineTokenRefusal::Rejected)?;
        if header.alg != "EdDSA" || header.typ != "at+jwt" || header.kid.is_empty() {
            return Err(MachineTokenRefusal::Rejected);
        }
        let claims: MachineAccessTokenClaims =
            serde_json::from_slice(&parsed.claims).map_err(|_| MachineTokenRefusal::Rejected)?;
        if claims.iss != self.policy.issuer {
            return Err(MachineTokenRefusal::Rejected);
        }
        match self
            .signatures
            .verify(&header.kid, &parsed.signed, &parsed.signature)
            .await
        {
            Ok(SignatureValidity::Valid) => {}
            Ok(SignatureValidity::Invalid) => return Err(MachineTokenRefusal::Rejected),
            Err(
                fleet_contracts::IssuerSignatureRefusal::Keys(
                    fleet_contracts::IssuerKeysUnavailable::MalformedKeySet
                    | fleet_contracts::IssuerKeysUnavailable::CacheUnavailable
                    | fleet_contracts::IssuerKeysUnavailable::InvalidClock
                    | fleet_contracts::IssuerKeysUnavailable::Fetch { .. },
                )
                | fleet_contracts::IssuerSignatureRefusal::Crypto { .. },
            ) => return Err(MachineTokenRefusal::Unavailable),
        }
        let now = self.clock.now().unix_millis().div_euclid(1000);
        claims.validate(&self.policy, now)
    }
}

impl MachineAccessTokenClaims {
    fn validate(
        self,
        policy: &MachineTokenPolicy,
        now: i64,
    ) -> Result<VerifiedMachineToken, MachineTokenRefusal> {
        let rejected = MachineTokenRefusal::Rejected;
        let MachineTokenClaims {
            principal:
                MachinePrincipal::Machine {
                    machine_id,
                    enrollment_id,
                },
            organization_id,
            grant_expires_at: UnixSeconds(grant_expires_at),
        } = self.machine;
        let UnixSeconds(issued_at) = self.iat;
        let UnixSeconds(expires_at) = self.exp;
        if self.iss != policy.issuer
            || self.aud != policy.audience
            || self.client_id != self.azp
            || self.client_id != enrollment_id.as_str()
            || expires_at <= issued_at
            || expires_at - issued_at > MACHINE_TOKEN_LIFETIME_SECONDS
        {
            return Err(rejected);
        }
        let expiry = expires_at
            .checked_add(60)
            .and_then(|seconds| UnixSeconds::try_from(seconds).ok())
            .ok_or(rejected)?
            .0;
        let future = now.checked_add(60).ok_or(rejected)?;
        if issued_at > future || now >= expiry || now >= grant_expires_at {
            return Err(rejected);
        }
        let effective_expires_at = expiry.min(grant_expires_at);
        effective_expires_at
            .checked_mul(1000)
            .and_then(|millis| UnixMillis::try_from(millis).ok())
            .ok_or(rejected)?;
        for id in [
            organization_id.as_str(),
            enrollment_id.as_str(),
            self.client_id.as_str(),
            self.azp.as_str(),
        ] {
            let uuid = uuid::Uuid::parse_str(id).map_err(|_| rejected)?;
            if !fleet_runtime_ports::uuid_v7(uuid) || uuid.to_string() != id {
                return Err(rejected);
            }
        }
        Ok(VerifiedMachineToken {
            organization_id,
            machine_id,
            enrollment_id,
            client_id: self.client_id.try_into().map_err(|_| rejected)?,
            effective_expires_at: UnixSeconds(effective_expires_at),
        })
    }
}

struct CompactMachineToken {
    header: Vec<u8>,
    claims: Vec<u8>,
    signed: Vec<u8>,
    signature: Vec<u8>,
}
impl CompactMachineToken {
    fn parse(token: &str) -> Result<Self, MachineTokenRefusal> {
        let rejected = MachineTokenRefusal::Rejected;
        if token.len() > 16_384 {
            return Err(rejected);
        }
        let mut parts = token.split('.');
        let header = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or(rejected)?;
        let claims = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or(rejected)?;
        let signature = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or(rejected)?;
        if parts.next().is_some() {
            return Err(rejected);
        }
        fn decode(part: &str) -> Result<Vec<u8>, MachineTokenRefusal> {
            let bytes = URL_SAFE_NO_PAD
                .decode(part)
                .map_err(|_| MachineTokenRefusal::Rejected)?;
            if URL_SAFE_NO_PAD.encode(&bytes) != part {
                return Err(MachineTokenRefusal::Rejected);
            }
            Ok(bytes)
        }
        Ok(Self {
            header: decode(header)?,
            claims: decode(claims)?,
            signed: format!("{header}.{claims}").into_bytes(),
            signature: decode(signature)?,
        })
    }
}

#[cfg(test)]
#[path = "machine_token_test.rs"]
mod tests;
