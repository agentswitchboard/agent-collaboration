//! The bearer exchanged by Sign-in for one person WebSocket upgrade.

use super::{
    compact_jwt::{CompactJwt, MAX_SAFE_INTEGER, from_json, uuid_v7},
    refusal::PersonRequestRefusal,
    signed_claims::PersonTokenAudience,
    verifier::{PersonHeaderPolicy, PersonLifetime, PersonProofVerifier, validate_person_lifetime},
};
use crate::{FenceRevision, FleetPermission};
use fleet_identity_protocol::{OrganizationId, UnixMillis, UnixSeconds};
use fleet_runtime_ports::{Ed25519Verification, Es256Verification, IssuerKeySource};
use serde::Deserialize;
use workspace_ids::PersonId;

const MAX_UPGRADE_LIFETIME_SECONDS: i64 = 60;
const PERSON_LINK_PROFILE: &str = "person-link";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonLinkClaims {
    iss: String,
    aud: PersonTokenAudience,
    sub: PersonId,
    iat: UnixSeconds,
    exp: UnixSeconds,
    jti: String,
    token_profile: String,
    organization_id: OrganizationId,
    authorization_subject_id: String,
    person_revision: FenceRevision,
    organization_revision: FenceRevision,
    permission_policy_digest: String,
    permissions: Vec<FleetPermission>,
    session_expires_at_ms: UnixMillis,
    authorization_snapshot_at_ms: UnixMillis,
    authorization_snapshot_valid_until_ms: UnixMillis,
    second_factor_at: Option<UnixSeconds>,
    second_factor_method: Option<String>,
}

/// Unsigned input only chooses the verifier. Every trusted claim is read again after signature verification.
pub fn is_person_link_candidate(token: &str) -> bool {
    #[derive(Deserialize)]
    struct ProfileHint {
        token_profile: String,
    }
    CompactJwt::parse(token)
        .ok()
        .and_then(|parsed| serde_json::from_slice::<ProfileHint>(&parsed.claims).ok())
        .is_some_and(|claims| claims.token_profile == PERSON_LINK_PROFILE)
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PersonLinkSecondFactor {
    pub at: UnixSeconds,
    pub method: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersonLinkFacts {
    pub organization_id: OrganizationId,
    pub person_id: PersonId,
    pub authorization_subject_id: String,
    pub person_revision: FenceRevision,
    pub organization_revision: FenceRevision,
    pub permission_policy_digest: String,
    pub permissions: Vec<FleetPermission>,
    pub second_factor: Option<PersonLinkSecondFactor>,
    pub authorization_snapshot_at_ms: UnixMillis,
    pub authorization_snapshot_valid_until_ms: UnixMillis,
    pub session_expires_at_ms: UnixMillis,
}

/// Successful signature and upgrade-time claims checks. Live floors still apply.
#[derive(Clone, Debug)]
pub struct CheckedPersonLink {
    facts: PersonLinkFacts,
    upgrade_expires_at_ms: i64,
}

impl CheckedPersonLink {
    pub fn upgrade_expires_at_ms(&self) -> i64 {
        self.upgrade_expires_at_ms
    }

    pub fn facts(&self) -> &PersonLinkFacts {
        &self.facts
    }
}

impl<Ed: Ed25519Verification, Es: Es256Verification, Keys: IssuerKeySource>
    PersonProofVerifier<Ed, Es, Keys>
{
    pub async fn verify_person_link(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<CheckedPersonLink, PersonRequestRefusal> {
        if audience.is_empty() || self.trusted_issuer().is_empty() {
            return Err(PersonRequestRefusal::Policy);
        }
        let token = self
            .verify_signed_token(token, PersonHeaderPolicy::Link)
            .await?;
        let claims: PersonLinkClaims =
            from_json(&token.claims, PersonRequestRefusal::PersonClaims)?;
        if claims.iss != self.trusted_issuer() {
            return Err(PersonRequestRefusal::Issuer);
        }
        if !claims.aud.names(audience) {
            return Err(PersonRequestRefusal::Audience);
        }
        if claims.token_profile != PERSON_LINK_PROFILE {
            return Err(PersonRequestRefusal::Profile);
        }
        let now_ms = self.clock_millis();
        if claims.iat.0 > now_ms.div_euclid(1000)
            || claims
                .exp
                .0
                .checked_sub(claims.iat.0)
                .is_none_or(|lifetime| !(1..=MAX_UPGRADE_LIFETIME_SECONDS).contains(&lifetime))
            || claims.permission_policy_digest.is_empty()
        {
            return Err(PersonRequestRefusal::PersonClaims);
        }
        uuid_v7(&claims.jti, PersonRequestRefusal::PersonClaims)?;
        uuid_v7(
            &claims.authorization_subject_id,
            PersonRequestRefusal::PersonClaims,
        )?;
        let second_factor = match (claims.second_factor_at, claims.second_factor_method) {
            (None, None) => None,
            (Some(at), Some(method)) if at.0 <= MAX_SAFE_INTEGER / 1000 && !method.is_empty() => {
                Some(PersonLinkSecondFactor { at, method })
            }
            (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
                return Err(PersonRequestRefusal::PersonClaims);
            }
        };
        let upgrade_expires_at_ms = validate_person_lifetime(
            PersonLifetime {
                expires_seconds: claims.exp.0,
                snapshot_ms: claims.authorization_snapshot_at_ms.0,
                snapshot_expires_ms: claims.authorization_snapshot_valid_until_ms.0,
                session_expires_ms: claims.session_expires_at_ms.0,
            },
            now_ms,
        )?;
        Ok(CheckedPersonLink {
            upgrade_expires_at_ms,
            facts: PersonLinkFacts {
                organization_id: claims.organization_id,
                person_id: claims.sub,
                authorization_subject_id: claims.authorization_subject_id,
                person_revision: claims.person_revision,
                organization_revision: claims.organization_revision,
                permission_policy_digest: claims.permission_policy_digest,
                permissions: claims.permissions,
                second_factor,
                authorization_snapshot_at_ms: claims.authorization_snapshot_at_ms,
                authorization_snapshot_valid_until_ms: claims.authorization_snapshot_valid_until_ms,
                session_expires_at_ms: claims.session_expires_at_ms,
            },
        })
    }
}
