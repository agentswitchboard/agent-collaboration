//! Verify the signed person token and its request-bound DPoP proof.
use super::{
    compact_jwt::{
        CompactJwt, MAX_SAFE_INTEGER, decode, from_json, sha256_url, uuid_v7, uuid_v7_millis,
    },
    refusal::PersonRequestRefusal,
    signed_claims::{PersonClaims, PersonTokenHeader, ProofClaims, ProofHeader},
};
use crate::IssuerSignatureVerifier;
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::{
    Clock, Ed25519Verification, Es256Verification, IssuerKeySource, ProofJwk, SignatureValidity,
};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

/// The deployment's trust: who signs person tokens, the audience they name for
/// central's admin surface, and central's configured external origin.
#[derive(Clone, Debug)]
pub struct PersonRequestPolicy {
    pub issuer: String,
    pub audience: String,
    /// `https://host[:port]` only; a route's `htu` is this plus its path.
    pub public_origin: String,
}

/// The facts the applying transaction admits or refuses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersonProofFacts {
    pub person_id: String,
    pub session_id: String,
    pub organization_id: OrganizationId,
    pub authorization_subject_id: String,
    pub person_revision: i64,
    pub organization_revision: i64,
    pub permission_policy_digest: String,
    pub permissions: Vec<String>,
    /// Seconds; absent when the session never passed a second factor.
    pub second_factor_at: Option<i64>,
    /// The fresh request ID (the proof's `jti`); only `organization_used_requests.id`.
    pub request_id: Uuid,
    pub request_time_ms: i64,
    pub proof_time_ms: i64,
    pub proof_key_thumbprint: String,
    pub intent_digest: String,
    /// min(exp, session deadline, snapshot deadline), milliseconds.
    pub effective_expiry_ms: i64,
}

/// Successful cryptographic verification. The transport operation still needs to
/// match the proof's signed intent before the applying policy may use it.
#[derive(Debug)]
pub struct CheckedPersonProof {
    facts: PersonProofFacts,
}

impl CheckedPersonProof {
    #[must_use]
    pub const fn facts(&self) -> &PersonProofFacts {
        &self.facts
    }

    pub fn bind_intent(
        self,
        organization: &OrganizationId,
        intent_digest: &str,
    ) -> Result<PersonProofFacts, PersonRequestRefusal> {
        if intent_digest != self.facts.intent_digest {
            return Err(PersonRequestRefusal::Intent);
        }
        if organization != &self.facts.organization_id {
            return Err(PersonRequestRefusal::OrganizationMismatch);
        }
        Ok(self.facts)
    }
}

pub(super) enum PersonHeaderPolicy {
    Admin,
    Link,
}

impl PersonHeaderPolicy {
    fn accepts(self, header: Option<&str>) -> bool {
        match self {
            Self::Admin => matches!(header, None | Some("JWT" | "at+jwt")),
            Self::Link => header == Some("at+jwt"),
        }
    }
}

pub struct PersonProofVerifier<Ed, Es, Keys> {
    signatures: IssuerSignatureVerifier<Ed, Keys>,
    proofs: Es,
    clock: Arc<dyn Clock>,
    policy: PersonRequestPolicy,
}

impl<Ed: Ed25519Verification, Es: Es256Verification, Keys: IssuerKeySource>
    PersonProofVerifier<Ed, Es, Keys>
{
    #[must_use]
    pub fn new(
        signatures: IssuerSignatureVerifier<Ed, Keys>,
        proofs: Es,
        clock: Arc<dyn Clock>,
        policy: PersonRequestPolicy,
    ) -> Self {
        Self {
            signatures,
            proofs,
            clock,
            policy,
        }
    }

    pub(super) async fn verify_signed_token(
        &self,
        token: &str,
        header_policy: PersonHeaderPolicy,
    ) -> Result<CompactJwt, PersonRequestRefusal> {
        let parsed_token = CompactJwt::parse(token)?;
        let header: PersonTokenHeader = from_json(
            &parsed_token.header,
            PersonRequestRefusal::UnsupportedHeader,
        )?;
        if header.alg != "EdDSA"
            || header.kid.is_empty()
            || !header_policy.accepts(header.typ.as_deref())
        {
            return Err(PersonRequestRefusal::UnsupportedHeader);
        }
        match self
            .signatures
            .verify(&header.kid, &parsed_token.signed, &parsed_token.signature)
            .await
        {
            Ok(SignatureValidity::Valid) => {}
            Ok(SignatureValidity::Invalid) => return Err(PersonRequestRefusal::TokenSignature),
            Err(fleet_contracts::IssuerSignatureRefusal::Keys(
                fleet_contracts::IssuerKeysUnavailable::MalformedKeySet
                | fleet_contracts::IssuerKeysUnavailable::CacheUnavailable,
            )) => {
                return Err(PersonRequestRefusal::IssuerKey);
            }
            Err(
                fleet_contracts::IssuerSignatureRefusal::Keys(
                    fleet_contracts::IssuerKeysUnavailable::Fetch { .. }
                    | fleet_contracts::IssuerKeysUnavailable::InvalidClock,
                )
                | fleet_contracts::IssuerSignatureRefusal::Crypto { .. },
            ) => return Err(PersonRequestRefusal::VerificationUnavailable),
        }
        Ok(parsed_token)
    }

    pub(super) fn clock_millis(&self) -> i64 {
        self.clock.now().unix_millis()
    }

    pub(super) fn trusted_issuer(&self) -> &str {
        &self.policy.issuer
    }

    pub async fn verify(
        &self,
        path: &str,
        method: &str,
        authorization: &str,
        proof: &str,
    ) -> Result<CheckedPersonProof, PersonRequestRefusal> {
        let htu = expected_htu(&self.policy, path)?;
        let token = authorization
            .strip_prefix("DPoP ")
            .filter(|token| !token.is_empty() && !token.contains(char::is_whitespace))
            .ok_or(PersonRequestRefusal::DpopRequired)?;
        let parsed_token = self
            .verify_signed_token(token, PersonHeaderPolicy::Admin)
            .await?;
        let person: PersonClaims =
            from_json(&parsed_token.claims, PersonRequestRefusal::PersonClaims)?;
        let proof = CompactJwt::parse(proof)?;
        let proof_header: ProofHeader =
            from_json(&proof.header, PersonRequestRefusal::UnsupportedHeader)?;
        if proof_header.typ != "dpop+jwt" || proof_header.alg != "ES256" {
            return Err(PersonRequestRefusal::UnsupportedHeader);
        }
        let thumbprint = proof_thumbprint(&proof_header.jwk)?;
        match self
            .proofs
            .verify_es256(&proof_header.jwk, &proof.signed, &proof.signature)
            .await
        {
            Ok(SignatureValidity::Valid) => {}
            Ok(SignatureValidity::Invalid) => return Err(PersonRequestRefusal::ProofSignature),
            Err(_) => return Err(PersonRequestRefusal::VerificationUnavailable),
        }
        let claims: ProofClaims = from_json(&proof.claims, PersonRequestRefusal::MalformedJwt)?;
        // Preliminary time checks on a clock sampled after the crypto; the
        // applying transaction samples again and rechecks every bound.
        let now_ms = self.clock.now().unix_millis();
        let effective_expiry_ms = validate_person(&person, &self.policy, now_ms)?;
        if thumbprint != person.cnf.jkt {
            return Err(PersonRequestRefusal::KeyBinding);
        }
        if method != "POST" || claims.htm != method {
            return Err(PersonRequestRefusal::Method);
        }
        if claims.htu != htu {
            return Err(PersonRequestRefusal::Target);
        }
        let proof_time_ms = claims
            .iat
            .0
            .checked_mul(1000)
            .ok_or(PersonRequestRefusal::ProofTime)?;
        if !in_request_window(proof_time_ms, now_ms) {
            return Err(PersonRequestRefusal::ProofTime);
        }
        let request_id = uuid_v7(&claims.jti, PersonRequestRefusal::RequestId)?;
        let request_time_ms = uuid_v7_millis(request_id).ok_or(PersonRequestRefusal::RequestId)?;
        if !in_request_window(request_time_ms, now_ms) {
            return Err(PersonRequestRefusal::RequestTime);
        }
        if claims.ath != sha256_url(token.as_bytes()) {
            return Err(PersonRequestRefusal::TokenHash);
        }
        let second_factor_at = person.second_factor_at.map(|timestamp| timestamp.0);
        let organization_id: OrganizationId = person
            .organization_id
            .clone()
            .try_into()
            .map_err(|_| PersonRequestRefusal::PersonClaims)?;
        Ok(CheckedPersonProof {
            facts: PersonProofFacts {
                person_id: person.sub,
                session_id: person.sid,
                organization_id,
                authorization_subject_id: person.authorization_subject_id,
                person_revision: person.person_revision.get(),
                organization_revision: person.organization_revision.get(),
                permission_policy_digest: person.permission_policy_digest,
                permissions: person.permissions,
                second_factor_at,
                request_id,
                request_time_ms,
                proof_time_ms,
                proof_key_thumbprint: thumbprint,
                intent_digest: claims.intent,
                effective_expiry_ms,
            },
        })
    }
}

/// `N − 65000 < T ≤ N + 5000` (T04 with the 5-second leeway on both sides).
pub fn in_request_window(timestamp_ms: i64, now_ms: i64) -> bool {
    timestamp_ms > now_ms - 65_000 && timestamp_ms <= now_ms + 5_000
}

fn expected_htu(policy: &PersonRequestPolicy, path: &str) -> Result<String, PersonRequestRefusal> {
    let origin =
        url::Url::parse(&policy.public_origin).map_err(|_| PersonRequestRefusal::Policy)?;
    if origin.scheme() != "https"
        || origin.host_str().is_none()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.query().is_some()
        || origin.fragment().is_some()
        || origin.path() != "/"
        || policy.issuer.is_empty()
        || policy.audience.is_empty()
    {
        return Err(PersonRequestRefusal::Policy);
    }
    Ok(format!("{}{}", origin.origin().ascii_serialization(), path))
}

fn validate_person(
    person: &PersonClaims,
    policy: &PersonRequestPolicy,
    now_ms: i64,
) -> Result<i64, PersonRequestRefusal> {
    if person.iss != policy.issuer {
        return Err(PersonRequestRefusal::Issuer);
    }
    if !person.aud.names(&policy.audience) {
        return Err(PersonRequestRefusal::Audience);
    }
    if person.token_profile != "person-fleet" {
        return Err(PersonRequestRefusal::Profile);
    }
    for id in [
        &person.sub,
        &person.sid,
        &person.organization_id,
        &person.authorization_subject_id,
    ] {
        uuid_v7(id, PersonRequestRefusal::PersonClaims)?;
    }
    if decode(&person.cnf.jkt, PersonRequestRefusal::PersonClaims)?.len() != 32
        || person.iat.0 > now_ms.div_euclid(1000)
        || person.exp <= person.iat
        || person.permission_policy_digest.is_empty()
        || person
            .second_factor_at
            .is_some_and(|at| at.0 > MAX_SAFE_INTEGER / 1000)
        || person.second_factor_at.is_some() != person.second_factor_method.is_some()
    {
        return Err(PersonRequestRefusal::PersonClaims);
    }
    validate_person_lifetime(
        PersonLifetime {
            expires_seconds: person.exp.0,
            snapshot_ms: person.authorization_snapshot_at_ms.0,
            snapshot_expires_ms: person.authorization_snapshot_valid_until_ms.0,
            session_expires_ms: person.session_expires_at_ms.0,
        },
        now_ms,
    )
}

pub(super) struct PersonLifetime {
    pub(super) expires_seconds: i64,
    pub(super) snapshot_ms: i64,
    pub(super) snapshot_expires_ms: i64,
    pub(super) session_expires_ms: i64,
}

pub(super) fn validate_person_lifetime(
    lifetime: PersonLifetime,
    now_ms: i64,
) -> Result<i64, PersonRequestRefusal> {
    let jwt_expiry_ms = lifetime
        .expires_seconds
        .checked_mul(1000)
        .filter(|value| *value <= MAX_SAFE_INTEGER)
        .ok_or(PersonRequestRefusal::ExpiryBounds)?;
    let snapshot = lifetime.snapshot_ms;
    let snapshot_deadline = lifetime.snapshot_expires_ms;
    let session_deadline = lifetime.session_expires_ms;
    if snapshot > now_ms
        || snapshot_deadline <= snapshot
        || snapshot_deadline - snapshot > 300_000
        || session_deadline <= snapshot
    {
        return Err(PersonRequestRefusal::ExpiryBounds);
    }
    let effective_expiry_ms = jwt_expiry_ms.min(snapshot_deadline).min(session_deadline);
    if now_ms >= effective_expiry_ms {
        return Err(PersonRequestRefusal::Expired);
    }
    Ok(effective_expiry_ms)
}

fn proof_thumbprint(key: &ProofJwk) -> Result<String, PersonRequestRefusal> {
    if key.kty != "EC"
        || key.crv != "P-256"
        || decode(&key.x, PersonRequestRefusal::ProofKey)?.len() != 32
        || decode(&key.y, PersonRequestRefusal::ProofKey)?.len() != 32
    {
        return Err(PersonRequestRefusal::ProofKey);
    }
    // RFC 7638: exactly these members in lexicographic order, no whitespace.
    #[derive(Serialize)]
    struct Thumbprint<'a> {
        crv: &'a str,
        kty: &'a str,
        x: &'a str,
        y: &'a str,
    }
    let canonical = serde_json::to_vec(&Thumbprint {
        crv: &key.crv,
        kty: &key.kty,
        x: &key.x,
        y: &key.y,
    })
    .map_err(|_| PersonRequestRefusal::ProofKey)?;
    Ok(sha256_url(&canonical))
}
