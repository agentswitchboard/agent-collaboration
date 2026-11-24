//! Fleet identity verification owns no storage, policy lifetime or host routing.
use super::fleet_statement_wire::{ParsedFleetStatement, digest_compact_statement};
use crate::{IssuerKeyCache, IssuerSignatureVerifier};
use fleet_contracts::{
    FleetOrganizationStatementRefusal, IssuerKeyId, OrganizationStatementDigest,
    OrganizationStatementDigestReply, StatementSeconds, StoredStatementRefusal,
};
use fleet_identity_protocol::{OrganizationId, UnixSeconds};
use fleet_runtime_ports::{Clock, SyncTime};
use fleet_runtime_ports::{Ed25519Verification, IssuerKeySource, SignatureValidity};
use std::sync::Arc;

/// Owner choice 3f: a statement lives 120 days and is renewed with 60 days left.
/// The sign-in service owns issuance; central refuses anything longer-lived.
const MAXIMUM_STATEMENT_LIFETIME_SECONDS: i64 = 10_368_000;

/// The configured issuer and audience must both be usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("Fleet organization statement trust configuration is invalid")]
pub struct InvalidStampTrust;

pub struct FleetOrganizationStatementTrust {
    issuer: String,
    audience: String,
    organization: OrganizationId,
}

impl FleetOrganizationStatementTrust {
    /// Construct trust from its configuration values.
    ///
    /// ```compile_fail,E0308
    /// use fleet_contracts::FleetOrganizationStatementRefusal;
    /// use fleet_identity_protocol::OrganizationId;
    /// use fleet_signin::organization_authority::FleetOrganizationStatementTrust;
    /// fn configure_stamp_trust(organization: OrganizationId) {
    ///     let _: Result<_, FleetOrganizationStatementRefusal> =
    ///         FleetOrganizationStatementTrust::new("https://issuer".into(), "central".into(), organization);
    /// }
    /// ```
    pub fn new(
        issuer: String,
        audience: String,
        organization: OrganizationId,
    ) -> Result<Self, InvalidStampTrust> {
        if issuer.is_empty()
            || audience.is_empty()
            || issuer.contains('\0')
            || audience.contains('\0')
        {
            return Err(InvalidStampTrust);
        }
        Ok(Self {
            issuer,
            audience,
            organization,
        })
    }
}

/// A configured verifier checks a signed profile and its current validity.
///
/// ```compile_fail,E0599
/// use fleet_signin::organization_authority::ConfiguredStatementRefusal;
/// let _ = ConfiguredStatementRefusal::InvalidConfiguration;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfiguredStatementRefusal {
    #[error(transparent)]
    Profile(#[from] StoredStatementRefusal),
    #[error(transparent)]
    Time(#[from] StampTimeRefusal),
}

impl From<ConfiguredStatementRefusal> for FleetOrganizationStatementRefusal {
    fn from(refusal: ConfiguredStatementRefusal) -> Self {
        match refusal {
            ConfiguredStatementRefusal::Profile(reason) => reason.into(),
            ConfiguredStatementRefusal::Time(reason) => reason.into(),
        }
    }
}

/// A validity interval can refuse at either of its two time bounds.
///
/// ```compile_fail,E0599
/// use fleet_signin::organization_authority::StampTimeRefusal;
/// let _ = StampTimeRefusal::BadSignature;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StampTimeRefusal {
    #[error("Fleet organization statement has expired")]
    Expired,
    #[error("Fleet organization statement is not yet valid")]
    NotYetValid,
}

impl From<StampTimeRefusal> for FleetOrganizationStatementRefusal {
    fn from(refusal: StampTimeRefusal) -> Self {
        match refusal {
            StampTimeRefusal::Expired => Self::Expired,
            StampTimeRefusal::NotYetValid => Self::NotYetValid,
        }
    }
}

pub struct FleetOrganizationStatementVerifier<Crypto, Keys> {
    signatures: IssuerSignatureVerifier<Crypto, Keys>,
    clock: Arc<dyn Clock>,
    trust: FleetOrganizationStatementTrust,
}

impl<Crypto: Ed25519Verification, Keys: IssuerKeySource>
    FleetOrganizationStatementVerifier<Crypto, Keys>
{
    #[must_use]
    pub fn new(
        crypto: Crypto,
        keys: Keys,
        cache: Arc<IssuerKeyCache>,
        clock: Arc<dyn Clock>,
        trust: FleetOrganizationStatementTrust,
    ) -> Self {
        Self {
            signatures: IssuerSignatureVerifier::new(crypto, keys, cache, clock.clone()),
            clock,
            trust,
        }
    }

    pub async fn verify(
        &self,
        statement: &str,
    ) -> Result<VerifiedFleetOrganizationStatement, ConfiguredStatementRefusal> {
        let verified = self.verify_signature_and_profile(statement).await?;
        // Sample after key fetch/crypto, so verification cannot outlive its exp.
        verified.require_valid_at(self.clock.now())?;
        Ok(verified)
    }

    /// Read signed stored facts without granting current organization standing.
    /// Both time bounds remain facts, including after expiry or clock rollback.
    pub async fn verify_stored_statement_facts(
        &self,
        statement: &str,
    ) -> Result<StoredStatementFacts, StoredStatementRefusal> {
        let verified = self.verify_signature_and_profile(statement).await?;
        Ok(StoredStatementFacts {
            digest: verified.digest,
            key_id: verified.key_id,
            expires_at: verified.expires_at,
        })
    }

    async fn verify_signature_and_profile(
        &self,
        statement: &str,
    ) -> Result<VerifiedFleetOrganizationStatement, StoredStatementRefusal> {
        let parsed = ParsedFleetStatement::parse(statement)?;
        if parsed.header.typ != "fleet-organization-stamp+jwt" {
            return Err(StoredStatementRefusal::WrongTokenType);
        }
        if parsed.header.alg != "EdDSA" {
            return Err(StoredStatementRefusal::UnsupportedAlgorithm);
        }
        if parsed.signature.len() != 64 {
            return Err(StoredStatementRefusal::Malformed);
        }
        let key_id: IssuerKeyId = parsed
            .header
            .kid
            .try_into()
            .map_err(|_| StoredStatementRefusal::Malformed)?;
        let validity = self
            .signatures
            .verify(key_id.as_str(), &parsed.signed, &parsed.signature)
            .await
            .map_err(StoredStatementRefusal::VerificationUnavailable)?;
        if validity != SignatureValidity::Valid {
            return Err(StoredStatementRefusal::BadSignature);
        }
        if parsed.claims.purpose != "fleet-organization-stamp" {
            return Err(StoredStatementRefusal::WrongPurpose);
        }
        if parsed.claims.version != 1 {
            return Err(StoredStatementRefusal::UnsupportedVersion);
        }
        if parsed.claims.iss != self.trust.issuer {
            return Err(StoredStatementRefusal::WrongIssuer);
        }
        if parsed.claims.aud != self.trust.audience {
            return Err(StoredStatementRefusal::WrongAudience);
        }
        if parsed.claims.sub != parsed.claims.organization_id
            || parsed.claims.organization_id != self.trust.organization
        {
            return Err(StoredStatementRefusal::OrganizationMismatch);
        }
        if parsed.claims.exp.unix_seconds().0 <= parsed.claims.iat.unix_seconds().0 {
            return Err(StoredStatementRefusal::Malformed);
        }
        if parsed.claims.exp.unix_seconds().0 - parsed.claims.iat.unix_seconds().0
            > MAXIMUM_STATEMENT_LIFETIME_SECONDS
        {
            return Err(StoredStatementRefusal::LifetimeExceedsPolicy);
        }
        Ok(VerifiedFleetOrganizationStatement {
            organization: parsed.claims.organization_id,
            key_id,
            issued_at: parsed.claims.iat,
            expires_at: parsed.claims.exp,
            replaces: parsed.claims.replaces,
            digest: digest_compact_statement(statement),
        })
    }
}

/// Signed stored metadata for a digest read, without current-time authorization.
/// It exposes neither an authorizing verification nor a conversion into one.
///
/// Both types and their intended consumers remain usable:
///
/// ```
/// use fleet_signin::organization_authority::{
///     StoredStatementFacts, VerifiedFleetOrganizationStatement,
/// };
/// fn authorizing_control(verified: &VerifiedFleetOrganizationStatement) {
///     let _ = verified.organization_id();
/// }
/// fn read_metadata(facts: StoredStatementFacts) -> fleet_contracts::OrganizationStatementDigestReply {
///     facts.into()
/// }
/// ```
///
/// Facts cannot enter the same authorizing policy boundary:
///
/// ```compile_fail,E0308
/// use fleet_signin::organization_authority::{StoredStatementFacts, VerifiedFleetOrganizationStatement};
/// fn require_verified_stamp(_verified: &VerifiedFleetOrganizationStatement) {}
/// fn cannot_authorize(facts: &StoredStatementFacts) {
///     require_verified_stamp(facts);
/// }
/// ```
#[derive(Debug)]
pub struct StoredStatementFacts {
    digest: OrganizationStatementDigest,
    key_id: IssuerKeyId,
    expires_at: StatementSeconds,
}

#[cfg(test)]
impl StoredStatementFacts {
    #[must_use]
    fn digest(&self) -> &OrganizationStatementDigest {
        &self.digest
    }
    #[must_use]
    fn key_id(&self) -> &str {
        self.key_id.as_str()
    }
    #[must_use]
    const fn expires_at(&self) -> UnixSeconds {
        self.expires_at.unix_seconds()
    }
}

impl From<StoredStatementFacts> for OrganizationStatementDigestReply {
    fn from(facts: StoredStatementFacts) -> Self {
        Self {
            digest: facts.digest,
            expires_at: facts.expires_at,
            signing_key_id: facts.key_id,
        }
    }
}

/// Constructed only by the signature/profile verifier; not a person permission.
///
/// The applying-time recheck belongs to sign-in.
///
/// ```compile_fail,E0624
/// use fleet_signin::organization_authority::VerifiedFleetOrganizationStatement;
/// fn recheck_verified_stamp_time(verified: &VerifiedFleetOrganizationStatement, now: fleet_runtime_ports::SyncTime) {
///     let _ = verified.require_valid_at(now);
/// }
/// ```
#[derive(Debug)]
pub struct VerifiedFleetOrganizationStatement {
    organization: OrganizationId,
    key_id: IssuerKeyId,
    issued_at: StatementSeconds,
    expires_at: StatementSeconds,
    replaces: Option<OrganizationStatementDigest>,
    digest: OrganizationStatementDigest,
}

impl VerifiedFleetOrganizationStatement {
    #[must_use]
    pub fn organization_id(&self) -> &OrganizationId {
        &self.organization
    }
    #[cfg(test)]
    #[must_use]
    pub(crate) fn key_id(&self) -> &str {
        self.key_id.as_str()
    }
    #[must_use]
    pub const fn issued_at(&self) -> UnixSeconds {
        self.issued_at.unix_seconds()
    }
    #[must_use]
    pub const fn expires_at(&self) -> UnixSeconds {
        self.expires_at.unix_seconds()
    }
    #[must_use]
    pub fn replaces(&self) -> Option<&OrganizationStatementDigest> {
        self.replaces.as_ref()
    }
    #[must_use]
    pub fn digest(&self) -> &OrganizationStatementDigest {
        &self.digest
    }

    /// Recheck only time at the applying sample.
    pub(crate) fn require_valid_at(&self, now: SyncTime) -> Result<(), StampTimeRefusal> {
        let now_seconds = now.unix_millis().div_euclid(1000);
        if now_seconds < self.issued_at.unix_seconds().0 {
            return Err(StampTimeRefusal::NotYetValid);
        }
        if now_seconds >= self.expires_at.unix_seconds().0 {
            return Err(StampTimeRefusal::Expired);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "fleet_statement_test.rs"]
mod tests;
