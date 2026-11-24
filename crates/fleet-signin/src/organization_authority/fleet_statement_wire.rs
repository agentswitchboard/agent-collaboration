//! Decode the stamp profile without granting identity to unverified claims.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use fleet_contracts::{OrganizationStatementDigest, StatementSeconds, StoredStatementRefusal};
use fleet_identity_protocol::OrganizationId;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

pub(super) fn digest_compact_statement(statement: &str) -> OrganizationStatementDigest {
    let bytes: [u8; 32] = Sha256::digest(statement.as_bytes()).into();
    bytes.into()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct OrganizationStampHeader {
    pub typ: String,
    pub alg: String,
    pub kid: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct OrganizationStampClaims {
    pub purpose: String,
    pub version: u32,
    pub iss: String,
    pub aud: String,
    pub sub: OrganizationId,
    pub organization_id: OrganizationId,
    pub iat: StatementSeconds,
    pub exp: StatementSeconds,
    #[serde(
        default,
        deserialize_with = "present_predecessor",
        skip_serializing_if = "predecessor_missing"
    )]
    #[cfg_attr(
        all(feature = "wire-export", not(target_arch = "wasm32")),
        specta(type = StatementPredecessorDigest)
    )]
    pub replaces: Option<StatementPredecessorDigest>,
}

type StatementPredecessorDigest = OrganizationStatementDigest;

fn present_predecessor<'de, Decoder: Deserializer<'de>>(
    decoder: Decoder,
) -> Result<Option<StatementPredecessorDigest>, Decoder::Error> {
    StatementPredecessorDigest::deserialize(decoder).map(Some)
}

fn predecessor_missing(predecessor: &Option<StatementPredecessorDigest>) -> bool {
    predecessor.is_none()
}

pub(super) struct ParsedFleetStatement {
    pub header: OrganizationStampHeader,
    pub claims: OrganizationStampClaims,
    pub signed: Vec<u8>,
    pub signature: Vec<u8>,
}

impl ParsedFleetStatement {
    pub fn parse(statement: &str) -> Result<Self, StoredStatementRefusal> {
        let mut parts = statement.split('.');
        let header = parts.next().ok_or(StoredStatementRefusal::Malformed)?;
        let claims = parts.next().ok_or(StoredStatementRefusal::Malformed)?;
        let signature = parts.next().ok_or(StoredStatementRefusal::Malformed)?;
        if parts.next().is_some() {
            return Err(StoredStatementRefusal::Malformed);
        }
        Ok(Self {
            header: serde_json::from_slice(&decode(header)?)
                .map_err(|_| StoredStatementRefusal::Malformed)?,
            claims: serde_json::from_slice(&decode(claims)?)
                .map_err(|_| StoredStatementRefusal::Malformed)?,
            signed: format!("{header}.{claims}").into_bytes(),
            signature: decode(signature)?,
        })
    }
}

fn decode(value: &str) -> Result<Vec<u8>, StoredStatementRefusal> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| StoredStatementRefusal::Malformed)
}

/// A stored statement's expiry guard only; this does not establish signature trust.
pub(super) fn stored_statement_unexpired(
    compact: &str,
    now: fleet_runtime_ports::SyncTime,
) -> bool {
    ParsedFleetStatement::parse(compact)
        .is_ok_and(|parsed| now.unix_millis().div_euclid(1000) < parsed.claims.exp.unix_seconds().0)
}
