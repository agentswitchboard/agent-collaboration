//! Issuer key decoding and five-minute retention, separate from legacy Access.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use fleet_contracts::{IssuerKeyId, IssuerKeysUnavailable};
use fleet_runtime_ports::RawJwks;
use serde::Deserialize;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition,
};
use std::{collections::BTreeSet, sync::Mutex};

const MAXIMUM_KEY_AGE_SECONDS: u64 = 300;

#[derive(Clone)]
pub(crate) struct IssuerVerificationKey {
    key_id: IssuerKeyId,
    public_key: [u8; 32],
}

impl IssuerVerificationKey {
    #[must_use]
    pub(crate) const fn public_key(&self) -> &[u8; 32] {
        &self.public_key
    }
}

#[derive(Clone)]
pub(crate) struct IssuerKeySet {
    keys: Vec<IssuerVerificationKey>,
}

// This is a projection of the provider-owned public JWK envelope. Registered
// extensions may be present; every field used for verification is typed here.
#[derive(Deserialize)]
struct PublishedIssuerKeys {
    keys: Vec<PublishedIssuerKey>,
}

#[derive(Deserialize)]
struct PublishedIssuerKey {
    #[serde(rename = "kid")]
    key_id: IssuerKeyId,
    #[serde(rename = "kty")]
    _key_type: IssuerKeyType,
    #[serde(rename = "crv")]
    _curve: IssuerKeyCurve,
    #[serde(rename = "x")]
    public_key_base64: String,
    #[serde(rename = "alg")]
    _algorithm: Option<IssuerKeyAlgorithm>,
    #[serde(rename = "use")]
    _key_use: Option<IssuerKeyUse>,
}

#[derive(Deserialize)]
enum IssuerKeyType {
    #[serde(rename = "OKP")]
    OctetKeyPair,
}

#[derive(Deserialize)]
enum IssuerKeyCurve {
    Ed25519,
}

#[derive(Deserialize)]
enum IssuerKeyAlgorithm {
    #[serde(rename = "EdDSA")]
    EdDsa,
}

#[derive(Deserialize)]
enum IssuerKeyUse {
    #[serde(rename = "sig")]
    Signature,
}

impl IssuerKeySet {
    fn decode(bytes: &[u8]) -> Result<Self, IssuerKeysUnavailable> {
        let published: PublishedIssuerKeys =
            serde_json::from_slice(bytes).map_err(|_| IssuerKeysUnavailable::MalformedKeySet)?;
        let mut identities = BTreeSet::new();
        let mut keys = Vec::with_capacity(published.keys.len());
        for published in published.keys {
            if !identities.insert(published.key_id.clone()) {
                return Err(IssuerKeysUnavailable::MalformedKeySet);
            }
            let public_key = URL_SAFE_NO_PAD
                .decode(published.public_key_base64)
                .map_err(|_| IssuerKeysUnavailable::MalformedKeySet)?
                .try_into()
                .map_err(|_| IssuerKeysUnavailable::MalformedKeySet)?;
            keys.push(IssuerVerificationKey {
                key_id: published.key_id,
                public_key,
            });
        }
        Ok(Self { keys })
    }

    pub(crate) fn key(&self, key_id: &IssuerKeyId) -> Option<&IssuerVerificationKey> {
        self.keys.iter().find(|key| &key.key_id == key_id)
    }
}

#[derive(Default)]
pub struct IssuerKeyCache {
    cached: Mutex<IssuerCacheState>,
    #[cfg(feature = "test-support")]
    unavailable: Mutex<Option<IssuerCacheOperation>>,
}

#[cfg(feature = "test-support")]
#[derive(Clone, Copy, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IssuerCacheOperation {
    Read,
    Remember,
}

#[derive(Clone)]
struct CachedIssuerKeys {
    source: String,
    keys: IssuerKeySet,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Default)]
enum IssuerCacheState {
    #[default]
    Empty,
    Retained(CachedIssuerKeys),
}

#[derive(Clone)]
struct RememberSet(CachedIssuerKeys);

impl StateTag for IssuerCacheState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Retained(_) => "retained",
        }
    }
    fn is_terminal(&self) -> bool {
        false
    }
}

impl EventTag for RememberSet {
    fn event_tag(&self) -> &'static str {
        "rememberSet"
    }
}

enum CacheRefusal {}
impl RefusalTag for CacheRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

struct IssuerCacheMachine;
impl StateMachine for IssuerCacheMachine {
    type State = IssuerCacheState;
    type Event = RememberSet;
    type Effect = ();
    type Error = CacheRefusal;

    fn transition(state: &IssuerCacheState, event: RememberSet) -> StateMachineResult<Self> {
        match state {
            IssuerCacheState::Empty | IssuerCacheState::Retained(_) => {
                Ok((IssuerCacheState::Retained(event.0), vec![]))
            }
        }
    }
    fn transitions() -> &'static [Transition] {
        &[
            Transition {
                from: "empty",
                event: "rememberSet",
                to: "retained",
                terminal: false,
            },
            Transition {
                from: "retained",
                event: "rememberSet",
                to: "retained",
                terminal: false,
            },
        ]
    }
}

impl IssuerKeyCache {
    #[cfg(feature = "test-support")]
    pub fn fail_next(&self, operation: IssuerCacheOperation) {
        *self
            .unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(operation);
    }

    #[cfg(feature = "test-support")]
    fn take_unavailable(
        &self,
        operation: IssuerCacheOperation,
    ) -> Result<(), IssuerKeysUnavailable> {
        let mut fault = self
            .unavailable
            .lock()
            .map_err(|_| IssuerKeysUnavailable::CacheUnavailable)?;
        if *fault == Some(operation) {
            *fault = None;
            Err(IssuerKeysUnavailable::CacheUnavailable)
        } else {
            Ok(())
        }
    }

    pub(crate) fn fresh(
        &self,
        configured_source: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<IssuerKeySet>, IssuerKeysUnavailable> {
        #[cfg(feature = "test-support")]
        self.take_unavailable(IssuerCacheOperation::Read)?;
        let cached = self
            .cached
            .lock()
            .map_err(|_| IssuerKeysUnavailable::CacheUnavailable)?;
        match &*cached {
            IssuerCacheState::Retained(entry)
                if entry.source == configured_source && now < entry.expires_at =>
            {
                Ok(Some(entry.keys.clone()))
            }
            IssuerCacheState::Empty | IssuerCacheState::Retained(_) => Ok(None),
        }
    }

    pub(crate) fn remember(
        &self,
        configured_source: String,
        response: RawJwks,
        received_at: DateTime<Utc>,
    ) -> Result<IssuerKeySet, IssuerKeysUnavailable> {
        let keys = IssuerKeySet::decode(&response.body)?;
        let max_age = response
            .cache_control
            .as_deref()
            .and_then(|header| {
                header
                    .split(',')
                    .find_map(|part| part.trim().strip_prefix("max-age=")?.parse::<u64>().ok())
            })
            .unwrap_or(MAXIMUM_KEY_AGE_SECONDS)
            .min(MAXIMUM_KEY_AGE_SECONDS);
        let expires_at = received_at
            .checked_add_signed(chrono::TimeDelta::seconds(
                i64::try_from(max_age).map_err(|_| IssuerKeysUnavailable::CacheUnavailable)?,
            ))
            .ok_or(IssuerKeysUnavailable::CacheUnavailable)?;
        #[cfg(feature = "test-support")]
        self.take_unavailable(IssuerCacheOperation::Remember)?;
        let mut cached = self
            .cached
            .lock()
            .map_err(|_| IssuerKeysUnavailable::CacheUnavailable)?;
        let (replacement, _) = IssuerCacheMachine::transition(
            &cached,
            RememberSet(CachedIssuerKeys {
                source: configured_source,
                keys: keys.clone(),
                expires_at,
            }),
        )
        .map_err(|_| IssuerKeysUnavailable::CacheUnavailable)?;
        *cached = replacement;
        Ok(keys)
    }
}

#[cfg(test)]
#[path = "issuer_key_cache_test.rs"]
mod tests;
