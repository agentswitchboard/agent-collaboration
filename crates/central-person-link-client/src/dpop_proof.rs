use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::header::HeaderValue;
use ring::{
    digest::{SHA256, digest},
    rand::{SecureRandom, SystemRandom},
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
};
use router_clock::RuntimeClock;
use serde::Serialize;
use url::Url;
use zeroize::Zeroizing;

use crate::{FleetCredentialError, clock_uuid};

#[derive(Serialize)]
struct PublicJwk {
    // This order is part of RFC 7638 thumbprinting.
    crv: &'static str,
    kty: &'static str,
    x: String,
    y: String,
}

pub(crate) struct DpopKey {
    public: PublicJwk,
    signing: EcdsaKeyPair,
}

pub(crate) enum ProofRequest<'request> {
    TokenEndpoint,
    PersonUpgrade { access_token: &'request str },
}

impl DpopKey {
    pub(crate) fn generate() -> Result<(Self, Zeroizing<String>), FleetCredentialError> {
        let document =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .map_err(|_| FleetCredentialError::Proof)?;
        let encoded = Zeroizing::new(URL_SAFE_NO_PAD.encode(document.as_ref()));
        Ok((Self::import(&encoded)?, encoded))
    }

    pub(crate) fn import(encoded: &str) -> Result<Self, FleetCredentialError> {
        let invalid = FleetCredentialError::InvalidStoredCredential;
        let document = Zeroizing::new(URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid)?);
        let signing = EcdsaKeyPair::from_pkcs8(
            &ECDSA_P256_SHA256_FIXED_SIGNING,
            &document,
            &SystemRandom::new(),
        )
        .map_err(|_| invalid)?;
        let public = signing.public_key().as_ref();
        let [4, coordinates @ ..] = public else {
            return Err(invalid);
        };
        let (x, y) = coordinates.split_at_checked(32).ok_or(invalid)?;
        if y.len() != 32 {
            return Err(invalid);
        }
        Ok(Self {
            public: PublicJwk {
                crv: "P-256",
                kty: "EC",
                x: URL_SAFE_NO_PAD.encode(x),
                y: URL_SAFE_NO_PAD.encode(y),
            },
            signing,
        })
    }
    pub(crate) fn thumbprint(&self) -> Result<String, FleetCredentialError> {
        let canonical =
            serde_json::to_vec(&self.public).map_err(|_| FleetCredentialError::Proof)?;
        Ok(sha256_base64url(&canonical))
    }

    pub(crate) fn proof(
        &self,
        request: ProofRequest<'_>,
        url: &Url,
        clock: &dyn RuntimeClock,
    ) -> Result<HeaderValue, FleetCredentialError> {
        #[derive(Serialize)]
        struct DpopHeader<'key> {
            typ: &'static str,
            alg: &'static str,
            jwk: &'key PublicJwk,
        }

        #[derive(Serialize)]
        struct DpopClaims<'request> {
            jti: uuid::Uuid,
            htm: &'static str,
            htu: &'request str,
            iat: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            ath: Option<String>,
        }

        let now_ms = clock.now().unix_millis();
        let (method, token_hash) = match request {
            ProofRequest::TokenEndpoint => ("POST", None),
            ProofRequest::PersonUpgrade { access_token } => {
                ("GET", Some(sha256_base64url(access_token.as_bytes())))
            }
        };
        let header = encode_segment(&DpopHeader {
            typ: "dpop+jwt",
            alg: "ES256",
            jwk: &self.public,
        })?;
        let claims = encode_segment(&DpopClaims {
            jti: clock_uuid(now_ms),
            htm: method,
            htu: url.as_str(),
            iat: now_ms.div_euclid(1_000),
            ath: token_hash,
        })?;
        let input = format!("{header}.{claims}");
        let signature = self
            .signing
            .sign(&SystemRandom::new(), input.as_bytes())
            .map_err(|_| FleetCredentialError::Proof)?;
        let mut proof = HeaderValue::from_str(&format!(
            "{input}.{}",
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
        .map_err(|_| FleetCredentialError::Proof)?;
        proof.set_sensitive(true);
        Ok(proof)
    }
}

pub(crate) fn oauth_nonce() -> Result<Zeroizing<String>, FleetCredentialError> {
    let mut bytes = Zeroizing::new([0; 32]);
    SystemRandom::new()
        .fill(bytes.as_mut())
        .map_err(|_| FleetCredentialError::Proof)?;
    Ok(Zeroizing::new(URL_SAFE_NO_PAD.encode(bytes.as_slice())))
}

pub(crate) fn sha256_base64url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, bytes))
}

fn encode_segment(value: &impl Serialize) -> Result<String, FleetCredentialError> {
    let bytes = serde_json::to_vec(value).map_err(|_| FleetCredentialError::Proof)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
