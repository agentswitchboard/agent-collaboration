use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::header::HeaderValue;
use ring::{
    digest::{SHA256, digest},
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair},
};
use router_clock::RuntimeClock;
use serde::{Deserialize, Serialize};
use url::Url;
use zeroize::{ZeroizeOnDrop, Zeroizing};

use crate::{FleetCredentialError, clock_uuid};

#[derive(Deserialize, Serialize, ZeroizeOnDrop)]
pub(crate) struct PrivateJwk {
    kty: String,
    crv: String,
    x: String,
    y: String,
    d: String,
}

#[derive(Serialize)]
struct PublicJwk<'key> {
    // This order is part of RFC 7638 thumbprinting.
    crv: &'key str,
    kty: &'key str,
    x: &'key str,
    y: &'key str,
}

pub(crate) struct DpopKey<'key> {
    public: PublicJwk<'key>,
    signing: EcdsaKeyPair,
}

pub(crate) enum ProofRequest<'request> {
    TokenEndpoint,
    PersonUpgrade { access_token: &'request str },
}

impl PrivateJwk {
    pub(crate) fn import(&self) -> Result<DpopKey<'_>, FleetCredentialError> {
        let invalid = FleetCredentialError::InvalidStoredCredential;
        let decode_coordinate = |encoded: &str| -> Result<[u8; 32], FleetCredentialError> {
            URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| invalid)?
                .try_into()
                .map_err(|_| invalid)
        };
        if self.kty != "EC" || self.crv != "P-256" {
            return Err(invalid);
        }
        let mut public = Vec::with_capacity(65);
        public.push(4);
        public.extend_from_slice(&decode_coordinate(&self.x)?);
        public.extend_from_slice(&decode_coordinate(&self.y)?);
        let private = Zeroizing::new(URL_SAFE_NO_PAD.decode(&self.d).map_err(|_| invalid)?);
        let signing = EcdsaKeyPair::from_private_key_and_public_key(
            &ECDSA_P256_SHA256_FIXED_SIGNING,
            &private,
            &public,
            &SystemRandom::new(),
        )
        .map_err(|_| invalid)?;
        Ok(DpopKey {
            public: PublicJwk {
                crv: &self.crv,
                kty: &self.kty,
                x: &self.x,
                y: &self.y,
            },
            signing,
        })
    }
}

impl DpopKey<'_> {
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
            jwk: &'key PublicJwk<'key>,
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

fn sha256_base64url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, bytes))
}

fn encode_segment(value: &impl Serialize) -> Result<String, FleetCredentialError> {
    let bytes = serde_json::to_vec(value).map_err(|_| FleetCredentialError::Proof)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
