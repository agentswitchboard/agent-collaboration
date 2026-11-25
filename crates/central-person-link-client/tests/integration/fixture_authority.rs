use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use central_person_link_client::{FleetCredentialError, FleetCredentialStore};
use collaboration_central_conformance::signin_fixtures::{
    ORGANIZATION, OWNER, OWNER_SUBJECT, POLICY, SESSION, fresh_request_id,
};
use ring::{
    digest::{SHA256, digest},
    rand::SystemRandom,
    signature::{
        ECDSA_P256_SHA256_FIXED, ECDSA_P256_SHA256_FIXED_SIGNING, ED25519, EcdsaKeyPair,
        Ed25519KeyPair, KeyPair, UnparsedPublicKey,
    },
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use zeroize::Zeroizing;

pub(super) const INITIAL_SECONDS: i64 = 1_900_000_000;
pub(super) const CLIENT_ID: &str = "fixture-owner-fleet";
pub(super) const ADMIN_AUDIENCE: &str = "https://central.example.invalid/admin";
pub(super) const AUDIENCE: &str = "https://central.example.invalid/identity";
pub(super) const PUBLIC_ORIGIN: &str = "https://central.example.invalid";

pub(super) struct FixtureCredential {
    stored: Mutex<Option<Zeroizing<Vec<u8>>>>,
    pub refuse_successor: AtomicBool,
    pub public_key: Vec<u8>,
    pub jkt: String,
}

impl FixtureCredential {
    pub fn new() -> Arc<Self> {
        let random = SystemRandom::new();
        let pkcs8 =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &random).unwrap();
        let key =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &random)
                .unwrap();
        let public = key.public_key().as_ref();
        let coordinates = json!({
            "crv":"P-256", "kty":"EC", "x":B64.encode(public.get(1..33).unwrap()),
            "y":B64.encode(public.get(33..65).unwrap()),
        });
        let jkt = sha256_url(&serde_json::to_vec(&coordinates).unwrap());
        let stored = json!({
            "state":"ready",
            "private_key":B64.encode(pkcs8.as_ref()),
            "refresh_token":fresh_request_id(INITIAL_SECONDS).unwrap(),
        });
        Arc::new(Self {
            stored: Mutex::new(Some(Zeroizing::new(serde_json::to_vec(&stored).unwrap()))),
            refuse_successor: AtomicBool::new(false),
            public_key: public.to_vec(),
            jkt,
        })
    }

    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            stored: Mutex::new(None),
            refuse_successor: AtomicBool::new(false),
            public_key: Vec::new(),
            jkt: String::new(),
        })
    }

    pub fn refresh_matches(&self, expected: &str) -> bool {
        self.document().get("refresh_token").and_then(Value::as_str) == Some(expected)
    }

    pub fn initial_refresh(&self) -> String {
        self.document()
            .get("refresh_token")
            .and_then(Value::as_str)
            .unwrap()
            .to_owned()
    }

    pub fn document(&self) -> Value {
        let item = self.load().unwrap().unwrap();
        serde_json::from_slice(&item).unwrap()
    }
}

impl FleetCredentialStore for FixtureCredential {
    fn load(&self) -> Result<Option<Zeroizing<Vec<u8>>>, FleetCredentialError> {
        let stored = self.stored.lock().map_err(|_| FleetCredentialError::Read)?;
        Ok(stored.clone())
    }

    fn compare_and_save(
        &self,
        expected: Option<&[u8]>,
        replacement: &[u8],
    ) -> Result<(), FleetCredentialError> {
        let mut saved = self.stored.lock().map_err(|_| FleetCredentialError::Save)?;
        if saved.as_ref().map(|value| value.as_slice()) != expected {
            return Err(FleetCredentialError::Changed);
        }
        let incoming: Value =
            serde_json::from_slice(replacement).map_err(|_| FleetCredentialError::Save)?;
        if incoming.get("state").and_then(Value::as_str) == Some("ready")
            && self.refuse_successor.load(Ordering::SeqCst)
        {
            return Err(FleetCredentialError::Save);
        }
        *saved = Some(Zeroizing::new(replacement.to_vec()));
        Ok(())
    }
}

pub(super) struct IssuerKey(Ed25519KeyPair);

impl IssuerKey {
    pub fn generate() -> Self {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        Self(Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap())
    }

    pub fn jwks(&self) -> Value {
        json!({"keys":[{"kid":"fixture-ed25519","kty":"OKP","crv":"Ed25519","alg":"EdDSA",
             "x":B64.encode(self.0.public_key().as_ref())}]})
    }

    pub fn sign(&self, typ: &str, claims: &Value) -> String {
        let header = json!({"typ":typ,"alg":"EdDSA","kid":"fixture-ed25519"});
        let input = format!("{}.{}", segment(&header), segment(claims));
        format!(
            "{input}.{}",
            B64.encode(self.0.sign(input.as_bytes()).as_ref())
        )
    }

    pub fn read_access_token(&self, token: &str) -> Option<Value> {
        let (input, signature) = token.rsplit_once('.')?;
        UnparsedPublicKey::new(&ED25519, self.0.public_key().as_ref())
            .verify(input.as_bytes(), &B64.decode(signature).ok()?)
            .ok()?;
        let (_, claims) = input.split_once('.')?;
        decode_segment(claims)
    }
}

pub(super) fn authority_claims(issuer: &str, audience: &str, now: i64) -> Value {
    json!({
        "iss":issuer, "aud":audience, "sub":OWNER, "iat":now, "exp":now+60,
        "jti":fresh_request_id(now).unwrap(),
        "organization_id":ORGANIZATION, "authorization_subject_id":OWNER_SUBJECT,
        "person_revision":4, "organization_revision":3, "permission_policy_digest":POLICY,
        "permissions":["organization:view", "workspace:create"],
        "session_expires_at_ms":(INITIAL_SECONDS+3_600)*1000,
        "authorization_snapshot_at_ms":now*1000,
        "authorization_snapshot_valid_until_ms":(now+300)*1000,
        "second_factor_at":INITIAL_SECONDS-60,"second_factor_method":"totp",
    })
}

pub(super) fn fleet_claims(issuer: &str, now: i64, jkt: &str) -> Value {
    let mut claims = authority_claims(issuer, ADMIN_AUDIENCE, now);
    claims.as_object_mut().unwrap().extend([
        ("exp".to_owned(), json!(now + 300)),
        ("token_profile".to_owned(), json!("person-fleet")),
        ("sid".to_owned(), json!(SESSION)),
        ("client_id".to_owned(), json!(CLIENT_ID)),
        ("cnf".to_owned(), json!({"jkt":jkt})),
    ]);
    claims
}

pub(super) fn verify_dpop(
    proof: &str,
    credential: &FixtureCredential,
    method: &str,
    url: &str,
    at: i64,
    access_token: Option<&str>,
) -> Option<uuid::Uuid> {
    verify_dpop_key(
        proof,
        &credential.public_key,
        &credential.jkt,
        method,
        url,
        at,
        access_token,
    )
}

pub(super) fn verify_dpop_key(
    proof: &str,
    public_key: &[u8],
    jkt: &str,
    method: &str,
    url: &str,
    at: i64,
    access_token: Option<&str>,
) -> Option<uuid::Uuid> {
    let (signing_input, signature) = proof.rsplit_once('.')?;
    let (header, claims) = signing_input.split_once('.')?;
    let header = decode_segment(header)?;
    let claims = decode_segment(claims)?;
    if header.get("typ")?.as_str()? != "dpop+jwt"
        || header.get("alg")?.as_str()? != "ES256"
        || sha256_url(&serde_json::to_vec(header.get("jwk")?).ok()?) != jkt
        || claims.get("htm")?.as_str()? != method
        || claims.get("htu")?.as_str()? != url
        || claims.get("iat")?.as_i64()? != at
        || claims.get("intent").is_some()
        || claims.get("ath").and_then(Value::as_str)
            != access_token
                .map(|token| sha256_url(token.as_bytes()))
                .as_deref()
    {
        return None;
    }
    UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, public_key)
        .verify(signing_input.as_bytes(), &B64.decode(signature).ok()?)
        .ok()?;
    let id: uuid::Uuid = claims.get("jti")?.as_str()?.parse().ok()?;
    (id.get_version_num() == 7).then_some(id)
}

pub(super) fn sha256_url(bytes: &[u8]) -> String {
    B64.encode(digest(&SHA256, bytes))
}

fn segment(value: &Value) -> String {
    B64.encode(serde_json::to_vec(value).unwrap())
}

pub(super) fn decode_segment(value: &str) -> Option<Value> {
    serde_json::from_slice(&B64.decode(value).ok()?).ok()
}
