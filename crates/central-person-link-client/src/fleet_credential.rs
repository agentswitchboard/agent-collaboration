use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, header::HeaderValue};
use router_clock::{ClockInstant, RuntimeClock, timeout_at};
use serde::{Deserialize, Serialize};
use zeroize::{ZeroizeOnDrop, Zeroizing};

use crate::{
    FleetCredentialError, FleetTarget, PersonLinkError,
    dpop_proof::{PrivateJwk, ProofRequest},
};

/// The existing Fleet credential item, encoded exactly as written by the Node owner CLI.
pub trait FleetCredentialStore: Send + Sync + 'static {
    fn load(&self) -> Result<Zeroizing<Vec<u8>>, FleetCredentialError>;
    fn replace(&self, credential: &[u8]) -> Result<(), FleetCredentialError>;
}

#[derive(Deserialize, Serialize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredFleetCredential {
    private_jwk: PrivateJwk,
    refresh_token: String,
}

#[derive(Deserialize, ZeroizeOnDrop)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    #[zeroize(skip)]
    token_type: TokenType,
}

#[derive(Deserialize)]
enum TokenType {
    DPoP,
}

#[derive(Deserialize)]
struct TokenBinding {
    aud: String,
    client_id: String,
    token_profile: FleetTokenProfile,
    cnf: KeyConfirmation,
}

#[derive(Deserialize)]
enum FleetTokenProfile {
    #[serde(rename = "person-fleet")]
    PersonFleet,
}

#[derive(Deserialize)]
struct KeyConfirmation {
    jkt: String,
}

pub(crate) struct UpgradeCredential {
    pub authorization: HeaderValue,
    pub proof: HeaderValue,
}

pub(crate) async fn refresh_for_upgrade(
    http: &Client,
    target: &FleetTarget,
    store: Arc<dyn FleetCredentialStore>,
    clock: &dyn RuntimeClock,
    deadline: ClockInstant,
) -> Result<UpgradeCredential, PersonLinkError> {
    let reader = store.clone();
    let mut stored = tokio::task::spawn_blocking(move || read_credential(reader.as_ref()))
        .await
        .map_err(|_| FleetCredentialError::Read)??;
    let token_url = target.token_url();
    let key = stored.private_jwk.import()?;
    let proof = key.proof(ProofRequest::TokenEndpoint, &token_url, clock)?;
    let mut tokens = timeout_at(
        clock,
        deadline,
        exchange_refresh(http, target, token_url, &stored.refresh_token, proof),
    )
    .await
    .map_err(|_| PersonLinkError::Deadline)??;
    verify_binding(&tokens.access_token, target, &key.thumbprint()?)?;

    // Persist the rotation before attempting the upgrade, even if the socket will be refused.
    let access_token = Zeroizing::new(std::mem::take(&mut tokens.access_token));
    std::mem::swap(&mut stored.refresh_token, &mut tokens.refresh_token);
    let stored = tokio::task::spawn_blocking(move || {
        let bytes =
            Zeroizing::new(serde_json::to_vec(&stored).map_err(|_| FleetCredentialError::Save)?);
        let encoded = Zeroizing::new(URL_SAFE_NO_PAD.encode(bytes.as_slice()));
        store.replace(encoded.as_bytes())?;
        Ok::<_, FleetCredentialError>(stored)
    })
    .await
    .map_err(|_| FleetCredentialError::Save)??;

    let proof = stored.private_jwk.import()?.proof(
        ProofRequest::PersonUpgrade {
            access_token: &access_token,
        },
        &target.person_link_url(),
        clock,
    )?;
    let authorization = Zeroizing::new(format!("DPoP {}", access_token.as_str()));
    let mut authorization = HeaderValue::from_str(&authorization)
        .map_err(|_| FleetCredentialError::InvalidTokenResponse)?;
    authorization.set_sensitive(true);
    Ok(UpgradeCredential {
        authorization,
        proof,
    })
}

fn read_credential(
    store: &dyn FleetCredentialStore,
) -> Result<StoredFleetCredential, FleetCredentialError> {
    let encoded = store.load()?;
    let invalid = FleetCredentialError::InvalidStoredCredential;
    let bytes = Zeroizing::new(
        URL_SAFE_NO_PAD
            .decode(encoded.as_slice())
            .map_err(|_| invalid)?,
    );
    let stored: StoredFleetCredential = serde_json::from_slice(&bytes).map_err(|_| invalid)?;
    if stored.refresh_token.is_empty() {
        return Err(invalid);
    }
    Ok(stored)
}

async fn exchange_refresh(
    http: &Client,
    target: &FleetTarget,
    token_url: url::Url,
    refresh_token: &str,
    proof: HeaderValue,
) -> Result<TokenResponse, FleetCredentialError> {
    let mut response = http
        .post(token_url)
        .header("dpop", proof)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", target.client_id()),
            ("resource", target.admin_audience()),
        ])
        .send()
        .await
        .map_err(|_| FleetCredentialError::TokenEndpoint)?;
    if !response.status().is_success() {
        return Err(FleetCredentialError::RefreshRefused(response.status()));
    }
    let invalid = FleetCredentialError::InvalidTokenResponse;
    let mut body = Zeroizing::new(Vec::new());
    while let Some(chunk) = response.chunk().await.map_err(|_| invalid)? {
        if body.len().saturating_add(chunk.len()) > 64 * 1024 {
            return Err(invalid);
        }
        body.extend_from_slice(&chunk);
    }
    let tokens: TokenResponse = serde_json::from_slice(&body).map_err(|_| invalid)?;
    if tokens.access_token.is_empty() || tokens.refresh_token.is_empty() {
        return Err(invalid);
    }
    Ok(tokens)
}

fn verify_binding(
    access_token: &str,
    target: &FleetTarget,
    thumbprint: &str,
) -> Result<(), FleetCredentialError> {
    let mismatch = FleetCredentialError::TokenBinding;
    let mut parts = access_token.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(mismatch);
    };
    if header.is_empty() || signature.is_empty() {
        return Err(mismatch);
    }
    let payload = Zeroizing::new(URL_SAFE_NO_PAD.decode(payload).map_err(|_| mismatch)?);
    let binding: TokenBinding = serde_json::from_slice(&payload).map_err(|_| mismatch)?;
    if binding.aud != target.admin_audience()
        || binding.client_id != target.client_id()
        || binding.cnf.jkt != thumbprint
        || !matches!(binding.token_profile, FleetTokenProfile::PersonFleet)
    {
        return Err(mismatch);
    }
    // This checks the selection and binding of the issuer's response. Sign-in verifies the JWT.
    Ok(())
}
