use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, header::HeaderValue};
use router_clock::{ClockInstant, RuntimeClock, timeout_at};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    FleetCredentialError, FleetTarget, PersonLinkError, RefreshUncertainty, TokenExchangeError,
    clock_uuid,
    dpop_proof::{DpopKey, ProofRequest},
};

/// The Rust Central login item. Saving must atomically match the expected item or its absence.
pub trait FleetCredentialStore: Send + Sync + 'static {
    fn load(&self) -> Result<Option<Zeroizing<Vec<u8>>>, FleetCredentialError>;
    fn compare_and_save(
        &self,
        expected: Option<&[u8]>,
        replacement: &[u8],
    ) -> Result<(), FleetCredentialError>;
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "camelCase", deny_unknown_fields)]
enum StoredFleetCredential {
    Ready {
        private_key: Zeroizing<String>,
        refresh_token: Zeroizing<String>,
    },
    LoginRequired {
        refresh_attempt: uuid::Uuid,
    },
}

#[derive(Deserialize)]
pub(crate) struct TokenResponse {
    pub(crate) access_token: Zeroizing<String>,
    pub(crate) refresh_token: Zeroizing<String>,
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

pub(crate) enum TokenGrant<'grant> {
    Refresh(&'grant str),
    AuthorizationCode {
        code: &'grant str,
        verifier: &'grant str,
        redirect: &'grant str,
    },
}

pub(crate) async fn load_item(
    store: Arc<dyn FleetCredentialStore>,
) -> Result<Option<Zeroizing<Vec<u8>>>, FleetCredentialError> {
    tokio::task::spawn_blocking(move || store.load())
        .await
        .map_err(|_| FleetCredentialError::Read)?
}

async fn save_item(
    store: Arc<dyn FleetCredentialStore>,
    expected: Option<Zeroizing<Vec<u8>>>,
    replacement: Zeroizing<Vec<u8>>,
) -> Result<(), FleetCredentialError> {
    tokio::task::spawn_blocking(move || {
        store.compare_and_save(
            expected.as_ref().map(|bytes| bytes.as_slice()),
            &replacement,
        )
    })
    .await
    .map_err(|_| FleetCredentialError::Save)?
}

pub(crate) async fn save_login(
    store: Arc<dyn FleetCredentialStore>,
    expected: Option<Zeroizing<Vec<u8>>>,
    private_key: Zeroizing<String>,
    refresh_token: Zeroizing<String>,
) -> Result<(), FleetCredentialError> {
    let credential = StoredFleetCredential::Ready {
        private_key,
        refresh_token,
    };
    save_item(store, expected, encode_item(&credential)?).await
}

pub(crate) async fn refresh_for_upgrade(
    http: &Client,
    target: &FleetTarget,
    store: Arc<dyn FleetCredentialStore>,
    clock: &dyn RuntimeClock,
    deadline: ClockInstant,
) -> Result<UpgradeCredential, PersonLinkError> {
    let loaded = load_item(store.clone())
        .await?
        .ok_or(FleetCredentialError::Missing)?;
    let credential = serde_json::from_slice(&loaded)
        .map_err(|_| FleetCredentialError::InvalidStoredCredential)?;
    let StoredFleetCredential::Ready {
        private_key,
        refresh_token,
    } = credential
    else {
        return Err(FleetCredentialError::SignInAgain(RefreshUncertainty::NoSavedSuccessor).into());
    };
    if refresh_token.is_empty() {
        return Err(FleetCredentialError::InvalidStoredCredential.into());
    }
    let key = DpopKey::import(&private_key)?;
    let proof = key.proof(ProofRequest::TokenEndpoint, &target.token_url(), clock)?;
    let thumbprint = key.thumbprint()?;
    if clock.monotonic_now() >= deadline {
        return Err(PersonLinkError::Deadline);
    }

    // Only the process that installs this marker may send the predecessor or save its successor.
    let marker = encode_item(&StoredFleetCredential::LoginRequired {
        refresh_attempt: clock_uuid(clock.now().unix_millis()),
    })?;
    save_item(store.clone(), Some(loaded), marker.clone()).await?;

    let tokens = timeout_at(
        clock,
        deadline,
        exchange_token(
            http,
            target,
            TokenGrant::Refresh(&refresh_token),
            proof,
            &thumbprint,
        ),
    )
    .await
    .map_err(|_| FleetCredentialError::SignInAgain(RefreshUncertainty::Deadline))?
    .map_err(|error| FleetCredentialError::SignInAgain(RefreshUncertainty::Exchange(error)))?;

    save_login(store, Some(marker), private_key, tokens.refresh_token)
        .await
        .map_err(|error| match error {
            FleetCredentialError::Changed => error,
            _ => FleetCredentialError::SignInAgain(RefreshUncertainty::Save),
        })?;

    let proof = key.proof(
        ProofRequest::PersonUpgrade {
            access_token: &tokens.access_token,
        },
        &target.person_link_url(),
        clock,
    )?;
    let authorization = Zeroizing::new(format!("DPoP {}", tokens.access_token.as_str()));
    let mut authorization = HeaderValue::from_str(&authorization)
        .map_err(|_| TokenExchangeError::InvalidResponse)
        .map_err(FleetCredentialError::from)?;
    authorization.set_sensitive(true);
    Ok(UpgradeCredential {
        authorization,
        proof,
    })
}

fn encode_item(
    credential: &StoredFleetCredential,
) -> Result<Zeroizing<Vec<u8>>, FleetCredentialError> {
    serde_json::to_vec(credential)
        .map(Zeroizing::new)
        .map_err(|_| FleetCredentialError::Save)
}

pub(crate) async fn exchange_token(
    http: &Client,
    target: &FleetTarget,
    grant: TokenGrant<'_>,
    proof: HeaderValue,
    thumbprint: &str,
) -> Result<TokenResponse, TokenExchangeError> {
    let mut form = vec![
        ("client_id", target.client_id()),
        ("resource", target.admin_audience()),
    ];
    match grant {
        TokenGrant::Refresh(refresh_token) => form.extend([
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ]),
        TokenGrant::AuthorizationCode {
            code,
            verifier,
            redirect,
        } => form.extend([
            ("grant_type", "authorization_code"),
            ("code", code),
            ("code_verifier", verifier),
            ("redirect_uri", redirect),
        ]),
    }
    let mut response = http
        .post(target.token_url())
        .header("dpop", proof)
        .form(&form)
        .send()
        .await
        .map_err(|_| TokenExchangeError::Transport)?;
    if !response.status().is_success() {
        return Err(TokenExchangeError::Refused(response.status()));
    }
    let invalid = TokenExchangeError::InvalidResponse;
    let mut body = Zeroizing::new(Vec::new());
    while let Some(chunk) = response.chunk().await.map_err(|_| invalid)? {
        if body.len().saturating_add(chunk.len()) > 64 * 1024 {
            return Err(invalid);
        }
        body.extend_from_slice(&chunk);
    }
    let tokens: TokenResponse = serde_json::from_slice(&body).map_err(|_| invalid)?;
    let TokenType::DPoP = tokens.token_type;
    if tokens.access_token.is_empty() || tokens.refresh_token.is_empty() {
        return Err(invalid);
    }
    verify_binding(&tokens.access_token, target, thumbprint)?;
    Ok(tokens)
}

fn verify_binding(
    access_token: &str,
    target: &FleetTarget,
    thumbprint: &str,
) -> Result<(), TokenExchangeError> {
    let mismatch = TokenExchangeError::Binding;
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
    // The issuer's HTTPS response supplies this access token. Sign-in verifies its signature.
    Ok(())
}
