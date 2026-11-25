use std::{
    future::IntoFuture,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    extract::State,
    http::{Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use reqwest::Client;
use router_clock::{RuntimeClock, timeout_at};
use tokio::{net::TcpListener, sync::oneshot};
use url::Url;
use zeroize::Zeroizing;

use crate::{
    FleetCredentialError, FleetCredentialStore, FleetLoginError, FleetTarget, FleetTargetError,
    dpop_proof::{DpopKey, ProofRequest, oauth_nonce, sha256_base64url},
    fleet_credential::{TokenGrant, exchange_token, load_item, save_login},
};

const AUTHORIZATION_BUDGET: Duration = Duration::from_secs(300);
const EXCHANGE_BUDGET: Duration = Duration::from_secs(30);
const CALLBACK_DRAIN_BUDGET: Duration = Duration::from_secs(2);

pub(crate) async fn login(
    http: &Client,
    target: &FleetTarget,
    store: Arc<dyn FleetCredentialStore>,
    clock: &dyn RuntimeClock,
    show_address: impl FnOnce(&Url) -> io::Result<()>,
) -> Result<(), FleetLoginError> {
    let (redirect, address) = target.loopback_redirect()?;
    let expected_item = load_item(store.clone()).await?;
    let (key, private_key) = DpopKey::generate()?;
    let verifier = oauth_nonce()?;
    let state = oauth_nonce()?;
    let listener = TcpListener::bind(address)
        .await
        .map_err(|_| FleetLoginError::Listener)?;
    let port = listener
        .local_addr()
        .map_err(|_| FleetLoginError::Listener)?
        .port();
    let mut redirect = redirect;
    redirect
        .set_port(Some(port))
        .map_err(|()| FleetTargetError::InvalidRedirect)?;

    let browser = browser_address(target, &redirect, &state, &verifier);
    let deadline = clock.monotonic_now().after(AUTHORIZATION_BUDGET);
    show_address(&browser).map_err(|_| FleetLoginError::ShowAddress)?;

    let code = await_callback(listener, &redirect, target.issuer(), state, clock, deadline).await?;
    let proof = key.proof(ProofRequest::TokenEndpoint, &target.token_url(), clock)?;
    let thumbprint = key.thumbprint()?;
    let tokens = timeout_at(
        clock,
        clock.monotonic_now().after(EXCHANGE_BUDGET),
        exchange_token(
            http,
            target,
            TokenGrant::AuthorizationCode {
                code: &code,
                verifier: &verifier,
                redirect: redirect.as_str(),
            },
            proof,
            &thumbprint,
        ),
    )
    .await
    .map_err(|_| FleetLoginError::Deadline)?
    .map_err(FleetCredentialError::from)?;

    save_login(store, expected_item, private_key, tokens.refresh_token).await?;
    Ok(())
}

fn browser_address(target: &FleetTarget, redirect: &Url, state: &str, verifier: &str) -> Url {
    let mut authorization = target.issuer_route("/api/auth/oauth2/authorize");
    authorization.query_pairs_mut().extend_pairs([
        ("client_id", target.client_id()),
        ("response_type", "code"),
        ("redirect_uri", redirect.as_str()),
        ("scope", "fleet:admin offline_access"),
        ("resource", target.admin_audience()),
        ("code_challenge", &sha256_base64url(verifier.as_bytes())),
        ("code_challenge_method", "S256"),
        ("state", state),
    ]);
    let mut browser = target.issuer_route("/sign-in");
    browser
        .query_pairs_mut()
        .append_pair("authorize", authorization.as_str());
    browser
}

type CallbackAnswer = Result<Zeroizing<String>, FleetLoginError>;

struct CallbackListener {
    path: String,
    issuer: String,
    state: Zeroizing<String>,
    answer: Mutex<Option<oneshot::Sender<CallbackAnswer>>>,
}

async fn await_callback(
    listener: TcpListener,
    redirect: &Url,
    issuer: &str,
    state: Zeroizing<String>,
    clock: &dyn RuntimeClock,
    deadline: router_clock::ClockInstant,
) -> CallbackAnswer {
    let (answer, callback) = oneshot::channel();
    let (shutdown, stopped) = oneshot::channel::<()>();
    let app = Router::new()
        .fallback(receive_callback)
        .with_state(Arc::new(CallbackListener {
            path: redirect.path().to_owned(),
            issuer: issuer.to_owned(),
            state,
            answer: Mutex::new(Some(answer)),
        }));
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .into_future();
    tokio::pin!(server);
    let outcome = tokio::select! {
        _ = &mut server => return Err(FleetLoginError::Listener),
        response = timeout_at(clock, deadline, callback) => match response {
            Ok(Ok(answer)) => answer,
            Ok(Err(_)) => Err(FleetLoginError::Listener),
            Err(_) => Err(FleetLoginError::Deadline),
        },
    };
    let _ = shutdown.send(());
    let drain_budget = clock.monotonic_now().after(CALLBACK_DRAIN_BUDGET);
    let _ = timeout_at(clock, drain_budget, server).await;
    outcome
}

async fn receive_callback(
    State(listener): State<Arc<CallbackListener>>,
    method: Method,
    uri: Uri,
) -> Response {
    let valid_target = method == Method::GET && uri.path() == listener.path;
    let query = uri.query().filter(|query| query.len() <= 16 * 1024);
    let Some(query) = query.filter(|_| valid_target) else {
        return callback_response(StatusCode::BAD_REQUEST, "Invalid owner sign-in callback.");
    };
    let mut state = None;
    let mut issuer = None;
    let mut code = None;
    let mut refused = false;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let previous = match name.as_ref() {
            "state" => state.replace(value),
            "iss" => issuer.replace(value),
            "code" => code.replace(value),
            "error" => {
                refused = true;
                continue;
            }
            _ => continue,
        };
        if previous.is_some() {
            return callback_response(StatusCode::BAD_REQUEST, "Invalid owner sign-in callback.");
        }
    }
    if state.as_deref() != Some(listener.state.as_str()) {
        return callback_response(StatusCode::BAD_REQUEST, "Invalid owner sign-in callback.");
    }

    let outcome = match code {
        Some(code)
            if !refused && !code.is_empty() && issuer.as_deref() == Some(&listener.issuer) =>
        {
            Ok(Zeroizing::new(code.into_owned()))
        }
        _ => Err(FleetLoginError::Refused),
    };
    let (status, body) = if outcome.is_ok() {
        (StatusCode::OK, "Owner sign-in complete. Return to the CLI.")
    } else {
        (StatusCode::BAD_REQUEST, "Owner sign-in refused.")
    };
    if let Ok(mut answer) = listener.answer.lock()
        && let Some(sender) = answer.take()
    {
        let _ = sender.send(outcome);
    }
    callback_response(status, body)
}

fn callback_response(status: StatusCode, body: &'static str) -> Response {
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::CONNECTION, "close"),
        ],
        body,
    )
        .into_response()
}
