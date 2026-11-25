use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Form, Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use central_person_link_client::{
    FleetCredentialError, FleetCredentialStore, FleetLoginError, FleetTarget, PersonLinkError,
    PersonWorkspaceClient, RefreshUncertainty,
};
use collaboration_central_conformance::signin_fixtures::{ORGANIZATION, fresh_request_id};
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
};
use router_clock::ManualClock;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use url::Url;

use super::{
    fixture_authority::{
        ADMIN_AUDIENCE, CLIENT_ID, FixtureCredential, INITIAL_SECONDS, IssuerKey, decode_segment,
        fleet_claims, sha256_url, verify_dpop_key,
    },
    fixture_signin::{TestSignIn, TestTlsListener},
};

struct TestIssuer {
    origin: String,
    certificate: reqwest::Certificate,
    clock: Arc<ManualClock>,
    listener: Option<TestTlsListener>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl TestIssuer {
    async fn new() -> Self {
        let (listener, certificate) =
            TestTlsListener::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
        let origin = format!("https://{}", listener.tcp.local_addr().unwrap());
        Self {
            origin,
            certificate,
            clock: Arc::new(ManualClock::new(
                chrono::DateTime::from_timestamp(INITIAL_SECONDS, 0).unwrap(),
            )),
            listener: Some(listener),
            task: None,
        }
    }

    fn serve(&mut self, routes: Router) {
        let listener = self.listener.take().unwrap();
        self.task = Some(tokio::spawn(axum::serve(listener, routes).into_future()));
    }

    fn client(&self, store: Arc<FixtureCredential>) -> PersonWorkspaceClient {
        let target = FleetTarget::from_json(
            &serde_json::to_vec(&json!({
                "issuer": self.origin,
                "organizationId": ORGANIZATION,
                "centralAdminAudience": ADMIN_AUDIENCE,
                "ownerFleetCliClientId": CLIENT_ID,
                "ownerCliRedirectUri": "http://127.0.0.1/callback",
            }))
            .unwrap(),
        )
        .unwrap();
        PersonWorkspaceClient::with_trusted_root(
            target,
            store,
            self.clock.clone(),
            self.certificate.clone(),
        )
        .unwrap()
    }

    fn refresh_routes(&mut self, credential: Arc<FixtureCredential>) -> Arc<TestSignIn> {
        // These cases stop before a workspace upgrade.
        let signin = TestSignIn::new(
            self.clock.clone(),
            self.origin.clone(),
            "http://127.0.0.1:1".to_owned(),
            Arc::new(IssuerKey::generate()),
            credential,
        );
        self.serve(signin.router());
        signin
    }
}

impl Drop for TestIssuer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct LoginIssuer {
    origin: String,
    signing: IssuerKey,
    code: String,
    refresh: String,
    exchange_attempts: AtomicUsize,
    authorization: Mutex<Option<Authorization>>,
    proof_key: Mutex<Option<Vec<u8>>>,
}

struct Authorization {
    challenge: String,
    redirect: Url,
}

#[derive(Deserialize)]
struct CodeForm {
    grant_type: String,
    code: String,
    client_id: String,
    resource: String,
    redirect_uri: String,
    code_verifier: String,
}

impl LoginIssuer {
    fn start(issuer: &mut TestIssuer) -> Arc<Self> {
        let login = Arc::new(Self {
            origin: issuer.origin.clone(),
            signing: IssuerKey::generate(),
            code: fresh_request_id(INITIAL_SECONDS).unwrap(),
            refresh: fresh_request_id(INITIAL_SECONDS).unwrap(),
            exchange_attempts: AtomicUsize::new(0),
            authorization: Mutex::new(None),
            proof_key: Mutex::new(None),
        });
        issuer.serve(
            Router::new()
                .route("/api/auth/oauth2/token", post(exchange_code))
                .with_state(login.clone()),
        );
        login
    }

    fn callback(&self, browser: &Url, issuer: &str) -> Url {
        assert_eq!(browser.path(), "/sign-in");
        let authorize = browser
            .query_pairs()
            .find(|(name, _)| name == "authorize")
            .unwrap()
            .1;
        let authorize = Url::parse(&authorize).unwrap();
        assert_eq!(authorize.path(), "/api/auth/oauth2/authorize");
        assert_eq!(authorize.origin(), browser.origin());
        let parameters: HashMap<_, _> = authorize.query_pairs().into_owned().collect();
        let parameter = |name: &str| parameters.get(name).map(String::as_str).unwrap();
        assert_eq!(parameter("client_id"), CLIENT_ID);
        assert_eq!(parameter("response_type"), "code");
        assert_eq!(parameter("resource"), ADMIN_AUDIENCE);
        assert_eq!(parameter("scope"), "fleet:admin offline_access");
        assert_eq!(parameter("code_challenge_method"), "S256");
        assert_eq!(parameter("state").len(), 43);
        let redirect = Url::parse(parameter("redirect_uri")).unwrap();
        assert_eq!(redirect.host_str(), Some("127.0.0.1"));
        assert!(redirect.port().is_some());
        assert_eq!(redirect.path(), "/callback");
        *self.authorization.lock().unwrap() = Some(Authorization {
            challenge: parameter("code_challenge").to_owned(),
            redirect: redirect.clone(),
        });
        let mut redirect = redirect;
        redirect.query_pairs_mut().extend_pairs([
            ("state", parameter("state")),
            ("code", self.code.as_str()),
            ("iss", issuer),
        ]);
        redirect
    }
}

async fn exchange_code(
    State(issuer): State<Arc<LoginIssuer>>,
    headers: HeaderMap,
    Form(form): Form<CodeForm>,
) -> Result<Json<Value>, StatusCode> {
    issuer.exchange_attempts.fetch_add(1, Ordering::SeqCst);
    let refused = StatusCode::BAD_REQUEST;
    let authorization = issuer.authorization.lock().unwrap().take().ok_or(refused)?;
    if form.code != issuer.code
        || form.grant_type != "authorization_code"
        || form.client_id != CLIENT_ID
        || form.resource != ADMIN_AUDIENCE
        || form.redirect_uri != authorization.redirect.as_str()
        || sha256_url(form.code_verifier.as_bytes()) != authorization.challenge
    {
        return Err(refused);
    }
    let proof = headers
        .get("dpop")
        .and_then(|header| header.to_str().ok())
        .ok_or(refused)?;
    let header = proof
        .split('.')
        .next()
        .and_then(decode_segment)
        .ok_or(refused)?;
    let jwk = header.get("jwk").ok_or(refused)?;
    let mut public = vec![4];
    for coordinate in ["x", "y"] {
        let bytes = URL_SAFE_NO_PAD
            .decode(jwk[coordinate].as_str().ok_or(refused)?)
            .map_err(|_| refused)?;
        if bytes.len() != 32 {
            return Err(refused);
        }
        public.extend(bytes);
    }
    let jkt = sha256_url(&serde_json::to_vec(jwk).unwrap());
    verify_dpop_key(
        proof,
        &public,
        &jkt,
        "POST",
        &format!("{}/api/auth/oauth2/token", issuer.origin),
        INITIAL_SECONDS,
        None,
    )
    .ok_or(refused)?;
    *issuer.proof_key.lock().unwrap() = Some(public);
    let token = issuer.signing.sign(
        "at+jwt",
        &fleet_claims(&issuer.origin, INITIAL_SECONDS, &jkt),
    );
    Ok(Json(
        json!({ "access_token": token, "token_type": "DPoP", "refresh_token": issuer.refresh }),
    ))
}

#[tokio::test]
async fn login_rejects_wrong_state_then_saves_the_s256_and_dpop_bound_grant() {
    let mut issuer = TestIssuer::new().await;
    let login = LoginIssuer::start(&mut issuer);
    let store = FixtureCredential::empty();
    let mut client = issuer.client(store.clone());
    let (show, shown) = oneshot::channel();
    let browser = async {
        let address: Url = shown.await.unwrap();
        let callback = login.callback(&address, &issuer.origin);
        let mut wrong_state = callback.clone();
        wrong_state.query_pairs_mut().clear().extend_pairs([
            ("state", "incorrect"),
            ("code", login.code.as_str()),
            ("iss", issuer.origin.as_str()),
        ]);
        let browser = reqwest::Client::new();
        let wrong = browser.get(wrong_state).send().await.unwrap();
        assert_eq!(wrong.status(), StatusCode::BAD_REQUEST);
        assert_eq!(login.exchange_attempts.load(Ordering::SeqCst), 0);
        let response = browser.get(callback).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    };

    let (result, ()) = tokio::join!(
        client.login(|address| {
            show.send(address.clone())
                .map_err(|_| io::ErrorKind::BrokenPipe.into())
        }),
        browser
    );

    assert_eq!(result, Ok(()));
    assert_eq!(login.exchange_attempts.load(Ordering::SeqCst), 1);
    assert!(store.refresh_matches(&login.refresh));
    let document = store.document();
    let pkcs8 = URL_SAFE_NO_PAD
        .decode(document["private_key"].as_str().unwrap())
        .unwrap();
    let key = EcdsaKeyPair::from_pkcs8(
        &ECDSA_P256_SHA256_FIXED_SIGNING,
        &pkcs8,
        &SystemRandom::new(),
    )
    .unwrap();
    assert!(login.proof_key.lock().unwrap().as_deref() == Some(key.public_key().as_ref()));
}

#[tokio::test]
async fn login_refuses_the_right_state_from_the_wrong_issuer_before_exchange() {
    let mut issuer = TestIssuer::new().await;
    let login = LoginIssuer::start(&mut issuer);
    let store = FixtureCredential::empty();
    let mut client = issuer.client(store.clone());
    let (show, shown) = oneshot::channel();
    let browser = async {
        let address: Url = shown.await.unwrap();
        let callback = login.callback(&address, "https://wrong.example.invalid");
        let response = reqwest::Client::new().get(callback).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    };

    let (result, ()) = tokio::join!(
        client.login(|address| {
            show.send(address.clone())
                .map_err(|_| io::ErrorKind::BrokenPipe.into())
        }),
        browser
    );

    assert_eq!(result, Err(FleetLoginError::Refused));
    assert_eq!(login.exchange_attempts.load(Ordering::SeqCst), 0);
    assert!(store.load().unwrap().is_none());
}

#[tokio::test]
async fn login_waiting_for_the_browser_cannot_overwrite_a_newly_saved_item() {
    let mut issuer = TestIssuer::new().await;
    let login = LoginIssuer::start(&mut issuer);
    let store = FixtureCredential::empty();
    let newer = FixtureCredential::new().load().unwrap().unwrap();
    let mut client = issuer.client(store.clone());
    let (show, shown) = oneshot::channel();
    let browser = async {
        let address: Url = shown.await.unwrap();
        store.compare_and_save(None, &newer).unwrap();
        let callback = login.callback(&address, &issuer.origin);
        let response = reqwest::Client::new().get(callback).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    };

    let (result, ()) = tokio::join!(
        client.login(|address| {
            show.send(address.clone())
                .map_err(|_| io::ErrorKind::BrokenPipe.into())
        }),
        browser
    );

    assert_eq!(login.exchange_attempts.load(Ordering::SeqCst), 1);
    assert!(store.load().unwrap().unwrap().as_slice() == newer.as_slice());
    assert_eq!(
        result,
        Err(FleetLoginError::Credential(FleetCredentialError::Changed))
    );
}

#[tokio::test]
async fn deadline_after_issuer_rotation_cannot_replay_the_predecessor_after_reopen() {
    let mut issuer = TestIssuer::new().await;
    let store = FixtureCredential::new();
    let signin = issuer.refresh_routes(store.clone());
    let gate = signin.hold_next_refresh();
    let mut client = issuer.client(store.clone());

    let (result, ()) = tokio::join!(client.list_workspaces(), async {
        gate.committed.notified().await;
        let item = store.document();
        assert_eq!(item["state"], "loginRequired");
        assert!(item.get("refresh_token").is_none());
        issuer.clock.advance(Duration::from_secs(31)).unwrap();
    });
    gate.release.notify_one();

    assert_eq!(
        result.unwrap_err(),
        sign_in_again(RefreshUncertainty::Deadline)
    );
    let mut reopened = issuer.client(store);
    assert_eq!(
        reopened.list_workspaces().await.unwrap_err(),
        no_saved_successor()
    );
    assert_eq!(signin.evidence.lock().unwrap().refresh_attempts, 1);
}

#[tokio::test]
async fn refused_successor_save_preserves_the_login_required_marker() {
    let mut issuer = TestIssuer::new().await;
    let store = FixtureCredential::new();
    store.refuse_successor.store(true, Ordering::SeqCst);
    let signin = issuer.refresh_routes(store.clone());
    let mut client = issuer.client(store.clone());

    let result = client.list_workspaces().await;

    assert_eq!(result.unwrap_err(), sign_in_again(RefreshUncertainty::Save));
    let mut reopened = issuer.client(store);
    assert_eq!(
        reopened.list_workspaces().await.unwrap_err(),
        no_saved_successor()
    );
    assert_eq!(signin.evidence.lock().unwrap().refresh_attempts, 1);
}

#[tokio::test]
async fn stale_refresh_successor_cannot_replace_a_newer_login() {
    let mut issuer = TestIssuer::new().await;
    let store = FixtureCredential::new();
    let signin = issuer.refresh_routes(store.clone());
    let gate = signin.hold_next_refresh();
    let newer = FixtureCredential::new().load().unwrap().unwrap();
    let mut client = issuer.client(store.clone());

    let (result, ()) = tokio::join!(client.list_workspaces(), async {
        gate.committed.notified().await;
        let loaded = store.load().unwrap().unwrap();
        store.compare_and_save(Some(&loaded), &newer).unwrap();
        gate.release.notify_one();
    });

    assert!(store.load().unwrap().unwrap().as_slice() == newer.as_slice());
    assert_eq!(
        result.unwrap_err(),
        PersonLinkError::Credential(FleetCredentialError::Changed)
    );
    assert_eq!(signin.evidence.lock().unwrap().refresh_attempts, 1);
}

fn sign_in_again(reason: RefreshUncertainty) -> PersonLinkError {
    PersonLinkError::Credential(FleetCredentialError::SignInAgain(reason))
}

fn no_saved_successor() -> PersonLinkError {
    sign_in_again(RefreshUncertainty::NoSavedSuccessor)
}
