use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Form, Json, Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    serve::Listener,
};
use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkFrame, decode_text,
    person_link_rpc::ListOrganizationWorkspaces,
};
use collaboration_central_conformance::signin_fixtures::{ORGANIZATION, fresh_request_id};
use futures_util::SinkExt;
use reqwest_websocket::{Message as CentralMessage, RequestBuilderExt, WebSocket as CentralSocket};
use router_clock::{ManualClock, RuntimeClock};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{ServerConfig, pki_types::PrivateKeyDer},
    server::TlsStream,
};
use tokio_util::task::TaskTracker;

use super::fixture_authority::{
    ADMIN_AUDIENCE, CLIENT_ID, FixtureCredential, IssuerKey, authority_claims, fleet_claims,
    verify_dpop,
};

pub(super) const PERSON_AUDIENCE: &str = "https://central.example.invalid/person-link";
const TOKEN_PATH: &str = "/api/auth/oauth2/token";
const PERSON_PATH: &str = "/api/fleet/person-link";

pub(super) struct TestTlsListener {
    pub tcp: TcpListener,
    acceptor: TlsAcceptor,
}

impl TestTlsListener {
    pub fn new(tcp: TcpListener) -> (Self, reqwest::Certificate) {
        let identity = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
        let certificate = identity.cert.der().clone();
        let trusted = reqwest::Certificate::from_der(certificate.as_ref()).unwrap();
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate],
                PrivateKeyDer::Pkcs8(identity.signing_key.serialize_der().into()),
            )
            .unwrap();
        (
            Self {
                tcp,
                acceptor: TlsAcceptor::from(Arc::new(config)),
            },
            trusted,
        )
    }
}

impl Listener for TestTlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (tcp, address) = self.tcp.accept().await.unwrap();
            if let Ok(tls) = self.acceptor.accept(tcp).await {
                return (tls, address);
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }
}

#[derive(Clone, Debug)]
pub(super) struct ObservedRead {
    pub socket: usize,
    pub correlation: CorrelationId,
    pub request: ListOrganizationWorkspaces,
}

pub(super) struct SignInEvidence {
    pub refreshes: usize,
    pub upgrades: usize,
    pub closes: Vec<u16>,
    pub reads: Vec<ObservedRead>,
    refresh_expected: String,
    access_issued: Option<String>,
    used_proofs: HashSet<uuid::Uuid>,
}

pub(super) struct TestSignIn {
    pub clock: Arc<ManualClock>,
    pub origin: String,
    pub central: String,
    pub signing: Arc<IssuerKey>,
    pub credential: Arc<FixtureCredential>,
    pub evidence: Mutex<SignInEvidence>,
    pub sockets: TaskTracker,
}

impl TestSignIn {
    pub fn new(
        clock: Arc<ManualClock>,
        origin: String,
        central: String,
        signing: Arc<IssuerKey>,
        credential: Arc<FixtureCredential>,
    ) -> Arc<Self> {
        let refresh_expected = credential.initial_refresh();
        Arc::new(Self {
            clock,
            origin,
            central,
            signing,
            credential,
            evidence: Mutex::new(SignInEvidence {
                refreshes: 0,
                upgrades: 0,
                closes: Vec::new(),
                reads: Vec::new(),
                refresh_expected,
                access_issued: None,
                used_proofs: HashSet::new(),
            }),
            sockets: TaskTracker::new(),
        })
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route(TOKEN_PATH, post(refresh))
            .route(&format!("{PERSON_PATH}/{{organization}}"), get(person_link))
            .with_state(self.clone())
    }

    pub fn rotation_saved(&self) -> bool {
        let evidence = self.evidence.lock().unwrap();
        self.credential.refresh_matches(&evidence.refresh_expected)
    }
}

#[derive(Deserialize)]
struct RefreshForm {
    grant_type: String,
    refresh_token: String,
    client_id: String,
    resource: String,
}

async fn refresh(
    State(signin): State<Arc<TestSignIn>>,
    headers: HeaderMap,
    Form(form): Form<RefreshForm>,
) -> Result<Json<Value>, StatusCode> {
    let now = signin.clock.utc_now().timestamp();
    let invalid = StatusCode::UNAUTHORIZED;
    let proof = verify_dpop(
        header(&headers, "dpop").ok_or(invalid)?,
        &signin.credential,
        "POST",
        &format!("{}{TOKEN_PATH}", signin.origin),
        now,
        None,
    )
    .ok_or(invalid)?;
    let mut evidence = signin.evidence.lock().unwrap();
    if form.grant_type != "refresh_token"
        || form.client_id != CLIENT_ID
        || form.resource != ADMIN_AUDIENCE
        || form.refresh_token != evidence.refresh_expected
        || !evidence.used_proofs.insert(proof)
    {
        return Err(invalid);
    }

    let access = signin.signing.sign(
        "at+jwt",
        &fleet_claims(&signin.origin, now, &signin.credential.jkt),
    );
    evidence.refresh_expected = fresh_request_id(now).unwrap();
    evidence.refreshes += 1;
    evidence.access_issued = Some(access.clone());
    Ok(Json(json!({
        "access_token":access,"token_type":"DPoP","refresh_token":evidence.refresh_expected,
    })))
}

async fn person_link(
    State(signin): State<Arc<TestSignIn>>,
    Path(organization): Path<String>,
    headers: HeaderMap,
    downstream: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let now = signin.clock.utc_now().timestamp();
    let invalid = StatusCode::UNAUTHORIZED;
    let token = header(&headers, "authorization")
        .and_then(|value| value.strip_prefix("DPoP "))
        .ok_or(invalid)?;
    let claims = signin.signing.read_access_token(token).ok_or(invalid)?;
    let proof = verify_dpop(
        header(&headers, "dpop").ok_or(invalid)?,
        &signin.credential,
        "GET",
        &format!("{}{PERSON_PATH}/{organization}", signin.origin),
        now,
        Some(token),
    )
    .ok_or(invalid)?;
    if organization != ORGANIZATION
        || claims.get("token_profile").and_then(Value::as_str) != Some("person-fleet")
        || claims.pointer("/cnf/jkt").and_then(Value::as_str)
            != Some(signin.credential.jkt.as_str())
        || claims
            .get("exp")
            .and_then(Value::as_i64)
            .is_none_or(|expiry| expiry <= now)
        || headers.contains_key("cookie")
        || !signin.rotation_saved()
    {
        return Err(invalid);
    }
    {
        let mut evidence = signin.evidence.lock().unwrap();
        if evidence.access_issued.as_deref() != Some(token) || !evidence.used_proofs.insert(proof) {
            return Err(invalid);
        }
    }

    let mut claims = authority_claims(&signin.origin, PERSON_AUDIENCE, now);
    claims
        .as_object_mut()
        .unwrap()
        .insert("token_profile".to_owned(), json!("person-link"));
    let bearer = signin.signing.sign("at+jwt", &claims);
    let upstream = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!(
            "{}/v1/organizations/{organization}/person-link",
            signin.central
        ))
        .bearer_auth(bearer)
        .upgrade()
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?
        .into_websocket()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let socket = {
        let mut evidence = signin.evidence.lock().unwrap();
        evidence.upgrades += 1;
        evidence.upgrades
    };
    let running = signin.sockets.token();
    Ok(downstream
        .on_upgrade(move |remote| async move {
            let _running = running;
            relay(remote, upstream, &signin, socket).await;
        })
        .into_response())
}

async fn relay(
    mut downstream: WebSocket,
    mut central: CentralSocket,
    signin: &TestSignIn,
    socket: usize,
) {
    use futures_util::StreamExt;

    loop {
        tokio::select! {
            incoming = downstream.recv() => {
                let Some(Ok(Message::Text(text))) = incoming else { return };
                let Ok(LinkFrame::Data(DataFrame::CallCentral { correlation_id, request })) =
                    decode_text(&text, FrameDirection::ToCentral)
                else { return };
                let CentralCall::ListOrganizationWorkspaces(request) = *request else { return };
                assert_eq!(request.target.as_str(), ORGANIZATION);
                signin.evidence.lock().unwrap().reads.push(ObservedRead {
                    socket, correlation: correlation_id, request: request.body,
                });
                if central.send(CentralMessage::Text(text.to_string())).await.is_err() { return }
            }
            incoming = central.next() => {
                match incoming {
                    Some(Ok(CentralMessage::Text(text))) => {
                        if downstream.send(Message::Text(text.into())).await.is_err() { return }
                    }
                    Some(Ok(CentralMessage::Close { code, reason })) => {
                        let code = u16::from(code);
                        signin.evidence.lock().unwrap().closes.push(code);
                        let _ = downstream.send(Message::Close(Some(CloseFrame { code, reason: reason.into() }))).await;
                        return;
                    }
                    _ => return,
                }
            }
        }
    }
}

fn header<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    headers.get(name)?.to_str().ok()
}
