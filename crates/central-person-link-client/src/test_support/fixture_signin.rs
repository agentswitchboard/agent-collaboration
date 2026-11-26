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
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkFrame, TypedCallResult, decode_text,
    encode_text, person_link_rpc::ListOrganizationWorkspaces,
};
use futures_util::SinkExt;
use reqwest_websocket::{Message as CentralMessage, RequestBuilderExt, WebSocket as CentralSocket};
use router_clock::RuntimeClock;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{ServerConfig, pki_types::PrivateKeyDer},
    server::TlsStream,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub use super::approval_relay_fault::ApprovalRelayFault;
use super::approval_relay_fault::{approval_reply_with_fault, scripted_approval_reply};
use super::fixture_authority::{
    ADMIN_AUDIENCE, CLIENT_ID, FixtureCredential, FixturePerson, IssuerKey, authority_claims,
    fleet_claims, fresh_request_id, verify_dpop,
};

pub const PERSON_AUDIENCE: &str = "https://central.example.invalid/person-link";
const TOKEN_PATH: &str = "/api/auth/oauth2/token";
const PERSON_PATH: &str = "/api/fleet/person-link";

pub struct TestTlsListener {
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
pub struct ObservedRead {
    pub socket: usize,
    pub correlation: CorrelationId,
    pub request: ListOrganizationWorkspaces,
}

pub struct ObservedApprovalCall {
    pub socket: usize,
    pub correlation: CorrelationId,
    pub request: CentralCall,
}

pub struct SignInEvidence {
    pub refresh_attempts: usize,
    pub refreshes: usize,
    pub upgrades: usize,
    pub refused_upgrades: Vec<u16>,
    pub closes: Vec<u16>,
    pub reads: Vec<ObservedRead>,
    pub approval_calls: Vec<ObservedApprovalCall>,
    pub dropped_decisions: usize,
    refresh_expected: String,
    access_issued: Option<String>,
    used_proofs: HashSet<uuid::Uuid>,
}

type ApprovalBearer = Box<dyn Fn(i64, usize) -> String + Send + Sync>;

pub struct TestSignIn {
    clock: Arc<dyn RuntimeClock>,
    origin: String,
    central: String,
    signing: Arc<IssuerKey>,
    credential: Arc<FixtureCredential>,
    pub evidence: Mutex<SignInEvidence>,
    pub sockets: TaskTracker,
    person: FixturePerson,
    approval_bearer: Option<ApprovalBearer>,
    approval_fault: Mutex<Option<ApprovalRelayFault>>,
    refresh_gate: Mutex<Option<Arc<RefreshGate>>>,
}

pub struct ApprovalSignIn {
    pub relay: Arc<TestSignIn>,
    certificate: reqwest::Certificate,
    stop: CancellationToken,
    serving: JoinHandle<io::Result<()>>,
}

impl ApprovalSignIn {
    pub async fn start(
        clock: Arc<dyn RuntimeClock>,
        central_origin: String,
        person: FixturePerson,
        bearer: impl Fn(i64, usize) -> String + Send + Sync + 'static,
    ) -> Self {
        let (listener, certificate) =
            TestTlsListener::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
        let origin = format!("https://{}", listener.tcp.local_addr().unwrap());
        let relay = TestSignIn::for_approvals(
            clock,
            origin,
            central_origin,
            Arc::new(IssuerKey::generate()),
            FixtureCredential::new(),
            person,
            bearer,
        );
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let routes = relay.router();
        let serving = tokio::spawn(async move {
            axum::serve(listener, routes)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        Self {
            relay,
            certificate,
            stop,
            serving,
        }
    }

    pub fn client(&self) -> crate::PersonLinkClient {
        self.relay.client(self.certificate.clone())
    }

    pub async fn close(self) {
        self.stop.cancel();
        self.serving.await.unwrap().unwrap();
        self.relay.sockets.close();
        self.relay.sockets.wait().await;
    }
}

#[derive(Default)]
pub struct RefreshGate {
    pub committed: Notify,
    pub release: Notify,
}

impl TestSignIn {
    pub fn new(
        clock: Arc<dyn RuntimeClock>,
        origin: String,
        central: String,
        signing: Arc<IssuerKey>,
        credential: Arc<FixtureCredential>,
        person: FixturePerson,
    ) -> Arc<Self> {
        Self::with_bearer(clock, origin, central, signing, credential, person, None)
    }

    pub fn for_approvals(
        clock: Arc<dyn RuntimeClock>,
        origin: String,
        central: String,
        signing: Arc<IssuerKey>,
        credential: Arc<FixtureCredential>,
        person: FixturePerson,
        bearer: impl Fn(i64, usize) -> String + Send + Sync + 'static,
    ) -> Arc<Self> {
        Self::with_bearer(
            clock,
            origin,
            central,
            signing,
            credential,
            person,
            Some(Box::new(bearer)),
        )
    }

    fn with_bearer(
        clock: Arc<dyn RuntimeClock>,
        origin: String,
        central: String,
        signing: Arc<IssuerKey>,
        credential: Arc<FixtureCredential>,
        person: FixturePerson,
        approval_bearer: Option<ApprovalBearer>,
    ) -> Arc<Self> {
        let refresh_expected = credential.initial_refresh();
        Arc::new(Self {
            clock,
            origin,
            central,
            signing,
            credential,
            evidence: Mutex::new(SignInEvidence {
                refresh_attempts: 0,
                refreshes: 0,
                upgrades: 0,
                refused_upgrades: Vec::new(),
                closes: Vec::new(),
                reads: Vec::new(),
                approval_calls: Vec::new(),
                dropped_decisions: 0,
                refresh_expected,
                access_issued: None,
                used_proofs: HashSet::new(),
            }),
            sockets: TaskTracker::new(),
            person,
            approval_bearer,
            approval_fault: Mutex::new(None),
            refresh_gate: Mutex::new(None),
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

    pub fn hold_next_refresh(&self) -> Arc<RefreshGate> {
        let gate = Arc::new(RefreshGate::default());
        *self.refresh_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    pub fn script_once(&self, fault: ApprovalRelayFault) {
        assert!(self.approval_fault.lock().unwrap().replace(fault).is_none());
    }

    pub fn client(&self, root: reqwest::Certificate) -> crate::PersonLinkClient {
        let target = crate::FleetTarget::from_json(
            &serde_json::to_vec(&json!({
                "issuer":self.origin, "organizationId":self.person.organization,
                "centralAdminAudience":ADMIN_AUDIENCE, "ownerFleetCliClientId":CLIENT_ID,
                "centralPublicOrigin":super::fixture_authority::PUBLIC_ORIGIN,
            }))
            .unwrap(),
        )
        .unwrap();
        crate::PersonLinkClient::with_trusted_root(
            target,
            self.credential.clone(),
            self.clock.clone(),
            root,
        )
        .unwrap()
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
    signin.evidence.lock().unwrap().refresh_attempts += 1;
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
    let response = {
        let mut evidence = signin.evidence.lock().unwrap();
        if form.grant_type != "refresh_token"
            || form.client_id != CLIENT_ID
            || form.resource != ADMIN_AUDIENCE
            || form.refresh_token != evidence.refresh_expected
            || !evidence.used_proofs.insert(proof)
        {
            return Err(invalid);
        }

        let mut claims = fleet_claims(signin.person, &signin.origin, now, &signin.credential.jkt);
        if signin.approval_bearer.is_some() {
            let claims = claims.as_object_mut().unwrap();
            claims.remove("second_factor_at");
            claims.remove("second_factor_method");
        }
        let access = signin.signing.sign("at+jwt", &claims);
        evidence.refresh_expected = fresh_request_id(now);
        evidence.refreshes += 1;
        evidence.access_issued = Some(access.clone());
        Json(json!({
            "access_token":access,"token_type":"DPoP","refresh_token":evidence.refresh_expected,
        }))
    };
    let gate = signin.refresh_gate.lock().unwrap().take();
    if let Some(gate) = gate {
        gate.committed.notify_one();
        gate.release.notified().await;
    }
    Ok(response)
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
    if organization != signin.person.organization
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

    let bearer = match &signin.approval_bearer {
        Some(bearer) => bearer(now, signin.evidence.lock().unwrap().upgrades + 1),
        None => {
            let mut claims = authority_claims(signin.person, &signin.origin, PERSON_AUDIENCE, now);
            claims
                .as_object_mut()
                .unwrap()
                .insert("token_profile".to_owned(), json!("person-link"));
            signin.signing.sign("at+jwt", &claims)
        }
    };
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
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let status = upstream.status();
    if status != StatusCode::SWITCHING_PROTOCOLS {
        signin
            .evidence
            .lock()
            .unwrap()
            .refused_upgrades
            .push(status.as_u16());
        return Err(status);
    }
    let upstream = upstream
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
                if let CentralCall::ListOrganizationWorkspaces(request) = *request {
                    assert_eq!(request.target.as_str(), signin.person.organization);
                    signin.evidence.lock().unwrap().reads.push(ObservedRead {
                        socket, correlation: correlation_id, request: request.body,
                    });
                } else if signin.approval_bearer.is_some()
                    && matches!(*request, CentralCall::ListPersonApprovals(_) | CentralCall::ReadPersonApproval(_) | CentralCall::DecidePersonApproval(_))
                {
                    let scripted = scripted_approval_reply(&signin.approval_fault, &request);
                    signin.evidence.lock().unwrap().approval_calls.push(ObservedApprovalCall {
                        socket, correlation: correlation_id.clone(), request: *request,
                    });
                    if let Some(reply) = scripted {
                        let frame = LinkFrame::Data(DataFrame::CentralCallReply {
                            correlation_id,
                            result: Box::new(TypedCallResult::Answered { reply }),
                        });
                        let text = encode_text(&frame, FrameDirection::ToRouter).unwrap();
                        if downstream.send(Message::Text(text.into())).await.is_err() { return }
                        continue;
                    }
                } else {
                    return;
                }
                if central.send(CentralMessage::Text(text.to_string())).await.is_err() { return }
            }
            incoming = central.next() => {
                match incoming {
                    Some(Ok(CentralMessage::Text(text))) => {
                        let Some(text) = approval_reply_with_fault(&signin.approval_fault, text) else {
                            signin.evidence.lock().unwrap().dropped_decisions += 1;
                            return;
                        };
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
