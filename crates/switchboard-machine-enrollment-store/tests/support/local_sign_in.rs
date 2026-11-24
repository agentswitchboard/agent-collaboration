//! Scripted local HTTPS peer shared by this package's enrollment tests.

use std::{
    net::Ipv4Addr,
    path::Path,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde_json::{Value, json};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    sync::{Notify, oneshot},
    task::JoinHandle,
};
use tokio_rustls::TlsAcceptor;

const ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000002";
const CLIENT: &str = "019a0000-0000-7000-8000-000000000004";
pub(crate) const REFRESH: &str = "fictional-tls-initial-refresh";
pub(super) const ROTATED: &str = "fictional-tls-rotated-refresh";

struct InitialGrantGate {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[derive(Clone)]
struct SignInState {
    origin: String,
    token_requests: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    request_arrived: Arc<Notify>,
    initial_grant: Arc<Mutex<Option<InitialGrantGate>>>,
}

impl SignInState {
    fn observe_request(&self) {
        self.requests.fetch_add(1, SeqCst);
        self.request_arrived.notify_one();
    }
}

/// Tests open this peer with local TLS, scripted replies, and request observations.
pub(crate) struct LocalSignIn {
    pub(crate) origin: String,
    pub(crate) root_certificate: Vec<u8>,
    token_requests: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    request_arrived: Arc<Notify>,
    initial_grant: Arc<Mutex<Option<InitialGrantGate>>>,
    proxy: JoinHandle<()>,
    http: JoinHandle<()>,
}

impl LocalSignIn {
    pub(crate) async fn start() -> Self {
        let certificate = tokio::task::spawn_blocking(test_identity)
            .await
            .expect("fixture certificate");
        let proxy = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let origin = format!("https://{}/", proxy.local_addr().unwrap());
        let token_requests = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let request_arrived = Arc::new(Notify::new());
        let initial_grant = Arc::new(Mutex::new(None));
        let state = SignInState {
            origin: origin.clone(),
            token_requests: token_requests.clone(),
            requests: requests.clone(),
            request_arrived: request_arrived.clone(),
            initial_grant: initial_grant.clone(),
        };
        let service = Router::new()
            .route("/.well-known/fleet-login", get(discover))
            .route("/api/auth/device/code", post(start))
            .route("/api/auth/oauth2/token", post(token))
            .with_state(state);
        let http = tokio::spawn(async move {
            axum::serve(upstream, service).await.unwrap();
        });
        let acceptor = TlsAcceptor::from(Arc::new(certificate.server));
        let proxy = tokio::spawn(async move {
            while let Ok((socket, _)) = proxy.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(socket).await else {
                        return;
                    };
                    let mut plain = TcpStream::connect(upstream_address).await.unwrap();
                    let _ = copy_bidirectional(&mut tls, &mut plain).await;
                });
            }
        });
        Self {
            origin,
            root_certificate: certificate.root,
            token_requests,
            requests,
            request_arrived,
            initial_grant,
            proxy,
            http,
        }
    }

    pub(crate) fn token_requests(&self) -> usize {
        self.token_requests.load(SeqCst)
    }

    pub(crate) fn requests(&self) -> usize {
        self.requests.load(SeqCst)
    }

    pub(crate) async fn next_request(&self) {
        self.request_arrived.notified().await;
    }

    pub(crate) fn hold_initial_grant(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, observed) = oneshot::channel();
        let (release, held) = oneshot::channel();
        *self.initial_grant.lock().unwrap() = Some(InitialGrantGate {
            reached,
            release: held,
        });
        (observed, release)
    }
}

impl Drop for LocalSignIn {
    fn drop(&mut self) {
        self.proxy.abort();
        self.http.abort();
    }
}

async fn discover(State(state): State<SignInState>) -> Json<Value> {
    state.observe_request();
    Json(json!({
        "client_id": CLIENT,
        "link_url": "https://central.example/v1/organizations/{organization_id}/link"
    }))
}

async fn start(State(state): State<SignInState>) -> Json<Value> {
    state.observe_request();
    Json(json!({
        "device_code": "fictional-test-device-code",
        "user_code": "ABCD-EFGH",
        "verification_uri": format!("{}device", state.origin),
        "verification_uri_complete": format!("{}device?user_code=ABCD-EFGH", state.origin),
        "expires_in": 120,
        "interval": 5
    }))
}

async fn token(State(state): State<SignInState>, form: Bytes) -> (StatusCode, Json<Value>) {
    state.observe_request();
    let form = std::str::from_utf8(&form).unwrap();
    let ordinal = state.token_requests.fetch_add(1, SeqCst);
    assert!(if ordinal == 0 {
        form.contains("device_code=fictional-test-device-code")
    } else {
        form.contains("grant_type=refresh_token") && form.contains(REFRESH)
    });
    let (access, refresh) = if ordinal == 0 {
        let held = state.initial_grant.lock().unwrap().take();
        if let Some(held) = held {
            held.reached.send(()).unwrap();
            held.release.await.unwrap();
        }
        ("fictional-tls-initial-access", REFRESH)
    } else {
        ("fictional-tls-rotated-access", ROTATED)
    };
    (
        StatusCode::OK,
        Json(json!({
            "access_token": access,
            "refresh_token": refresh,
            "token_type": "Bearer",
            "expires_in": 60,
            "machine_client_id": ENROLLMENT,
        "scope": "offline_access",
        "expires_at": 60
        })),
    )
}

struct TestIdentity {
    root: Vec<u8>,
    server: rustls::ServerConfig,
}

fn openssl(directory: &Path, arguments: &[&str]) {
    let result = std::process::Command::new("/usr/bin/openssl")
        .current_dir(directory)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("existing repository OpenSSL fixture tool");
    assert!(result.success(), "fixture certificate generation failed");
}

fn test_identity() -> TestIdentity {
    let root = tempfile::tempdir().expect("ephemeral certificate directory");
    let directory = root.path();
    std::fs::write(
        directory.join("server-extensions"),
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1\n",
    ).unwrap();
    openssl(
        directory,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "1",
            "-subj",
            "/CN=Enrollment Test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
    );
    openssl(
        directory,
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=Enrollment Test",
        ],
    );
    openssl(
        directory,
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-set_serial",
            "1",
            "-days",
            "1",
            "-extfile",
            "server-extensions",
            "-out",
            "server.pem",
        ],
    );
    let certificate = CertificateDer::from_pem_file(directory.join("server.pem")).unwrap();
    let key = PrivateKeyDer::from_pem_file(directory.join("server.key")).unwrap();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .unwrap();
    TestIdentity {
        server,
        root: std::fs::read(directory.join("ca.pem")).unwrap(),
    }
}
