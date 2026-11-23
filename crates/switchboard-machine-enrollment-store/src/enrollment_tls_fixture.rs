use std::{
    net::Ipv4Addr,
    path::Path,
    process::Stdio,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use router_clock::ClockInstant;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde_json::{Value, json};
use switchboard_machine_enrollment::{
    AccessRequest, EnrollmentRequest, LeaseUnavailable, RefreshAnswer, RefreshCredential,
    RefreshOwner, RefreshSource, SignInClient,
};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_rustls::TlsAcceptor;

use super::{
    ConditionalWrite, CredentialStore, EnrollmentEnd, Fixture, InvalidReason, StoredEnrollment,
    TestWriteFault, fixture::token_value, generation,
};

const CLIENT: &str = "019a0000-0000-7000-8000-000000000004";
const REFRESH: &str = "fictional-tls-initial-refresh";
const ROTATED: &str = "fictional-tls-rotated-refresh";

#[derive(Clone)]
struct SignInState {
    origin: String,
    token_requests: Arc<AtomicUsize>,
}

struct LocalSignIn {
    origin: String,
    root_certificate: Vec<u8>,
    token_requests: Arc<AtomicUsize>,
    proxy: JoinHandle<()>,
    http: JoinHandle<()>,
}

impl LocalSignIn {
    async fn start() -> Self {
        let certificate = tokio::task::spawn_blocking(test_identity)
            .await
            .expect("fixture certificate");
        let proxy = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let origin = format!("https://{}/", proxy.local_addr().unwrap());
        let token_requests = Arc::new(AtomicUsize::new(0));
        let state = SignInState {
            origin: origin.clone(),
            token_requests: token_requests.clone(),
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
            proxy,
            http,
        }
    }
}

impl Drop for LocalSignIn {
    fn drop(&mut self) {
        self.proxy.abort();
        self.http.abort();
    }
}

async fn discover() -> Json<Value> {
    Json(json!({
        "client_id": CLIENT,
        "link_url": "https://central.example/v1/organizations/{organization_id}/link"
    }))
}

async fn start(State(state): State<SignInState>) -> Json<Value> {
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
    let form = std::str::from_utf8(&form).unwrap();
    let ordinal = state.token_requests.fetch_add(1, SeqCst);
    assert!(if ordinal == 0 {
        form.contains("device_code=fictional-test-device-code")
    } else {
        form.contains("grant_type=refresh_token") && form.contains(REFRESH)
    });
    let (access, refresh) = if ordinal == 0 {
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
            "machine_client_id": super::ENROLLMENT,
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

#[tokio::test]
async fn public_https_install_rotation_and_explicit_marker_use_real_sqlite() {
    let service = LocalSignIn::start().await;
    let fixture = Fixture::open().await;
    let client = SignInClient::new_https_fixture(
        service.origin.parse().unwrap(),
        fixture.clock.clone(),
        &service.root_certificate,
    )
    .unwrap();
    let ordinary =
        SignInClient::new(service.origin.parse().unwrap(), fixture.clock.clone()).unwrap();
    assert!(
        ordinary.discover().await.is_err(),
        "ordinary client trusted the private fixture root"
    );
    let organization_id = super::fixture::ORGANIZATION.parse().unwrap();

    let request = EnrollmentRequest {
        machine_id: &fixture.machine,
        machine_label: "Fixture",
        organization_id,
    };
    let mut install = std::pin::pin!(client.enroll(request, &fixture.store, |_| Ok(())));
    let first_poll = ClockInstant::ZERO.after(Duration::from_secs(5));
    tokio::select! {
        result = &mut install => panic!("installation finished before its poll: {result:?}"),
        () = fixture.clock.not_due_check_now(first_poll) => {}
    }
    fixture.clock.advance(Duration::from_secs(5)).unwrap();
    let receipt = install.await.unwrap();

    let reopened = fixture.another_worker().await;
    let StoredEnrollment::Active(initial) = reopened.read().await.unwrap() else {
        panic!("installation was not persisted");
    };
    assert_eq!(receipt.generation, generation(1));
    assert_eq!(receipt.enrollment_id, initial.credential.enrollment_id);
    assert!(initial.credential.refresh_token == token_value(REFRESH));
    assert_eq!(initial.context.deployment.origin().as_str(), service.origin);

    let owner = client.refresh_owner(
        fixture.store.clone(),
        fixture.machine.clone(),
        organization_id,
    );
    fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
    let lease = owner.lease(AccessRequest::Usable).await.unwrap();
    assert_eq!(lease.binding_generation, generation(2));
    assert_eq!(lease.key, initial.key());
    assert_eq!(service.token_requests.load(SeqCst), 2);
    let StoredEnrollment::Active(rotated) = reopened.read().await.unwrap() else {
        panic!("rotation was not persisted");
    };
    assert!(rotated.credential.refresh_token == token_value(ROTATED));
    assert_eq!(fixture.faults.probes.load(SeqCst), 1);

    fixture.faults.arm(TestWriteFault::BeforeCommit);
    assert_eq!(
        owner
            .end_enrollment(&lease.key, EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    assert_eq!(
        reopened.read().await,
        Ok(StoredEnrollment::Ended {
            generation: generation(3),
            why: EnrollmentEnd::Revoked,
            key: lease.key,
        })
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 1);
}

struct Refused;

impl RefreshSource for Refused {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        RefreshAnswer::Refused {
            reason: InvalidReason::GrantRefused,
        }
    }
}

#[tokio::test]
async fn a_refresh_refusal_marks_the_real_row_ended_even_after_lost_acknowledgement() {
    let fixture = Fixture::open().await;
    let binding = fixture.install().await;
    let owner = RefreshOwner::new(
        fixture.store.clone(),
        Refused,
        fixture.clock.clone(),
        binding.context.clone(),
    );
    fixture.faults.arm(TestWriteFault::LoseAcknowledgement);

    assert_eq!(
        owner.lease(AccessRequest::Usable).await,
        Err(LeaseUnavailable::Ended {
            why: EnrollmentEnd::RefreshRefused,
        })
    );
    assert_eq!(
        fixture.another_worker().await.read().await,
        Ok(StoredEnrollment::Ended {
            generation: generation(2),
            why: EnrollmentEnd::RefreshRefused,
            key: binding.key(),
        })
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 1);
}
