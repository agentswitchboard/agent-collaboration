#![expect(
    clippy::expect_used,
    clippy::panic_in_result_fn,
    reason = "Fixture setup failures and mismatched Host observations must fail this test at their exact assertions."
)]

use std::{
    net::SocketAddr,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkFrame,
    approval_rpc::ApprovalServiceTarget, decode_text,
};
use codex_router_host::{
    HostConfig, HostConfigInputs, HostCoordinationPaths, HostDeadlineInputs, HostDeadlines,
    HostMessageSystemFixture, HostRuntime, ManagedChildLaunchPlans, ManagedUpdateInputs,
    MessageSystemObservation, MessageSystemReason, MessageSystemStatus, OperatorRequest,
    TerminalClassification,
};
use futures_util::{SinkExt, StreamExt};
use router_clock::{ManualClock, RuntimeClock};
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
    link_policy::LinkStatus,
};
use switchboard_link_lease::{DiscoverySource, EnrollmentLeaseSource};
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use switchboard_machine_enrollment::MachineId;
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, DiscoveryAnswer, EnrollmentBinding,
    EnrollmentContext, EnrollmentId, EnrollmentKey, EnrollmentMetadata, RefreshAnswer,
    RefreshCredential, RefreshSource, SecretValue, SignInDeployment, SignInFailure, TokenGrant,
    decode_discovery,
};
use switchboard_machine_enrollment_store::{
    MachineEnrollmentStore,
    test_support::{KeychainAccess, KeychainAccessError},
};
use switchboard_message_system::test_support::{LinkFixture, start_with_link_fixture};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
};
use tokio_tungstenite::{
    WebSocketStream, accept_async,
    tungstenite::{
        Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

use super::{
    BOUND, GatedRouterProbe, TestResult, manifest, native_launch, terminal, write_native_executable,
};

mod status_probe;

use self::status_probe::{CapabilityHold, collect_status, exercise_refusal_through_renewal};

const ORGANIZATION: &str = "01890f2e-7b4c-7cc0-98c4-000000000090";
const ENROLLMENT: &str = "01890f2e-7b4c-7cc0-98c4-000000000091";

#[tokio::test]
async fn changed_native_schema_closes_the_old_socket_and_registers_a_new_run_in_one_host()
-> TestResult {
    let root = tempfile::Builder::new()
        .prefix("host-link-run-")
        .tempdir_in("/tmp")?;
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))?;
    let router = GatedRouterProbe::start().await?;
    let central = TcpListener::bind("127.0.0.1:0").await?;
    let central_address = central.local_addr()?;
    let address = format!(
        "ws://{}/v1/organizations/{ORGANIZATION}/link",
        central.local_addr()?
    );
    let (status_publication, mut systems) = mpsc::unbounded_channel();
    let clock = Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse()?));
    let (capabilities, held, release) = CapabilityHold::new();
    let fixture = system_fixture(
        root.path().to_owned(),
        address,
        central_address,
        clock.clone(),
        capabilities,
        status_publication,
    );
    let paths = HostCoordinationPaths::for_router_root(root.path());
    let collaboration = root.path().join("collaboration");
    let socket = root.path().join("native.sock");
    let (executable, plan) = native_launch(root.path(), &socket).await?;
    let config = HostConfig::new(HostConfigInputs {
        coordination_paths: paths.clone(),
        router_endpoint: router.address,
        mcp_bind: "127.0.0.1:0".parse()?,
        app_server_socket: socket,
        managed_executable: executable.clone(),
        deadlines: HostDeadlines::new(HostDeadlineInputs {
            router_start: Duration::from_secs(30),
            app_server_start: BOUND,
            remote_control: Duration::from_secs(2),
            endpoint_inspection: Duration::from_secs(1),
            operator_request: BOUND,
        })?,
    })
    .with_collaboration_directory(collaboration.clone(), root.path().join("codex"))
    .with_collaboration_peer_registry_directory(root.path().join("peers"))
    .with_message_system_fixture(fixture);
    let host = tokio::spawn(HostRuntime::run(
        config,
        ManagedChildLaunchPlans::new(None, plan),
        ManagedUpdateInputs::production(),
    ));
    let observed = async {
        let startup = crate::operator_client::send_operator_request(
            paths.operator_socket(),
            OperatorRequest::AwaitHostStart,
            BOUND,
        )
        .await?;
        assert_eq!(
            terminal(&startup)?.classification(),
            TerminalClassification::Ready
        );
        let original = manifest(&collaboration)?;
        let SystemSamples {
            machine,
            mut status,
            mut drivers,
            ..
        } = systems
            .recv()
            .await
            .ok_or("initial message system was not started")?;
        let driver = drivers.recv().await.ok_or("first driver not observed")?;
        let mut first_socket = accept_async(central.accept().await?.0).await?;
        let (stop_history, history_started, history) =
            collect_status(driver.clone(), status.clone());
        history_started.await?;
        send_welcome(&mut first_socket, &machine, "2030-01-01T00:00:10Z").await?;
        held.await?;
        assert!(!matches!(*driver.borrow(), LinkStatus::Connected { .. }));
        assert_operator_status(&paths, MessageSystemStatus::Connecting).await?;
        release
            .send(())
            .map_err(|_| "capability writer left its hold")?;
        expect_capabilities(&mut first_socket).await?;
        let first = read_registration(&mut first_socket).await?;
        let sampled = status
            .wait_for(|status| {
                matches!(
                    status.status,
                    MessageSystemStatus::Registering | MessageSystemStatus::Connected
                )
            })
            .await?
            .status;
        assert_eq!(sampled, MessageSystemStatus::Registering);
        assert_operator_status(&paths, MessageSystemStatus::Registering).await?;
        exercise_refusal_through_renewal(&mut first_socket, &first, &paths, &clock, &mut status)
            .await?;
        stop_history
            .send(())
            .map_err(|_| "early status collector ended unexpectedly")?;
        let early = history.await?;
        assert!(
            !early.saw_connected(),
            "no early publication may claim Connected"
        );
        assert!(early.driver.contains(&LinkStatus::Registering));
        assert!(
            early
                .message_system
                .iter()
                .any(|observed| observed.status == MessageSystemStatus::Registering)
        );
        clock.advance(Duration::from_secs(1))?;
        let retried = read_registration(&mut first_socket).await?;
        assert_eq!(retried.run, first.run);
        assert_ne!(retried.correlation, first.correlation);
        send_registered(&mut first_socket, &retried).await?;
        status
            .wait_for(|status| status.status == MessageSystemStatus::Connected)
            .await?;
        assert_operator_status(&paths, MessageSystemStatus::Connected).await?;

        write_native_executable(&executable, 2)?;
        let restart = crate::operator_client::send_operator_request(
            paths.operator_socket(),
            OperatorRequest::RestartAppServer,
            BOUND,
        )
        .await?;
        assert_eq!(
            terminal(&restart)?.classification(),
            TerminalClassification::Succeeded
        );
        assert!(matches!(
            first_socket.next().await,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None
        ));

        let SystemSamples {
            machine: same_machine,
            mut status,
            mut drivers,
            hold_driver,
        } = systems
            .recv()
            .await
            .ok_or("replacement message system was not started")?;
        assert_eq!(same_machine, machine);
        let _replacement_driver = drivers
            .recv()
            .await
            .ok_or("replacement driver not observed")?;
        let (mut replacement_socket, replacement) = accept_registration(&central, &machine).await?;
        let rebuilt = manifest(&collaboration)?;
        assert!(original.native_schema_digest.is_some());
        assert_ne!(rebuilt.native_schema_digest, original.native_schema_digest);
        assert_ne!(rebuilt.service_epoch, original.service_epoch);
        assert_eq!(rebuilt.service_id, original.service_id);
        assert_ne!(replacement.run, first.run);
        assert_ne!(replacement.correlation, first.correlation);
        let sampled = status
            .wait_for(|status| {
                matches!(
                    status.status,
                    MessageSystemStatus::Registering | MessageSystemStatus::Connected
                )
            })
            .await?
            .status;
        assert_eq!(sampled, MessageSystemStatus::Registering);
        assert_operator_status(&paths, MessageSystemStatus::Registering).await?;
        send_registered(&mut replacement_socket, &replacement).await?;
        status
            .wait_for(|status| status.status == MessageSystemStatus::Connected)
            .await?;
        assert_operator_status(&paths, MessageSystemStatus::Connected).await?;
        hold_driver.send_replace(true);
        replacement_socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(central_link_protocol::MachineLinkCloseCode::Revoked.code()),
                reason: "".into(),
            })))
            .await?;
        status
            .wait_for(|status| {
                status.status == MessageSystemStatus::Stopping
                    && status.reason == MessageSystemReason::EnrollmentRevoked
            })
            .await?;
        assert_operator_observation(
            &paths,
            MessageSystemStatus::Stopping,
            MessageSystemReason::EnrollmentRevoked,
        )
        .await?;
        hold_driver.send_replace(false);
        status
            .wait_for(|status| {
                status.status == MessageSystemStatus::EnrollmentEnded
                    && status.reason == MessageSystemReason::EnrollmentRevoked
            })
            .await?;
        assert_operator_observation(
            &paths,
            MessageSystemStatus::EnrollmentEnded,
            MessageSystemReason::EnrollmentRevoked,
        )
        .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    // Nextest isolates this process, whose Host owns the fixture child and sockets.
    rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::TERM)?;
    host.await??;
    router.finish().await?;
    observed
}

async fn assert_operator_status(
    paths: &HostCoordinationPaths,
    expected: MessageSystemStatus,
) -> TestResult {
    assert_operator_observation(paths, expected, MessageSystemReason::NoReason).await
}

async fn assert_operator_observation(
    paths: &HostCoordinationPaths,
    expected: MessageSystemStatus,
    reason: MessageSystemReason,
) -> TestResult {
    let reply = crate::operator_client::send_operator_request(
        paths.operator_socket(),
        OperatorRequest::Status,
        BOUND,
    )
    .await?;
    let snapshot = terminal(&reply)?.snapshot();
    assert_eq!(snapshot.message_system_status(), Some(expected));
    assert_eq!(snapshot.message_system_reason(), Some(reason));
    Ok(())
}

struct Registration {
    correlation: CorrelationId,
    target: ApprovalServiceTarget,
    run: String,
}

async fn accept_registration(
    listener: &TcpListener,
    machine: &MachineId,
) -> TestResult<(WebSocketStream<TcpStream>, Registration)> {
    let mut socket = accept_async(listener.accept().await?.0).await?;
    send_welcome(&mut socket, machine, "2030-01-01T00:10:00Z").await?;
    expect_capabilities(&mut socket).await?;
    let registration = read_registration(&mut socket).await?;
    Ok((socket, registration))
}

async fn send_welcome(
    socket: &mut WebSocketStream<TcpStream>,
    machine: &MachineId,
    effective_expires_at: &str,
) -> TestResult {
    let welcome = serde_json::json!({
        "kind": "welcome",
        "machineId": machine,
        "centralTime": "2030-01-01T00:00:00Z",
        "effectiveExpiresAt": effective_expires_at,
    });
    socket
        .send(Message::Text(welcome.to_string().into()))
        .await?;
    Ok(())
}

async fn expect_capabilities(socket: &mut WebSocketStream<TcpStream>) -> TestResult {
    assert!(matches!(
        read_frame(socket).await?,
        LinkFrame::Data(DataFrame::MacCapabilities(_))
    ));
    Ok(())
}

async fn read_registration(socket: &mut WebSocketStream<TcpStream>) -> TestResult<Registration> {
    let LinkFrame::Data(DataFrame::CallCentral {
        correlation_id,
        request,
    }) = read_frame(socket).await?
    else {
        return Err("missing registration on the admitted socket".into());
    };
    let CentralCall::RegisterRun(request) = *request else {
        return Err("the admitted socket sent the wrong call".into());
    };
    assert_eq!(request.target.organization_id.as_str(), ORGANIZATION);
    Ok(Registration {
        correlation: correlation_id,
        target: request.target,
        run: request.body.router_run.to_string(),
    })
}

async fn read_frame(socket: &mut WebSocketStream<TcpStream>) -> TestResult<LinkFrame> {
    let Some(Ok(Message::Text(wire))) = socket.next().await else {
        return Err("the fixture expected a link text frame".into());
    };
    Ok(decode_text(wire.as_str(), FrameDirection::ToCentral)?)
}

async fn send_registered(
    socket: &mut WebSocketStream<TcpStream>,
    request: &Registration,
) -> TestResult {
    socket
        .send(Message::Text(
            serde_json::json!({
                "kind": "centralCallReply",
                "correlationId": request.correlation,
                "result": { "kind": "answered", "reply": { "call": "registerRun",
                    "envelope": { "target": request.target, "body": { "kind": "registered" } }
                }}
            })
            .to_string()
            .into(),
        ))
        .await?;
    Ok(())
}

struct SystemSamples {
    machine: MachineId,
    status: watch::Receiver<MessageSystemObservation>,
    drivers: mpsc::UnboundedReceiver<watch::Receiver<LinkStatus>>,
    hold_driver: watch::Sender<bool>,
}

fn system_fixture(
    root: PathBuf,
    address: String,
    socket: SocketAddr,
    clock: Arc<ManualClock>,
    capabilities: CapabilityHold,
    systems: mpsc::UnboundedSender<SystemSamples>,
) -> HostMessageSystemFixture {
    HostMessageSystemFixture::new(move |roots, _, recipients, events| {
        let root = root.clone();
        let address = address.clone();
        let systems = systems.clone();
        let clock = clock.clone();
        let capabilities = capabilities.clone();
        Box::pin(async move {
            let machine = load_or_create_machine_id(&root, clock.utc_now().timestamp_millis())
                .await
                .expect("fixture machine");
            let store = open_enrollment(&root, &machine, clock.clone()).await;
            let (driver_samples, drivers) = mpsc::unbounded_channel();
            let (hold_driver, held_driver) = watch::channel(false);
            let system = start_with_link_fixture(
                roots,
                machine.clone(),
                store,
                clock,
                recipients,
                events,
                LinkFixture {
                    discovery: ScriptedSignIn,
                    refresh: ScriptedSignIn,
                    run_driver: move |driver: LinkDriver<EnrollmentKey, EnrollmentLeaseSource>| {
                        assert!(driver_samples.send(driver.status.subscribe()).is_ok());
                        let (address, capabilities, mut held) =
                            (address.clone(), capabilities.clone(), held_driver.clone());
                        async move {
                            run_loopback(driver, address, socket, capabilities).await;
                            let _ = held.wait_for(|hold| !*hold).await;
                        }
                    },
                },
            );
            assert!(
                systems
                    .send(SystemSamples {
                        machine,
                        status: system.status(),
                        drivers,
                        hold_driver,
                    })
                    .is_ok()
            );
            system
        })
    })
}

async fn run_loopback(
    mut driver: LinkDriver<EnrollmentKey, EnrollmentLeaseSource>,
    address: String,
    socket: SocketAddr,
    capabilities: CapabilityHold,
) {
    let settings = {
        let discovered = driver
            .settings
            .wait_for(|settings| settings.address.is_ok())
            .await
            .expect("scripted sign-in discovery");
        DriverSettings {
            binding: discovered.binding.clone(),
            address: DialAddress::test_loopback(&address),
        }
    };
    let (_settings, received) = watch::channel(settings);
    driver.settings = received;
    driver
        .run_with_test_transport(move || {
            let capabilities = capabilities.clone();
            async move {
                let (reader, writer) = TcpStream::connect(socket).await?.into_split();
                Ok(tokio::io::join(reader, capabilities.wrap(writer)))
            }
        })
        .await;
}

async fn open_enrollment(
    root: &Path,
    machine: &MachineId,
    clock: Arc<dyn RuntimeClock>,
) -> MachineEnrollmentStore {
    let store = MachineEnrollmentStore::open_with_test_keychain(
        &root.join(MACHINE_LOCAL_DATABASE),
        root.join("secrets"),
        machine.clone(),
        clock,
        Arc::new(FixtureKeychain),
    )
    .await
    .expect("fixture enrollment store");
    if matches!(
        store.read_metadata().await.expect("fixture metadata"),
        EnrollmentMetadata::Empty
    ) {
        let binding = EnrollmentBinding {
            generation: BindingGeneration::from_persisted(1).expect("first generation"),
            context: EnrollmentContext {
                machine_id: machine.clone(),
                organization_id: ORGANIZATION.parse().expect("fixture organization"),
                deployment: SignInDeployment::new(
                    "https://signin.example/".parse().expect("fixture origin"),
                )
                .expect("valid deployment"),
            },
            credential: scripted_refresh_credential("fixture-original-refresh".to_owned()),
        };
        assert_eq!(
            store
                .replace(BindingGeneration::EMPTY, &binding)
                .await
                .expect("store enrollment"),
            ConditionalWrite::Applied
        );
    }
    store
}

fn scripted_refresh_credential(refresh: String) -> RefreshCredential {
    RefreshCredential {
        enrollment_id: ENROLLMENT
            .parse::<EnrollmentId>()
            .expect("fixture enrollment"),
        refresh_token: SecretValue::new(refresh).expect("scripted refresh"),
    }
}

#[derive(Clone)]
struct ScriptedSignIn;

impl DiscoverySource for ScriptedSignIn {
    async fn discover(&self) -> Result<DiscoveryAnswer, SignInFailure> {
        Ok(decode_discovery(br#"{"client_id":"01890f2e-7b4c-7cc0-98c4-000000000091","link_url":"https://central.example/v1/organizations/{organization_id}/link"}"#)
            .expect("scripted discovery"))
    }
}

impl RefreshSource for ScriptedSignIn {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        RefreshAnswer::Rotated(TokenGrant {
            access_token: SecretValue::new("fixture-machine.access.token".to_owned())
                .expect("scripted bearer"),
            expires_in: Duration::from_secs(600),
            credential: scripted_refresh_credential("fixture-rotated-refresh".to_owned()),
        })
    }
}

struct FixtureKeychain;

impl KeychainAccess for FixtureKeychain {
    fn read_secret(&self, _: &str, _: &str) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        Ok(Some(vec![0x5c; 32]))
    }

    fn add_secret(&self, _: &str, _: &str, _: &[u8]) -> Result<(), KeychainAccessError> {
        Err(KeychainAccessError::Unavailable)
    }
}
