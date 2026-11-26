#![expect(
    clippy::unwrap_used,
    reason = "Scripted peer and lease fixtures fail at the exact unexpected observation."
)]

use std::{
    future::poll_fn,
    sync::{Arc, Mutex},
    time::Duration,
};

use central_link_client::{EnrollmentEnd, LinkLaneEvent, ReadyLinkLane};
use central_link_protocol::{
    CentralCall, CentralCallReply, CorrelationId, DataFrame, FrameDirection, HandledMacCalls,
    LinkFrame, MacCapabilities, MachineLinkCloseCode, TokenRenewalResult, TypedCallResult,
    approval_rpc::{ApprovalServiceTarget, RegisterRunReply},
    decode_text, encode_text,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use router_clock::{ClockInstant, ManualClock};
use switchboard_central_link::{
    DialAddress,
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource, LinkLease},
    link_driver::{DriverSettings, LinkDriver},
    link_lane::{LinkLaneHandle, link_lane_channel},
    link_policy::{LeaseVersion, LeaseWindow, LinkStatus},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{
    WebSocketStream, accept_async, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        protocol::CloseFrame,
    },
};
use workspace_ids::MachineId;

mod admitted_commands;
mod central_calls;
mod credential_diagnostics;
mod lease_boundaries;
mod mac_capabilities;
mod registered_commands;
mod renewal_endings;
mod run_registration;
mod settings_replay;
mod transport_limits;

const TOKEN: &str = "fictional-fixture-access-token.0.signature";
const REFRESHED: &str = "fictional-fixture-access-token.1.signature";
const NEXT_REFRESHED: &str = "fictional-fixture-access-token.2.signature";
const DELIVER_FRAME: &str = r#"{"kind":"deliver","timeLeft":1,"sender":{"kind":"central"},"message":{"messageId":"01890f2e-7b4c-7cc0-98c4-000000000009","sender":{"kind":"central"},"target":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"},"body":{"kind":"direct","body":"hello","delivery":"auto","replyTo":null},"state":{"kind":"queued"},"source":null}}"#;

fn machine() -> MachineId {
    "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap()
}

fn tick(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

struct FakeSource {
    acquired: mpsc::UnboundedSender<Option<LeaseVersion>>,
    ended: mpsc::UnboundedSender<(u8, EnrollmentEnd)>,
    initial_gate: Mutex<Option<InitialLeaseGate>>,
}

type InitialLeaseGate = oneshot::Receiver<Result<(), LeaseFailure>>;

impl LeaseSource<u8> for FakeSource {
    fn registration_target(
        &self,
        _binding: &u8,
    ) -> Result<switchboard_central_link::lease_source::ApprovalServiceTarget, LeaseFailure> {
        let organization_id = "01890f2e-7b4c-7cc0-98c4-000000000010"
            .to_owned()
            .try_into()
            .map_err(|_| LeaseFailure::Fault)?;
        Ok(switchboard_central_link::lease_source::ApprovalServiceTarget { organization_id })
    }

    fn acquire<'a>(
        &'a self,
        binding: &'a u8,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'a, u8> {
        let _ = self.acquired.send(newer_than);
        let initial_gate = self.initial_gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some(gate) = initial_gate {
                gate.await.unwrap()?;
            }
            Ok(LinkLease {
                window: LeaseWindow {
                    binding: *binding,
                    version: LeaseVersion(newer_than.map_or(1, |old| old.0 + 1)),
                    usable_until: tick(600),
                },
                token: match newer_than {
                    None => TOKEN,
                    Some(LeaseVersion(1)) => REFRESHED,
                    Some(_) => NEXT_REFRESHED,
                }
                .to_owned()
                .try_into()
                .unwrap(),
            })
        })
    }
    fn end(&self, binding: &u8, cause: EnrollmentEnd) {
        let _ = self.ended.send((*binding, cause));
    }
}

struct Fixture {
    clock: Arc<ManualClock>,
    listener: TcpListener,
    settings: watch::Sender<DriverSettings<u8>>,
    shutdown: watch::Sender<bool>,
    status: watch::Receiver<LinkStatus>,
    acquired: mpsc::UnboundedReceiver<Option<LeaseVersion>>,
    ended: mpsc::UnboundedReceiver<(u8, EnrollmentEnd)>,
    inbox_events: FixtureInbox,
    driver_polls: watch::Receiver<u64>,
    task: JoinHandle<()>,
}

struct FixtureInbox {
    lane: LinkLaneHandle,
}

impl FixtureInbox {
    async fn recv(&self) -> Option<LinkLaneEvent> {
        while let Some(event) = self.lane.next_event().await {
            if matches!(event, LinkLaneEvent::Deliver { .. }) {
                return Some(event);
            }
        }
        None
    }
}

impl Fixture {
    async fn new() -> Self {
        Self::with_initial_lease(None).await
    }

    async fn with_initial_lease(initial_gate: Option<InitialLeaseGate>) -> Self {
        let clock = Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dial = DialAddress::test_loopback(&format!(
            "ws://{}/organization/link",
            listener.local_addr().unwrap()
        ));
        let (settings, settings_rx) = watch::channel(DriverSettings {
            binding: Some(1),
            address: dial,
        });
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (status_tx, status) = watch::channel(LinkStatus::NotEnrolled);
        let (acquired_tx, acquired) = mpsc::unbounded_channel();
        let (ended_tx, ended) = mpsc::unbounded_channel();
        let (lane_handle, lane) = link_lane_channel();
        let mut driver = Box::pin(
            LinkDriver {
                machine_id: machine(),
                clock: clock.clone(),
                lease_source: Arc::new(FakeSource {
                    acquired: acquired_tx,
                    ended: ended_tx,
                    initial_gate: Mutex::new(initial_gate),
                }),
                settings: settings_rx,
                shutdown: shutdown_rx,
                status: status_tx,
                lane,
            }
            .run(),
        );
        let (poll_count, driver_polls) = watch::channel(0);
        let task = tokio::spawn(poll_fn(move |cx| {
            let result = driver.as_mut().poll(cx);
            poll_count.send_modify(|count| *count += 1);
            result
        }));
        Self {
            clock,
            listener,
            settings,
            shutdown,
            status,
            acquired,
            ended,
            inbox_events: FixtureInbox { lane: lane_handle },
            driver_polls,
            task,
        }
    }

    async fn accept(&self) -> WebSocketStream<TcpStream> {
        let (stream, _) = self.listener.accept().await.unwrap();
        accept_async(stream).await.unwrap()
    }

    async fn stop(self) {
        self.shutdown.send_replace(true);
        self.task.await.unwrap();
    }
}

async fn send_welcome(socket: &mut WebSocketStream<TcpStream>) {
    let wire = r#"{"kind":"welcome","machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","centralTime":"2030-01-01T00:00:00Z","effectiveExpiresAt":"2030-01-01T00:02:00Z"}"#;
    socket.send(Message::Text(wire.into())).await.unwrap();
}

async fn welcome(socket: &mut WebSocketStream<TcpStream>) {
    welcome_before_registration(socket).await;
    acknowledge_registration(socket).await;
}

async fn welcome_before_registration(socket: &mut WebSocketStream<TcpStream>) {
    send_welcome(socket).await;
    assert_eq!(
        decode_text(&text(socket).await, FrameDirection::ToCentral).unwrap(),
        LinkFrame::Data(DataFrame::MacCapabilities(MacCapabilities {
            handled_calls: HandledMacCalls::default(),
        }))
    );
}

#[expect(
    clippy::panic,
    reason = "A fixture must fail when the driver emits a different control frame."
)]
async fn registration_request(
    socket: &mut WebSocketStream<TcpStream>,
) -> (
    CorrelationId,
    ApprovalServiceTarget,
    workspace_ids::RouterRunId,
) {
    let LinkFrame::Data(DataFrame::CallCentral {
        correlation_id,
        request,
    }) = decode_text(&text(socket).await, FrameDirection::ToCentral).unwrap()
    else {
        panic!("the registration frame must follow the capability frame");
    };
    let CentralCall::RegisterRun(request) = *request else {
        panic!("the driver's registration exchange must request RegisterRun");
    };
    (correlation_id, request.target, request.body.router_run)
}

async fn acknowledge_registration(socket: &mut WebSocketStream<TcpStream>) {
    let (correlation_id, target, _) = registration_request(socket).await;
    reply_registration(
        socket,
        correlation_id,
        target,
        RegisterRunReply::Registered {},
    )
    .await;
}

async fn reply_registration(
    socket: &mut WebSocketStream<TcpStream>,
    correlation_id: CorrelationId,
    target: ApprovalServiceTarget,
    body: RegisterRunReply,
) {
    let result = serde_json::json!({
        "kind": "answered",
        "reply": { "call": "registerRun", "envelope": { "target": target, "body": body } },
    });
    let result: TypedCallResult<CentralCallReply> = serde_json::from_value(result).unwrap();
    let wire = encode_text(
        &LinkFrame::Data(DataFrame::CentralCallReply {
            correlation_id,
            result: Box::new(result),
        }),
        FrameDirection::ToRouter,
    )
    .unwrap();
    socket.send(Message::Text(wire.into())).await.unwrap();
}

#[expect(
    clippy::panic,
    reason = "A non-text frame fails this scripted test immediately."
)]
async fn text(socket: &mut WebSocketStream<TcpStream>) -> String {
    let message = socket.next().await.unwrap().unwrap();
    if let Message::Text(value) = message {
        return value.to_string();
    }
    panic!("expected one textual link frame");
}

#[tokio::test]
async fn renewal_extends_the_same_socket_without_disclosing_credentials() {
    let diagnostics = credential_diagnostics::capture_trace();
    let mut fixture = Fixture::new().await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    let (stream, _) = fixture.listener.accept().await.unwrap();
    #[expect(
        clippy::result_large_err,
        reason = "The upstream handshake callback uses a full HTTP rejection response."
    )]
    let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
        assert!(
            request
                .headers()
                .get("authorization")
                .is_some_and(|value| value.as_bytes() == format!("Bearer {TOKEN}").as_bytes())
        );
        assert_eq!(
            request.uri().path_and_query().map(|value| value.as_str()),
            Some("/organization/link"),
            "bearer bytes must stay out of the request URL"
        );
        Ok(response)
    })
    .await
    .unwrap();
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    fixture
        .clock
        .set_utc("2070-04-01T00:00:00Z".parse().unwrap());
    fixture.clock.not_due_check_now(tick(20)).await;
    assert!(
        fixture.acquired.try_recv().is_err(),
        "wall-clock jumps cannot request a lease"
    );
    fixture.clock.advance(Duration::from_secs(60)).unwrap();
    assert_eq!(text(&mut socket).await, "ping");
    socket.send(Message::Text("pong".into())).await.unwrap();
    assert_eq!(fixture.acquired.recv().await, Some(Some(LeaseVersion(1))));
    let renewal = text(&mut socket).await;
    let LinkFrame::Data(DataFrame::RenewMachineToken {
        correlation_id,
        token,
    }) = decode_text(&renewal, FrameDirection::ToCentral).unwrap()
    else {
        panic!("renewal on the original socket");
    };
    assert!(token.expose() == REFRESHED);
    let answer = LinkFrame::Data(DataFrame::MachineTokenRenewal {
        correlation_id,
        result: TokenRenewalResult::Renewed {
            central_time: "2030-01-01T00:01:00Z".parse().unwrap(),
            effective_expires_at: "2030-01-01T00:06:00Z".parse().unwrap(),
        },
    });
    let result = central_link_protocol::encode_text(&answer, FrameDirection::ToRouter).unwrap();
    socket.send(Message::Text(result.into())).await.unwrap();
    socket
        .send(Message::Text(DELIVER_FRAME.into()))
        .await
        .unwrap();
    tokio::select! {
        event = fixture.inbox_events.recv() => assert!(matches!(event, Some(LinkLaneEvent::Deliver { .. }))),
        status = fixture.status.wait_for(|status| !matches!(status, LinkStatus::Connected { .. })) =>
            assert!(status.is_err(), "renewal must keep the original socket open for Deliver"),
    }
    // TCP ordering puts the renewal response before any later inbound text.
    socket.send(Message::Text("pong".into())).await.unwrap();
    fixture.clock.advance(Duration::from_secs(61)).unwrap();
    assert_eq!(
        text(&mut socket).await,
        "ping",
        "the old deadline cannot close a renewed socket"
    );
    assert!(matches!(
        &*fixture.status.borrow(),
        LinkStatus::Connected { .. }
    ));
    assert!(fixture.acquired.try_recv().is_err());
    socket
        .send(Message::Close(Some(CloseFrame {
            code: MachineLinkCloseCode::Inactive.code().into(),
            reason: REFRESHED.into(),
        })))
        .await
        .unwrap();
    assert_eq!(
        fixture.ended.recv().await,
        Some((1, EnrollmentEnd::Inactive))
    );
    let emitted = format!("{:?}", fixture.status.borrow());
    assert!(!emitted.contains(TOKEN) && !emitted.contains(REFRESHED));
    fixture.stop().await;
    diagnostics.assert_credentials_absent();
}

#[tokio::test]
async fn final_close_before_welcome_ends_the_exact_binding_and_waits_for_a_new_one() {
    for (code, cause) in [
        (MachineLinkCloseCode::Revoked, EnrollmentEnd::Revoked),
        (MachineLinkCloseCode::Inactive, EnrollmentEnd::Inactive),
    ] {
        let mut fixture = Fixture::new().await;
        let mut socket = fixture.accept().await;
        assert_eq!(fixture.acquired.recv().await, Some(None));
        socket
            .send(Message::Close(Some(CloseFrame {
                code: code.code().into(),
                reason: "".into(),
            })))
            .await
            .unwrap();
        assert_eq!(fixture.ended.recv().await, Some((1, cause)));
        assert_eq!(
            *fixture.status.borrow(),
            LinkStatus::EnrollmentEnded { cause }
        );
        fixture.clock.advance(Duration::from_secs(1000)).unwrap();
        let same = fixture.settings.borrow().clone();
        fixture.settings.send_replace(same);
        assert!(fixture.acquired.try_recv().is_err());
        let mut different = fixture.settings.borrow().clone();
        different.binding = Some(2);
        fixture.settings.send_replace(different);
        assert_eq!(fixture.acquired.recv().await, Some(None));
        fixture.stop().await;
    }
}

#[tokio::test]
async fn two_http_unauthorized_answers_request_one_newer_lease_then_fault() {
    let mut fixture = Fixture::new().await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    for token in [TOKEN, REFRESHED] {
        let (stream, _) = fixture.listener.accept().await.unwrap();
        let expected = format!("Bearer {token}");
        #[expect(
            clippy::result_large_err,
            reason = "The upstream handshake callback uses a full HTTP rejection response."
        )]
        let result = accept_hdr_async(stream, |request: &Request, _response: Response| {
            assert!(
                request
                    .headers()
                    .get("authorization")
                    .is_some_and(|header| header.as_bytes() == expected.as_bytes())
            );
            let mut rejection = ErrorResponse::new(None);
            *rejection.status_mut() =
                tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
            Err(rejection)
        })
        .await;
        assert!(result.is_err());
        if token == TOKEN {
            fixture.clock.not_due_check_now(tick(1)).await;
            fixture.clock.advance(Duration::from_secs(1)).unwrap();
            assert_eq!(fixture.acquired.recv().await, Some(Some(LeaseVersion(1))));
        }
    }
    let fault = fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Fault { .. }))
        .await
        .unwrap();
    assert_eq!(
        *fault,
        LinkStatus::Fault {
            kind: central_link_client::LinkFault::FreshCredentialRefused
        }
    );
    drop(fault);
    assert!(fixture.acquired.try_recv().is_err() && fixture.ended.try_recv().is_err());
    fixture.stop().await;
}

#[test]
fn only_secure_or_test_literal_loopback_addresses_parse() {
    assert!(DialAddress::secure("wss://central.example/organization/link").is_ok());
    assert!(DialAddress::test_loopback("ws://127.0.0.1:8080/organization/link").is_ok());
    assert!(DialAddress::test_loopback("ws://[::1]:8080/organization/link").is_ok());
    for address in [
        "ws://central.example/organization/link",
        "ws://localhost/organization/link",
        "ws://127.0.0.2/organization/link",
        "wss://access@central.example/organization/link",
        "wss://central.example/organization/link?access_token=secret",
    ] {
        assert!(DialAddress::secure(address).is_err());
        assert!(DialAddress::test_loopback(address).is_err());
    }
}

#[tokio::test]
async fn withheld_welcome_uses_one_elapsed_window_then_redials() {
    let mut fixture = Fixture::new().await;
    let mut socket = fixture.accept().await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    let actual_limit = tokio::select! {
        () = fixture.clock.not_due_check_now(tick(10)) => 10,
        () = fixture.clock.not_due_check_now(tick(9)) => 9,
    };
    assert_eq!(
        actual_limit, 10,
        "the dial must get its complete Welcome window"
    );
    fixture.clock.advance(Duration::from_secs(9)).unwrap();
    assert!(socket.next().now_or_never().is_none());
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        socket.next().await,
        None | Some(Ok(Message::Close(_))) | Some(Err(_))
    ));
    fixture.clock.not_due_check_now(tick(11)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut next = fixture.accept().await;
    welcome(&mut next).await;
    fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn reconnect_requires_two_transmitted_unanswered_heartbeats() {
    let mut fixture = Fixture::new().await;
    let mut socket = fixture.accept().await;
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(text(&mut socket).await, "ping");
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(text(&mut socket).await, "ping");
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    assert!(matches!(
        socket.next().await,
        None | Some(Ok(Message::Close(_))) | Some(Err(_))
    ));
    fixture.clock.not_due_check_now(tick(61)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut next = fixture.accept().await;
    welcome(&mut next).await;
    fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn busy_retries_and_bad_address_parks_with_no_final_enrollment_effect() {
    use central_link_client::LinkFault;
    use tokio_tungstenite::tungstenite::http::StatusCode;

    let mut fixture = Fixture::new().await;
    for reason in [StatusCode::SERVICE_UNAVAILABLE, StatusCode::BAD_REQUEST] {
        let (stream, _) = fixture.listener.accept().await.unwrap();
        #[expect(
            clippy::result_large_err,
            reason = "The upstream handshake callback uses a full HTTP rejection response."
        )]
        let result = accept_hdr_async(stream, |_request: &Request, _response| {
            let mut rejection = ErrorResponse::new(None);
            *rejection.status_mut() = reason;
            Err(rejection)
        })
        .await;
        assert!(result.is_err());
        assert_eq!(fixture.acquired.recv().await, Some(None));
        if reason == StatusCode::SERVICE_UNAVAILABLE {
            fixture.clock.not_due_check_now(tick(1)).await;
            fixture.clock.advance(Duration::from_secs(1)).unwrap();
        }
    }
    let fault = fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Fault { .. }))
        .await
        .unwrap();
    assert_eq!(
        *fault,
        LinkStatus::Fault {
            kind: LinkFault::MalformedAddress
        }
    );
    drop(fault);
    assert!(fixture.ended.try_recv().is_err());
    fixture.stop().await;
}

#[tokio::test]
async fn shutdown_joins_after_sending_the_idle_sockets_close() {
    let mut fixture = Fixture::new().await;
    let mut socket = fixture.accept().await;
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|state| matches!(state, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    fixture.clock.not_due_check_now(tick(20)).await;

    fixture.shutdown.send_replace(true);
    fixture.task.await.unwrap();
    assert!(
        matches!(socket.next().await, Some(Ok(Message::Close(_)))),
        "shutdown must send the idle socket's Close frame before dropping it"
    );
    fixture.clock.advance(Duration::from_secs(600)).unwrap();
    assert!(
        !fixture.clock.has_not_due_check_now(tick(620)),
        "the joined task cannot schedule another heartbeat"
    );
}
