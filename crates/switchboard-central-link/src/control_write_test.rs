use std::{
    cell::Cell,
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
    time::Duration,
};

use central_link_client::{
    ClientAction, ConnectionAttempt, LinkClient, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
    RequestStatusCommand, link_frame_answer_deadline, link_frame_write_deadline,
};
use central_link_protocol::{
    DataFrame, FrameDirection, HandledMacCalls, LinkFrame, MacCapabilities, RequestScope,
    RequestStatusQuery, decode_text, encode_text,
};
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use tokio::{
    io::{AsyncReadExt, DuplexStream},
    sync::watch,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as WebSocketError, Message, protocol::Role},
};

use super::{HeartbeatFixture, NoLeaseSource, WelcomedDriver, elapsed_tick, test_settings};
use crate::{
    lane_writer::tests::BufferedFixture,
    link_driver::{Control, DriverSettings, Interrupted, SocketAttemptTiming},
    link_lane::link_lane_channel,
    link_policy::{LeaseVersion, LeaseWindow, LinkInput},
    wire_write_fence::WireWriteFence,
};

type Peer = WebSocketStream<DuplexStream>;

const UNKNOWN_CALL: &str = r#"{"kind":"callMac","correlationId":"01890f2e-7b4c-7cc0-98c4-000000000091","request":{"call":"fixtureFutureCall","envelope":{"target":"s","body":{}}}}"#;

#[derive(Default)]
struct SocketProbe {
    close_attempts: Cell<usize>,
    quarantined_ready_polls: Cell<usize>,
    cross_after_pending_timer: Cell<Option<ClockInstant>>,
}

struct ProbedSocket<'test, Socket> {
    socket: Socket,
    clock: &'test ManualClock,
    boundary: &'test WireWriteFence,
    probe: &'test SocketProbe,
}

impl<Socket: Stream + Unpin> Stream for ProbedSocket<'_, Socket> {
    type Item = Socket::Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.socket).poll_next(context)
    }
}

impl<Socket: Sink<Message, Error = WebSocketError> + Unpin> Sink<Message>
    for ProbedSocket<'_, Socket>
{
    type Error = WebSocketError;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.boundary.must_discard() {
            self.probe
                .quarantined_ready_polls
                .set(self.probe.quarantined_ready_polls.get() + 1);
        }
        Pin::new(&mut self.socket).poll_ready(context)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        if matches!(item, Message::Close(_)) {
            self.probe
                .close_attempts
                .set(self.probe.close_attempts.get() + 1);
        }
        Pin::new(&mut self.socket).start_send(item)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(deadline) = self.probe.cross_after_pending_timer.take() {
            assert!(
                self.clock.has_not_due_check_now(deadline),
                "the biased timer must have returned Pending immediately before this flush"
            );
            let now = self.clock.monotonic_now();
            assert!(now < deadline);
            self.clock
                .advance(deadline.saturating_duration_since(now))
                .unwrap();
        }
        Pin::new(&mut self.socket).poll_flush(context)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.socket).poll_close(context)
    }
}

pub(super) fn fresh_driver(
    fixture: &HeartbeatFixture,
    binding: u8,
) -> WelcomedDriver<'_, NoLeaseSource> {
    let (mut driver, _) = fixture.welcomed_driver(&NoLeaseSource);
    driver.client = LinkClient::new(fixture.machine.clone(), fixture.clock.clone());
    driver.apply(LinkInput::BindingAvailable(binding)).unwrap();
    driver
}

pub(super) fn start_attempt(
    driver: &mut WelcomedDriver<'_, NoLeaseSource>,
    binding: u8,
) -> ConnectionAttempt {
    let Some(ClientAction::Connect { attempt }) = driver.client.poll_time() else {
        panic!("the new attempt must be ready to dial");
    };
    driver.client.dial_started(&attempt).unwrap();
    let now = driver.clock.monotonic_now();
    driver
        .apply(LinkInput::DialStarted {
            attempt: attempt.clone(),
            lease: LeaseWindow {
                binding,
                version: LeaseVersion(1),
                usable_until: now.after(Duration::from_secs(600)),
            },
            now,
        })
        .unwrap();
    attempt
}

pub(super) async fn send_welcome(
    peer: &mut Peer,
    fixture: &HeartbeatFixture,
    lifetime_seconds: u64,
) {
    let central_time = fixture.clock.utc_now();
    let wire = encode_text(
        &LinkFrame::Data(DataFrame::Welcome {
            machine_id: fixture.machine.clone(),
            central_time,
            effective_expires_at: central_time + Duration::from_secs(lifetime_seconds),
        }),
        FrameDirection::ToRouter,
    )
    .unwrap();
    peer.send(Message::Text(wire.into())).await.unwrap();
}

pub(super) async fn read_frame(peer: &mut Peer) -> LinkFrame {
    let Message::Text(text) = peer.next().await.unwrap().unwrap() else {
        panic!("expected a complete ordinary text frame");
    };
    decode_text(&text, FrameDirection::ToCentral).unwrap()
}

pub(super) async fn expect_advertisement(peer: &mut Peer) {
    assert_eq!(
        read_frame(peer).await,
        LinkFrame::Data(DataFrame::MacCapabilities(MacCapabilities {
            handled_calls: HandledMacCalls::default()
        }))
    );
}

async fn expect_discarded_tail(peer: Peer, probe: &SocketProbe) {
    let mut tail = Vec::new();
    peer.into_inner().read_to_end(&mut tail).await.unwrap();
    assert_eq!(
        tail.len(),
        1,
        "only the staged frame's first raw byte may reach the peer"
    );
    assert_eq!(
        probe.close_attempts.get(),
        0,
        "the driver must not attempt Close on a quarantined socket"
    );
    assert_eq!(
        probe.quarantined_ready_polls.get(),
        0,
        "the driver must not poll its quarantined sink to send Close"
    );
}

#[derive(Clone, Copy, Debug)]
enum AnnouncementEnd {
    Timeout,
    SettingsChanged,
    Shutdown,
}

#[tokio::test]
async fn partial_announcement_is_discarded_after_timeout() {
    check_announcement_ending(AnnouncementEnd::Timeout).await;
}

#[tokio::test]
async fn partial_announcement_is_discarded_after_settings_change() {
    check_announcement_ending(AnnouncementEnd::SettingsChanged).await;
}

#[tokio::test]
async fn partial_announcement_is_discarded_after_shutdown() {
    check_announcement_ending(AnnouncementEnd::Shutdown).await;
}

async fn check_announcement_ending(ending: AnnouncementEnd) {
    let fixture = HeartbeatFixture::new();
    let mut driver = fresh_driver(&fixture, 1);
    let attempt = start_attempt(&mut driver, 1);
    let (settings_sender, mut settings) = watch::channel(test_settings());
    let (shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let (lane, mut lane_owner) = link_lane_channel();
    let transport = BufferedFixture::with_clock(fixture.clock.clone(), 1).await;
    let probe = SocketProbe::default();
    let mut peer = WebSocketStream::from_raw_socket(transport.peer, Role::Server, None).await;
    let mut drive = Box::pin(driver.drive_socket(
        ProbedSocket {
            socket: transport.socket,
            clock: &fixture.clock,
            boundary: &transport.boundary,
            probe: &probe,
        },
        &attempt,
        &1,
        SocketAttemptTiming {
            started: elapsed_tick(0),
            writes: &transport.boundary,
        },
        &mut control,
        &mut lane_owner,
    ));
    send_welcome(&mut peer, &fixture, 120).await;
    assert!(drive.as_mut().now_or_never().is_none());
    assert_eq!(transport.crossed.load(Ordering::Relaxed), 1);
    assert!(lane.watch_admitted().borrow().is_none());

    match ending {
        AnnouncementEnd::Timeout => fixture
            .clock
            .advance(
                link_frame_write_deadline(elapsed_tick(0))
                    .saturating_duration_since(elapsed_tick(0)),
            )
            .unwrap(),
        AnnouncementEnd::SettingsChanged => {
            settings_sender.send_replace(DriverSettings {
                binding: Some(2),
                ..test_settings()
            });
        }
        AnnouncementEnd::Shutdown => {
            shutdown_sender.send_replace(true);
        }
    }
    // Teardown must still discard after the old writer becomes writable.
    transport.remaining.store(1024, Ordering::Relaxed);
    let result = drive.as_mut().now_or_never().unwrap();
    drop(drive);
    assert!(
        matches!(
            (ending, result),
            (AnnouncementEnd::Timeout, Ok(()))
                | (
                    AnnouncementEnd::SettingsChanged,
                    Err(Interrupted::SettingsChanged)
                )
                | (AnnouncementEnd::Shutdown, Err(Interrupted::Stopped))
        ),
        "unexpected attempt result for {ending:?}"
    );
    assert!(lane.watch_admitted().borrow().is_none());
    assert!(
        lane.next_event().now_or_never().is_none(),
        "no admission event for {ending:?}"
    );
    assert!(transport.boundary.must_discard());
    expect_discarded_tail(peer, &probe).await;

    let binding = if matches!(ending, AnnouncementEnd::SettingsChanged) {
        2
    } else {
        1
    };
    if matches!(ending, AnnouncementEnd::Timeout) {
        let retry = driver.client.next_deadline().unwrap();
        fixture
            .clock
            .advance(retry.saturating_duration_since(fixture.clock.monotonic_now()))
            .unwrap();
    } else {
        driver = fresh_driver(&fixture, binding);
    }
    // Shutdown ends the old driver. Exercise a fresh owner in that case.
    let (lane, mut lane_owner) = if matches!(ending, AnnouncementEnd::Shutdown) {
        link_lane_channel()
    } else {
        (lane, lane_owner)
    };
    let replacement = start_attempt(&mut driver, binding);
    assert_ne!(replacement, attempt);
    let transport = BufferedFixture::with_clock(fixture.clock.clone(), 1).await;
    let observed = DriverSettings {
        binding: Some(binding),
        ..test_settings()
    };
    let (_settings_sender, mut settings) = watch::channel(observed.clone());
    let (shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed,
    };
    let mut peer = WebSocketStream::from_raw_socket(transport.peer, Role::Server, None).await;
    let mut drive = Box::pin(driver.drive_socket(
        transport.socket,
        &replacement,
        &binding,
        SocketAttemptTiming {
            started: fixture.clock.monotonic_now(),
            writes: &transport.boundary,
        },
        &mut control,
        &mut lane_owner,
    ));
    send_welcome(&mut peer, &fixture, 120).await;
    assert!(drive.as_mut().now_or_never().is_none());
    assert_eq!(transport.crossed.load(Ordering::Relaxed), 1);
    assert!(lane.watch_admitted().borrow().is_none());
    transport.remaining.store(1024, Ordering::Relaxed);
    assert!(drive.as_mut().now_or_never().is_none());
    expect_advertisement(&mut peer).await;
    assert!(matches!(lane.next_event().now_or_never(),
        Some(Some(LinkLaneEvent::ConnectionAdmitted(admitted))) if admitted.connection == replacement
    ));
    shutdown_sender.send_replace(true);
    assert!(matches!(drive.await, Err(Interrupted::Stopped)));
}

#[tokio::test]
async fn started_announcement_cannot_cross_authority_deadline() {
    check_finish_boundary(false, 3).await;
}

#[tokio::test]
async fn started_announcement_cannot_cross_write_deadline() {
    check_finish_boundary(false, 120).await;
}

#[tokio::test]
async fn started_refusal_cannot_cross_authority_deadline() {
    check_finish_boundary(true, 3).await;
}

#[tokio::test]
async fn started_refusal_cannot_cross_write_deadline() {
    check_finish_boundary(true, 120).await;
}

async fn check_finish_boundary(refusal: bool, lifetime_seconds: u64) {
    let fixture = HeartbeatFixture::new();
    let mut driver = fresh_driver(&fixture, 1);
    let attempt = start_attempt(&mut driver, 1);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let (lane, mut lane_owner) = link_lane_channel();
    let transport =
        BufferedFixture::with_clock(fixture.clock.clone(), if refusal { 1024 } else { 1 }).await;
    let probe = SocketProbe::default();
    let mut peer = WebSocketStream::from_raw_socket(transport.peer, Role::Server, None).await;
    let mut drive = Box::pin(driver.drive_socket(
        ProbedSocket {
            socket: transport.socket,
            clock: &fixture.clock,
            boundary: &transport.boundary,
            probe: &probe,
        },
        &attempt,
        &1,
        SocketAttemptTiming {
            started: elapsed_tick(0),
            writes: &transport.boundary,
        },
        &mut control,
        &mut lane_owner,
    ));
    send_welcome(&mut peer, &fixture, lifetime_seconds).await;
    assert!(drive.as_mut().now_or_never().is_none());
    if refusal {
        expect_advertisement(&mut peer).await;
        assert!(matches!(
            lane.next_event().now_or_never(),
            Some(Some(LinkLaneEvent::ConnectionAdmitted(_)))
        ));
        transport.remaining.store(1, Ordering::Relaxed);
        transport.crossed.store(0, Ordering::Relaxed);
        peer.send(Message::Text(UNKNOWN_CALL.into())).await.unwrap();
        assert!(drive.as_mut().now_or_never().is_none());
    }
    assert_eq!(transport.crossed.load(Ordering::Relaxed), 1);

    let finish = elapsed_tick(lifetime_seconds).min(link_frame_write_deadline(elapsed_tick(0)));
    fixture
        .clock
        .advance(finish.saturating_duration_since(elapsed_tick(0)) - Duration::from_nanos(1))
        .unwrap();
    assert!(!fixture.clock.has_not_due_check_now(finish));
    probe.cross_after_pending_timer.set(Some(finish));
    transport.remaining.store(1024, Ordering::Relaxed);
    let outcome = drive.as_mut().now_or_never();
    assert_eq!(fixture.clock.monotonic_now(), finish);
    assert_eq!(
        transport.crossed.load(Ordering::Relaxed),
        1,
        "zero continuation bytes at the finish boundary (refusal={refusal}, authority={lifetime_seconds}s)"
    );
    assert!(matches!(outcome, Some(Ok(()))));
    drop(drive);
    assert!(transport.boundary.must_discard());
    assert!(lane.watch_admitted().borrow().is_none());
    expect_discarded_tail(peer, &probe).await;
}

#[tokio::test]
async fn blocked_refusal_preserves_the_pending_command_deadline_and_discards_its_tail() {
    let fixture = HeartbeatFixture::new();
    let mut driver = fresh_driver(&fixture, 1);
    let attempt = start_attempt(&mut driver, 1);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let (lane, mut lane_owner) = link_lane_channel();
    let transport = BufferedFixture::with_clock(fixture.clock.clone(), 1024).await;
    let probe = SocketProbe::default();
    let mut peer = WebSocketStream::from_raw_socket(transport.peer, Role::Server, None).await;
    let mut drive = Box::pin(driver.drive_socket(
        ProbedSocket {
            socket: transport.socket,
            clock: &fixture.clock,
            boundary: &transport.boundary,
            probe: &probe,
        },
        &attempt,
        &1,
        SocketAttemptTiming {
            started: elapsed_tick(0),
            writes: &transport.boundary,
        },
        &mut control,
        &mut lane_owner,
    ));
    send_welcome(&mut peer, &fixture, 120).await;
    assert!(drive.as_mut().now_or_never().is_none());
    expect_advertisement(&mut peer).await;
    assert!(matches!(
        lane.next_event().now_or_never(),
        Some(Some(LinkLaneEvent::ConnectionAdmitted(_)))
    ));
    let mut command = lane.request_status(RequestStatusCommand {
        connection: attempt.clone(),
        send_before: elapsed_tick(12),
        correlation_id: "01890f2e-7b4c-7cc0-98c4-000000000012".parse().unwrap(),
        caller: serde_json::from_str(r#"{"kind":"session","value":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}}"#).unwrap(),
        query: RequestStatusQuery {
            request_id: "01890f2e-7b4c-7cc0-98c4-000000000013".parse().unwrap(),
            scope: RequestScope::Organization,
        },
    });
    assert!(command.as_mut().now_or_never().is_none());
    assert!(drive.as_mut().now_or_never().is_none());
    assert!(matches!(
        read_frame(&mut peer).await,
        LinkFrame::Data(DataFrame::RequestStatus { .. })
    ));

    let reply_due = link_frame_answer_deadline(elapsed_tick(0), elapsed_tick(0));
    let refusal_started = elapsed_tick(28);
    fixture
        .clock
        .advance(refusal_started.saturating_duration_since(elapsed_tick(0)))
        .unwrap();
    assert!(drive.as_mut().now_or_never().is_none());
    assert_eq!(read_frame(&mut peer).await, LinkFrame::Ping);
    peer.send(Message::Text("pong".into())).await.unwrap();
    assert!(drive.as_mut().now_or_never().is_none());

    transport.remaining.store(1, Ordering::Relaxed);
    transport.crossed.store(0, Ordering::Relaxed);
    peer.send(Message::Text(UNKNOWN_CALL.into())).await.unwrap();
    assert!(drive.as_mut().now_or_never().is_none());
    assert_eq!(transport.crossed.load(Ordering::Relaxed), 1);
    assert!(command.as_mut().now_or_never().is_none());
    fixture
        .clock
        .advance(reply_due.saturating_duration_since(refusal_started))
        .unwrap();
    assert!(drive.as_mut().now_or_never().is_none());
    assert_eq!(
        command.as_mut().now_or_never(),
        Some(LinkLaneDisposition::AnswerLost),
        "the pending command must settle at its own deadline while the refusal is still blocked"
    );
    assert!(lane.watch_admitted().borrow().is_some());
    assert_eq!(transport.crossed.load(Ordering::Relaxed), 1);

    peer.send(Message::Text(UNKNOWN_CALL.into())).await.unwrap();
    fixture
        .clock
        .advance(link_frame_write_deadline(refusal_started).saturating_duration_since(reply_due))
        .unwrap();
    transport.remaining.store(1024, Ordering::Relaxed);
    assert!(matches!(drive.as_mut().now_or_never(), Some(Ok(()))));
    drop(drive);
    assert!(transport.boundary.must_discard());
    assert_eq!(
        lane.next_event().now_or_never(),
        Some(Some(LinkLaneEvent::ConnectionNotAdmitted {
            connection: attempt
        }))
    );
    expect_discarded_tail(peer, &probe).await;
}
