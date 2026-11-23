use std::{sync::Arc, time::Duration};

use central_link_client::{ClientAction, ConnectionAttempt, EnrollmentEnd, LinkClient};
use central_link_protocol::{DataFrame, LinkFrame};
use futures_util::{FutureExt, sink::unfold};
use hyper_util::client::legacy::connect::HttpConnector;
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use tokio::sync::{oneshot, watch};
use tokio_tungstenite::tungstenite::Message;
use workspace_ids::MachineId;

use super::{BindingDriver, Control, DriverSettings, Interrupted, SocketWriteFailure};
use crate::{
    AddressError,
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource},
    link_policy::{LeaseVersion, LinkState, LinkStatus},
    wire_write_fence::default_http_connector,
};

#[path = "renewal_lane_test.rs"]
mod renewal_lane;

struct NoLeaseSource;

impl LeaseSource<u8> for NoLeaseSource {
    fn acquire<'source>(
        &'source self,
        _binding: &'source u8,
        _newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, u8> {
        Box::pin(async { Err(LeaseFailure::Fault) })
    }

    fn end(&self, _binding: &u8, _cause: EnrollmentEnd) {}
}

struct HeartbeatFixture {
    machine: MachineId,
    clock: Arc<ManualClock>,
    status: watch::Sender<LinkStatus>,
}

type WelcomedDriver<'fixture, Source> = BindingDriver<'fixture, u8, Source, fn() -> HttpConnector>;

impl HeartbeatFixture {
    fn new() -> Self {
        Self {
            machine: "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap(),
            clock: Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap())),
            status: watch::channel(LinkStatus::Connecting).0,
        }
    }

    fn welcomed_driver<'fixture, Source>(
        &'fixture self,
        lease_source: &'fixture Source,
    ) -> (WelcomedDriver<'fixture, Source>, ConnectionAttempt) {
        let mut client = LinkClient::new(self.machine.clone(), self.clock.clone());
        let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
            panic!("first connection should be due");
        };
        client.dial_started(&attempt).unwrap();
        client
            .receive(
                &attempt,
                LinkFrame::Data(DataFrame::Welcome {
                    machine_id: self.machine.clone(),
                    central_time: self.clock.utc_now(),
                    effective_expires_at: "2030-01-01T00:05:00Z".parse().unwrap(),
                }),
            )
            .unwrap();
        (
            BindingDriver {
                lease_source,
                make_connector: &(default_http_connector as fn() -> HttpConnector),
                clock: self.clock.clone(),
                status: &self.status,
                address: Err(AddressError::InvalidLocation),
                machine_id: self.machine.clone(),
                state: LinkState::new(self.machine.clone()),
                client,
                lease_retry_at: None,
            },
            attempt,
        )
    }
}

fn test_settings() -> DriverSettings<u8> {
    DriverSettings {
        binding: Some(1),
        address: Err(AddressError::InvalidLocation),
    }
}

fn elapsed_tick(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

#[tokio::test]
async fn both_heartbeat_windows_start_at_their_completed_writes() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };

    for response_deadline in [43, 66] {
        fixture.clock.advance(Duration::from_secs(20)).unwrap();
        assert_eq!(driver.client.poll_time(), Some(ClientAction::SendPing));
        assert_eq!(
            driver.client.next_deadline(),
            None,
            "a queued ping has no response window"
        );
        let (release, wait) = oneshot::channel::<()>();
        let mut held = Some(wait);
        let mut socket = Box::pin(unfold((), |(), message| {
            let wait = held.take().unwrap();
            async move {
                assert_eq!(message, Message::Text("ping".into()));
                wait.await.unwrap();
                Ok::<(), ()>(())
            }
        }));
        let mut write = Box::pin(driver.write_heartbeat(&mut socket, &attempt, &mut control));
        assert!(write.as_mut().now_or_never().is_none());
        fixture.clock.advance(Duration::from_secs(3)).unwrap();
        assert!(write.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        assert!(matches!(write.await, Ok(true)));
        assert_eq!(
            driver.client.next_deadline(),
            Some(elapsed_tick(response_deadline))
        );
    }
    fixture.clock.advance(Duration::from_secs(19)).unwrap();
    assert_eq!(driver.client.poll_time(), None);
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        driver.client.poll_time(),
        Some(ClientAction::Reconnect { .. })
    ));
}

#[tokio::test]
async fn refused_heartbeat_write_retries_without_a_probe_window() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(driver.client.poll_time(), Some(ClientAction::SendPing));
    assert_eq!(
        driver.client.next_deadline(),
        None,
        "writer refusal cannot spend a probe window"
    );
    let mut socket = Box::pin(unfold((), |(), _message| async { Err::<(), ()>(()) }));
    assert!(matches!(
        driver
            .write_heartbeat(&mut socket, &attempt, &mut control)
            .await,
        Ok(false)
    ));
    assert_eq!(driver.client.next_deadline(), Some(elapsed_tick(21)));
}

#[tokio::test]
async fn shutdown_with_a_pending_heartbeat_leaves_no_probe_window() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(driver.client.poll_time(), Some(ClientAction::SendPing));
    let mut socket = Box::pin(unfold((), |(), _message| {
        std::future::pending::<Result<(), ()>>()
    }));
    let mut write = Box::pin(driver.write_heartbeat(&mut socket, &attempt, &mut control));
    assert!(write.as_mut().now_or_never().is_none());
    shutdown_sender.send_replace(true);
    assert!(matches!(write.await, Err(Interrupted::Stopped)));
    assert_eq!(
        driver.client.next_deadline(),
        None,
        "withdrawn write cannot spend a probe window"
    );
}

#[tokio::test]
async fn oversized_renewal_credential_is_rejected_before_writing_a_frame() {
    use crate::socket_upgrade::MAX_CREDENTIAL_BYTES;

    let fixture = HeartbeatFixture::new();
    let (mut driver, _) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let mut socket = Box::pin(unfold((), |(), _message| async { Ok::<(), ()>(()) }));
    let frame = LinkFrame::Data(DataFrame::RenewMachineToken {
        correlation_id: "01890f2e-7b4c-7cc0-98c4-000000000099".parse().unwrap(),
        token: "a".repeat(MAX_CREDENTIAL_BYTES + 1).try_into().unwrap(),
    });
    assert!(
        matches!(
            driver.write(&mut socket, frame, &mut control).await,
            Ok(Err(SocketWriteFailure::SizeLimit))
        ),
        "a renewal credential over the bearer bound must fail before the sink"
    );
}

#[test]
fn correlated_reply_at_the_authority_boundary_cannot_settle_the_writer() {
    use crate::{
        admitted_lane_policy::{ExpectedReply, LaneResult},
        admitted_lane_socket::{AdmittedSocketLane, FrameOffer},
        link_lane::{LaneRequest, link_lane_channel},
        link_policy::{LeaseWindow, LinkInput},
    };
    use central_link_client::{AdmittedConnection, WelcomeClockAnchor};
    use central_link_protocol::RequestStatusAnswer;

    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&NoLeaseSource);
    assert!(driver.apply(LinkInput::BindingAvailable(1)).is_some());
    assert!(
        driver
            .apply(LinkInput::DialStarted {
                attempt: attempt.clone(),
                lease: LeaseWindow {
                    binding: 1,
                    version: LeaseVersion(1),
                    usable_until: elapsed_tick(200)
                },
                now: elapsed_tick(0),
            })
            .is_some()
    );
    assert!(
        driver
            .apply(LinkInput::Welcome {
                attempt: attempt.clone(),
                machine_id: fixture.machine.clone(),
                deadline: elapsed_tick(120),
                now: elapsed_tick(0)
            })
            .is_some()
    );
    let (_user, owner) = link_lane_channel();
    let mut lane = AdmittedSocketLane::new(&owner);
    assert!(lane.admit(AdmittedConnection {
        connection: attempt.clone(),
        clock_anchor: WelcomeClockAnchor {
            central_time: fixture.clock.utc_now(),
            observed_at: elapsed_tick(0)
        }
    }));
    let (settled, mut disposition) = oneshot::channel();
    let correlation_id = "01890f2e-7b4c-7cc0-98c4-000000000090"
        .parse::<central_link_protocol::CorrelationId>()
        .unwrap();
    assert!(matches!(
        lane.offer(
            LaneRequest {
                connection: attempt.clone(),
                send_before: elapsed_tick(119),
                wire: "test".into(),
                expected: ExpectedReply::RequestStatus(correlation_id.clone()),
                settled,
            },
            elapsed_tick(110)
        ),
        FrameOffer::Queued { .. }
    ));
    assert!(lane.start_write());
    lane.finish_write(elapsed_tick(110), elapsed_tick(110));

    fixture.clock.advance(Duration::from_secs(120)).unwrap();
    let reply = LinkFrame::Data(DataFrame::RequestStatusReply {
        correlation_id,
        result: Box::new(RequestStatusAnswer::NotFound),
    });
    assert!(driver.receive_frame(&attempt, reply, elapsed_tick(0), None, &mut lane, None));
    assert_eq!(
        disposition.try_recv(),
        Err(oneshot::error::TryRecvError::Empty),
        "reply cannot settle at authority equality"
    );
    drop(lane);
    assert_eq!(disposition.try_recv(), Ok(LaneResult::SingleLost));
}

#[tokio::test]
async fn first_byte_to_disposition_stays_within_the_exported_limit_during_a_blocked_ping() {
    use std::sync::atomic::Ordering;

    use central_link_client::{
        LINK_LANE_SETTLEMENT_LIMIT, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
        RequestStatusCommand, link_frame_write_deadline,
    };
    use central_link_protocol::{FrameDirection, RequestScope, RequestStatusQuery, encode_text};
    use futures_util::SinkExt;
    use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

    use crate::{
        lane_writer::tests::BufferedFixture,
        link_lane::link_lane_channel,
        link_policy::{LeaseWindow, LinkInput},
    };

    let mut transport = BufferedFixture::new(1).await;
    let fixture = HeartbeatFixture {
        clock: transport.clock.clone(),
        ..HeartbeatFixture::new()
    };
    let (mut driver, _) = fixture.welcomed_driver(&NoLeaseSource);
    driver.client = LinkClient::new(fixture.machine.clone(), fixture.clock.clone());
    let Some(ClientAction::Connect { attempt }) = driver.client.poll_time() else {
        panic!("first connection should be due");
    };
    driver.client.dial_started(&attempt).unwrap();
    driver.apply(LinkInput::BindingAvailable(1)).unwrap();
    driver
        .apply(LinkInput::DialStarted {
            attempt: attempt.clone(),
            lease: LeaseWindow {
                binding: 1,
                version: LeaseVersion(1),
                usable_until: elapsed_tick(600),
            },
            now: elapsed_tick(0),
        })
        .unwrap();
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let (lane, mut lane_owner) = link_lane_channel();
    let mut open = Box::pin(driver.open_socket(
        &mut transport.socket,
        &attempt,
        &1,
        super::SocketAttemptTiming {
            started: elapsed_tick(0),
            writes: &transport.boundary,
        },
        &mut control,
        &mut lane_owner,
    ));
    let mut peer = WebSocketStream::from_raw_socket(transport.peer, Role::Server, None).await;
    let welcome = encode_text(
        &LinkFrame::Data(DataFrame::Welcome {
            machine_id: fixture.machine.clone(),
            central_time: fixture.clock.utc_now(),
            effective_expires_at: "2030-01-01T00:05:00Z".parse().unwrap(),
        }),
        FrameDirection::ToRouter,
    )
    .unwrap();
    peer.send(Message::Text(welcome.into())).await.unwrap();
    assert!(open.as_mut().now_or_never().is_none());
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
    assert!(open.as_mut().now_or_never().is_none());
    assert_eq!(transport.crossed.load(Ordering::Relaxed), 1);
    let first_byte = fixture.clock.monotonic_now();
    let last_flush = link_frame_write_deadline(first_byte).saturating_duration_since(first_byte)
        - Duration::from_nanos(1);
    fixture.clock.advance(last_flush).unwrap();
    transport.remaining.store(1024, Ordering::Relaxed);
    assert!(open.as_mut().now_or_never().is_none());
    let status = futures_util::StreamExt::next(&mut peer)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        status,
        Message::Text(wire) if matches!(
            central_link_protocol::decode_text(wire.as_str(), FrameDirection::ToCentral),
            Ok(LinkFrame::Data(DataFrame::RequestStatus { .. }))
        )
    ));

    let settlement_due = first_byte.after(LINK_LANE_SETTLEMENT_LIMIT);
    fixture
        .clock
        .advance(
            settlement_due.saturating_duration_since(fixture.clock.monotonic_now())
                - Duration::from_secs(1),
        )
        .unwrap();
    transport.remaining.store(0, Ordering::Relaxed);
    let polls = transport.write_polls.load(Ordering::Relaxed);
    assert!(open.as_mut().now_or_never().is_none());
    assert!(
        transport.write_polls.load(Ordering::Relaxed) > polls,
        "the heartbeat must be blocked in a socket write"
    );
    assert!(command.as_mut().now_or_never().is_none());
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    assert!(open.as_mut().now_or_never().is_none());
    assert_eq!(
        command.as_mut().now_or_never(),
        Some(LinkLaneDisposition::AnswerLost),
        "the reply timer settles the written command within the published bound even during a blocked control write"
    );
}
