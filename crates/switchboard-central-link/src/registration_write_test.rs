use std::{
    cell::{Cell, RefCell},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use central_link_client::{LinkLaneEvent, ReadyLinkLane, UntouchedCommand};
use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkFrame, decode_text,
};
use futures_util::{FutureExt, Sink, Stream};
use router_clock::RuntimeClock;
use tokio::{io::AsyncReadExt, sync::watch};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error, Message, protocol::Role},
};
use workspace_ids::RouterRunId;

use super::control_write::{
    complete_registration, fresh_driver, read_frame, send_welcome, start_attempt,
};
use super::{Control, HeartbeatFixture, elapsed_tick, test_settings};
use crate::{
    lane_writer::tests::BufferedFixture,
    link_driver::SocketAttemptTiming,
    link_lane::link_lane_channel,
    link_policy::{LinkStatus, RegistrationFailure},
};

#[derive(Clone, Copy)]
enum FaultAt {
    BeforeStaging,
    StagedWithoutBytes,
    AfterFirstByte,
}

struct WriteProbe {
    fault_at: Option<FaultAt>,
    block_before_staging: Cell<bool>,
    capabilities_staged: Cell<bool>,
    bytes_allowed: Arc<AtomicUsize>,
    registration_frames: RefCell<Vec<(CorrelationId, RouterRunId)>>,
}

struct MeasuredSocket<'probe, Socket> {
    probe: &'probe WriteProbe,
    socket: Socket,
}

impl<Socket: Stream + Unpin> Stream for MeasuredSocket<'_, Socket> {
    type Item = Socket::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.socket).poll_next(cx)
    }
}

impl<Socket: Sink<Message, Error = Error> + Unpin> Sink<Message> for MeasuredSocket<'_, Socket> {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.probe.capabilities_staged.get() && self.probe.block_before_staging.get() {
            return Poll::Pending;
        }
        Pin::new(&mut self.socket).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        if let Message::Text(text) = &item {
            match decode_text(text.as_str(), FrameDirection::ToCentral)
                .map_err(std::io::Error::other)?
            {
                LinkFrame::Data(DataFrame::MacCapabilities(_)) => {
                    self.probe.capabilities_staged.set(true);
                }
                LinkFrame::Data(DataFrame::CallCentral {
                    correlation_id,
                    request,
                }) => {
                    let CentralCall::RegisterRun(registration) = *request else {
                        return Err(
                            std::io::Error::other("expected a typed registration request").into(),
                        );
                    };
                    let mut frames = self.probe.registration_frames.borrow_mut();
                    if frames.is_empty() {
                        match self.probe.fault_at {
                            Some(FaultAt::StagedWithoutBytes) => {
                                self.probe.bytes_allowed.store(0, Ordering::Relaxed)
                            }
                            Some(FaultAt::AfterFirstByte) => {
                                self.probe.bytes_allowed.store(1, Ordering::Relaxed)
                            }
                            Some(FaultAt::BeforeStaging) | None => {}
                        }
                    }
                    frames.push((correlation_id, registration.body.router_run));
                }
                _ => {}
            }
        }
        Pin::new(&mut self.socket).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.socket).poll_close(cx)
    }
}

#[tokio::test]
async fn a_registration_held_before_staging_reuses_its_socket_after_the_retry_wait() {
    check_registration_writer(FaultAt::BeforeStaging).await;
}

#[tokio::test]
async fn a_staged_registration_with_zero_bytes_discards_the_socket_without_flushing() {
    check_registration_writer(FaultAt::StagedWithoutBytes).await;
}

#[tokio::test]
async fn a_registration_that_fails_after_the_first_byte_discards_the_exact_socket() {
    check_registration_writer(FaultAt::AfterFirstByte).await;
}

async fn check_registration_writer(fault_at: FaultAt) {
    let fixture = HeartbeatFixture::new();
    let (lane, mut owner) = link_lane_channel();
    let mut driver = fresh_driver(&fixture, 1);
    let attempt = start_attempt(&mut driver, 1);
    let (_settings_tx, mut settings) = watch::channel(test_settings());
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let BufferedFixture {
        socket,
        peer,
        boundary,
        remaining,
        crossed,
        fail,
        ..
    } = BufferedFixture::with_clock(fixture.clock.clone(), 1024).await;
    let probe = WriteProbe {
        fault_at: Some(fault_at),
        block_before_staging: Cell::new(true),
        capabilities_staged: Cell::default(),
        bytes_allowed: remaining.clone(),
        registration_frames: RefCell::default(),
    };
    let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
    let mut drive = Box::pin(driver.drive_socket(
        MeasuredSocket {
            probe: &probe,
            socket,
        },
        &attempt,
        &1,
        SocketAttemptTiming {
            started: elapsed_tick(0),
            writes: &boundary,
        },
        &mut control,
        &mut owner,
    ));
    send_welcome(&mut peer, &fixture, 120).await;
    assert!(drive.as_mut().now_or_never().is_none());
    assert!(matches!(
        read_frame(&mut peer).await,
        LinkFrame::Data(DataFrame::MacCapabilities(_))
    ));
    assert!(matches!(
        lane.next_event().now_or_never(),
        Some(Some(LinkLaneEvent::ConnectionAdmitted(_)))
    ));
    assert!(lane.watch_registered().borrow().is_none());
    assert!(probe.registration_frames.borrow().is_empty());
    if !matches!(fault_at, FaultAt::BeforeStaging) {
        crossed.store(0, Ordering::Relaxed);
        probe.block_before_staging.set(false);
        assert!(drive.as_mut().now_or_never().is_none());
        assert_eq!(probe.registration_frames.borrow().len(), 1);
        assert_eq!(
            crossed.load(Ordering::Relaxed),
            usize::from(matches!(fault_at, FaultAt::AfterFirstByte))
        );
    }

    if matches!(fault_at, FaultAt::AfterFirstByte) {
        assert!(boundary.crossed());
        fail.store(true, Ordering::Relaxed);
    } else {
        assert!(!boundary.crossed());
        fixture.clock.advance(Duration::from_secs(5)).unwrap();
    }

    if matches!(fault_at, FaultAt::BeforeStaging) {
        assert!(probe.registration_frames.borrow().is_empty());
        assert!(drive.as_mut().now_or_never().is_none());
        assert_eq!(
            *fixture.status.borrow(),
            LinkStatus::RegistrationFailed {
                reason: RegistrationFailure::NotWritten(UntouchedCommand::SendWindowElapsed),
            }
        );
        probe.block_before_staging.set(false);
        fixture
            .clock
            .advance(Duration::from_secs(1) - Duration::from_nanos(1))
            .unwrap();
        assert!(drive.as_mut().now_or_never().is_none());
        assert!(probe.registration_frames.borrow().is_empty());
        fixture.clock.advance(Duration::from_nanos(1)).unwrap();
        assert!(drive.as_mut().now_or_never().is_none());
        complete_registration(&mut peer).await;
        assert!(drive.as_mut().now_or_never().is_none());
        let registered = lane.watch_registered().borrow().clone().unwrap();
        assert_eq!(registered.admitted().connection, attempt);
        assert_eq!(probe.registration_frames.borrow().len(), 1);
        shutdown_tx.send_replace(true);
        let _ = drive.await;
        return;
    }

    assert!(matches!(drive.as_mut().now_or_never(), Some(Ok(()))));
    drop(drive);
    assert!(boundary.must_discard());
    assert!(lane.watch_registered().borrow().is_none());
    assert!(lane.watch_admitted().borrow().is_none());
    assert_eq!(*fixture.status.borrow(), LinkStatus::Connecting);
    remaining.store(1024, Ordering::Relaxed);
    fail.store(false, Ordering::Relaxed);
    let mut tail = Vec::new();
    peer.into_inner().read_to_end(&mut tail).await.unwrap();
    assert_eq!(
        tail.len(),
        usize::from(matches!(fault_at, FaultAt::AfterFirstByte))
    );
    let [(first_correlation, first_run)] =
        probe.registration_frames.into_inner().try_into().unwrap();

    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let replacement = start_attempt(&mut driver, 1);
    assert_ne!(replacement, attempt);
    let now = fixture.clock.monotonic_now();
    let BufferedFixture {
        socket,
        peer,
        boundary,
        remaining,
        ..
    } = BufferedFixture::with_clock(fixture.clock.clone(), 1024).await;
    let healthy = WriteProbe {
        fault_at: None,
        block_before_staging: Cell::default(),
        capabilities_staged: Cell::default(),
        bytes_allowed: remaining,
        registration_frames: RefCell::default(),
    };
    let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
    let mut drive = Box::pin(driver.drive_socket(
        MeasuredSocket {
            probe: &healthy,
            socket,
        },
        &replacement,
        &1,
        SocketAttemptTiming {
            started: now,
            writes: &boundary,
        },
        &mut control,
        &mut owner,
    ));
    send_welcome(&mut peer, &fixture, 120).await;
    assert!(drive.as_mut().now_or_never().is_none());
    assert!(matches!(
        read_frame(&mut peer).await,
        LinkFrame::Data(DataFrame::MacCapabilities(_))
    ));
    complete_registration(&mut peer).await;
    assert!(drive.as_mut().now_or_never().is_none());
    {
        let observed = healthy.registration_frames.borrow();
        assert_eq!(observed.len(), 1);
        assert_ne!(observed[0].0, first_correlation);
        assert_eq!(observed[0].1, first_run);
    }
    let registered = lane.watch_registered().borrow().clone().unwrap();
    assert_eq!(registered.admitted().connection, replacement);
    assert_eq!(registered.router_run(), &first_run);
    shutdown_tx.send_replace(true);
    let _ = drive.await;
}

#[tokio::test]
async fn a_fitting_acknowledgement_and_its_elapsed_timer_ready_together_time_out() {
    use futures_util::SinkExt as _;

    let fixture = HeartbeatFixture::new();
    let (lane, mut owner) = link_lane_channel();
    let mut driver = fresh_driver(&fixture, 1);
    let attempt = start_attempt(&mut driver, 1);
    let (_settings_tx, mut settings) = watch::channel(test_settings());
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let BufferedFixture {
        socket,
        peer,
        boundary,
        ..
    } = BufferedFixture::with_clock(fixture.clock.clone(), 1024).await;
    let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
    let mut drive = Box::pin(driver.drive_socket(
        socket,
        &attempt,
        &1,
        SocketAttemptTiming {
            started: elapsed_tick(0),
            writes: &boundary,
        },
        &mut control,
        &mut owner,
    ));
    send_welcome(&mut peer, &fixture, 120).await;
    assert!(drive.as_mut().now_or_never().is_none());
    assert!(matches!(
        read_frame(&mut peer).await,
        LinkFrame::Data(DataFrame::MacCapabilities(_))
    ));
    assert_eq!(*fixture.status.borrow(), LinkStatus::Registering);
    assert!(lane.watch_registered().borrow().is_none());

    // Queue the fitting answer on the exact transport without polling its owner.
    complete_registration(&mut peer).await;
    fixture.clock.advance(Duration::from_secs(30)).unwrap();
    assert!(drive.as_mut().now_or_never().is_none());
    assert_eq!(
        *fixture.status.borrow(),
        LinkStatus::RegistrationFailed {
            reason: RegistrationFailure::TimedOut
        }
    );
    assert!(lane.watch_registered().borrow().is_none());
    assert_eq!(read_frame(&mut peer).await, LinkFrame::Ping);
    peer.send(Message::Text("pong".into())).await.unwrap();
    shutdown_tx.send_replace(true);
    let _ = drive.await;
}
