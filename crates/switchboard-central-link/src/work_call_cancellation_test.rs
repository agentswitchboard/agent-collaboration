use central_link_protocol::{
    MacWorkCall, MacWorkRpc, MachineLinkCloseCode, session_lifecycle_rpc::EndFleetSessionRpc,
};
use router_clock::{ClockInstant, ManualClock};
use serde_json::json;
use tokio::{io::DuplexStream, task::JoinHandle};

use crate::{
    central_call_lane::{
        AcceptedCentralCall, CallEnding, CallNotReached, CallSettlement, CentralCallSubmission,
    },
    link_lane::LinkLaneHandle,
};

use super::{
    Arc, BufferedFixture, Control, DataFrame, Duration, GatedSocket, HeartbeatFixture, LinkFrame,
    Ordering, ReadyLinkLane, Role, SinkGate, SocketAttemptTiming, WebSocketStream,
    WriteBatchCommand, WriteCheckpoint, batch, elapsed_tick, expect_advertisement, fresh_driver,
    link_lane_channel, mpsc, oneshot, poll_driver, read_frame, send_welcome, start_attempt,
    test_settings, watch,
};
use futures_util::FutureExt;
use futures_util::SinkExt;
use std::{
    pin::pin,
    sync::atomic::{AtomicBool, AtomicUsize},
};
use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame};

struct WorkSocket {
    clock: Arc<ManualClock>,
    lane: LinkLaneHandle,
    peer: WebSocketStream<DuplexStream>,
    write_budget: Arc<AtomicUsize>,
    crossed: Arc<AtomicUsize>,
    transport_failed: Arc<AtomicBool>,
    gate: Arc<SinkGate>,
    turns: mpsc::UnboundedSender<oneshot::Sender<bool>>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl WorkSocket {
    async fn registered() -> Self {
        let BufferedFixture {
            clock,
            boundary,
            socket,
            peer,
            remaining,
            crossed,
            fail,
            ..
        } = BufferedFixture::new(1024).await;
        let fixture = HeartbeatFixture {
            clock: clock.clone(),
            ..HeartbeatFixture::new()
        };
        let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
        send_welcome(&mut peer, &fixture, 200).await;

        let (lane, mut owner) = link_lane_channel();
        let calls = lane.central_calls();
        calls.attach_clock(clock.clone());
        let (turns, mut driver_turns) = mpsc::unbounded_channel::<oneshot::Sender<bool>>();
        let (shutdown, mut stopping) = watch::channel(false);
        let gate = Arc::new(SinkGate::default());
        let transport_gate = gate.clone();
        let task = tokio::spawn(async move {
            let mut socket = GatedSocket {
                socket,
                gate: transport_gate,
            };
            let mut driver = fresh_driver(&fixture, 1);
            let attempt = start_attempt(&mut driver, 1);
            let (_setting_updates, mut settings) = watch::channel(test_settings());
            let mut control = Control {
                settings: &mut settings,
                shutdown: &mut stopping,
                observed: test_settings(),
            };
            let mut open = pin!(async {
                tokio::select! {
                    biased;
                    () = calls.watch_deadlines(fixture.clock.as_ref()) => {},
                    _ = driver.open_socket(&mut socket, &attempt, &1, SocketAttemptTiming { started: elapsed_tick(0), writes: &boundary }, &mut control, &mut owner) => {},
                }
            });
            while let Some(turn) = driver_turns.recv().await {
                let pending = open.as_mut().now_or_never().is_none();
                turn.send(pending).unwrap();
                if !pending {
                    break;
                }
            }
        });
        assert!(poll_driver(&turns).await);
        expect_advertisement(&mut peer).await;
        assert!(poll_driver(&turns).await);
        assert!(lane.watch_registered().borrow().is_some());
        crossed.store(0, Ordering::Relaxed);
        Self {
            clock,
            lane,
            peer,
            write_budget: remaining,
            crossed,
            transport_failed: fail,
            gate,
            turns,
            shutdown,
            task,
        }
    }

    async fn stop(self) {
        drop(self.turns);
        self.task.await.unwrap();
    }
}

fn end_call() -> MacWorkCall {
    EndFleetSessionRpc::work_request(
        serde_json::from_value(json!("019f0000-0000-7000-8000-000000000001")).unwrap(),
        serde_json::from_value(json!({"target":{"kind":"centralId","sessionId":"019f0000-0000-7000-8000-000000000002"},"cause":{"kind":"givenUp"}})).unwrap(),
    )
}

fn submit(lane: &LinkLaneHandle, deadline: ClockInstant) -> AcceptedCentralCall {
    let CentralCallSubmission::Accepted(receipt) =
        lane.central_calls().submit(end_call(), deadline)
    else {
        panic!("work call was not accepted")
    };
    receipt
}

#[derive(Clone, Copy)]
enum EndInFlight {
    Deadline,
    Shutdown,
    DropDriver,
    TransportFailure,
}

#[tokio::test]
async fn work_cancellation_preserves_the_first_byte_observation() {
    for phase in [
        WriteCheckpoint::Queued,
        WriteCheckpoint::Readiness,
        WriteCheckpoint::FirstByte,
        WriteCheckpoint::PartialFrame,
        WriteCheckpoint::Reply,
    ] {
        for end in [
            EndInFlight::Deadline,
            EndInFlight::Shutdown,
            EndInFlight::DropDriver,
            EndInFlight::TransportFailure,
        ] {
            if matches!(end, EndInFlight::TransportFailure)
                && !matches!(
                    phase,
                    WriteCheckpoint::FirstByte | WriteCheckpoint::PartialFrame
                )
            {
                continue;
            }
            let mut fixture = WorkSocket::registered().await;
            fixture.write_budget.store(
                match phase {
                    WriteCheckpoint::Reply => 1024,
                    WriteCheckpoint::PartialFrame => 1,
                    _ => 0,
                },
                Ordering::Relaxed,
            );
            if phase == WriteCheckpoint::Readiness {
                fixture.gate.hold.set(phase).unwrap();
            }
            let receipt = submit(&fixture.lane, elapsed_tick(2));
            if phase != WriteCheckpoint::Queued {
                assert!(poll_driver(&fixture.turns).await);
            }
            if phase == WriteCheckpoint::Reply {
                assert!(matches!(
                    read_frame(&mut fixture.peer).await,
                    LinkFrame::Data(DataFrame::CallCentral { .. })
                ));
            }
            assert_eq!(
                fixture.crossed.load(Ordering::Relaxed) > 0,
                phase.has_started()
            );
            let cause = match end {
                EndInFlight::Deadline => {
                    fixture.clock.advance(Duration::from_secs(2)).unwrap();
                    poll_driver(&fixture.turns).await;
                    CallEnding::Deadline
                }
                EndInFlight::Shutdown => {
                    fixture.shutdown.send_replace(true);
                    assert!(!poll_driver(&fixture.turns).await);
                    CallEnding::Shutdown
                }
                EndInFlight::DropDriver => {
                    fixture.task.abort();
                    assert!((&mut fixture.task).await.unwrap_err().is_cancelled());
                    CallEnding::DriverDropped
                }
                EndInFlight::TransportFailure => {
                    fixture.transport_failed.store(true, Ordering::Relaxed);
                    assert!(!poll_driver(&fixture.turns).await);
                    CallEnding::TransportLost
                }
            };
            let outcome = receipt.resolve().now_or_never().unwrap();
            if phase.has_started() {
                assert!(matches!(outcome, CallSettlement::AnswerLost(actual) if actual == cause));
            } else {
                assert!(
                    matches!(outcome, CallSettlement::NotReached(CallNotReached::Ended(actual)) if actual == cause)
                );
            }
            if !matches!(end, EndInFlight::DropDriver) {
                fixture.stop().await;
            }
        }
    }
}

#[tokio::test]
async fn queued_work_frees_its_slots_while_custody_still_holds_the_writer() {
    let fixture = WorkSocket::registered().await;
    let admitted = fixture
        .lane
        .watch_admitted()
        .borrow()
        .as_ref()
        .unwrap()
        .connection
        .clone();
    fixture.gate.hold.set(WriteCheckpoint::Readiness).unwrap();
    let mut custody = fixture.lane.submit_write_batch(WriteBatchCommand {
        connection: admitted,
        send_before: elapsed_tick(30),
        requests: batch(),
    });
    assert!(custody.as_mut().now_or_never().is_none());
    assert!(poll_driver(&fixture.turns).await);
    assert!(fixture.gate.observed.load(Ordering::Relaxed));

    let queued: Vec<_> = (0..8)
        .map(|_| submit(&fixture.lane, elapsed_tick(1)))
        .collect();
    assert!(matches!(
        fixture
            .lane
            .central_calls()
            .submit(end_call(), elapsed_tick(15)),
        CentralCallSubmission::NotReached(CallNotReached::LaneFull)
    ));
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    assert!(poll_driver(&fixture.turns).await);
    assert!(custody.as_mut().now_or_never().is_none());
    for receipt in queued {
        assert!(matches!(
            receipt.resolve().now_or_never(),
            Some(CallSettlement::NotReached(CallNotReached::Ended(
                CallEnding::Deadline
            )))
        ));
    }
    let replacement = submit(&fixture.lane, elapsed_tick(15));
    assert_eq!(fixture.crossed.load(Ordering::Relaxed), 0);
    drop(custody);
    fixture.stop().await;
    assert!(matches!(
        replacement.resolve().await,
        CallSettlement::NotReached(CallNotReached::Ended(CallEnding::DriverDropped))
    ));
}

#[tokio::test]
async fn close_retires_unwritten_and_written_work_with_the_same_owning_cause() {
    for code in [None, Some(MachineLinkCloseCode::Revoked)] {
        let mut fixture = WorkSocket::registered().await;
        let written = submit(&fixture.lane, elapsed_tick(5));
        assert!(poll_driver(&fixture.turns).await);
        assert!(matches!(
            read_frame(&mut fixture.peer).await,
            LinkFrame::Data(DataFrame::CallCentral { .. })
        ));
        let queued = submit(&fixture.lane, elapsed_tick(5));
        fixture
            .peer
            .send(Message::Close(code.map(|code| CloseFrame {
                code: code.code().into(),
                reason: "".into(),
            })))
            .await
            .unwrap();
        assert!(!poll_driver(&fixture.turns).await);
        let cause = code.map_or(CallEnding::CarrierClosed(None), |_| {
            CallEnding::EnrollmentEnded(central_link_client::EnrollmentEnd::Revoked)
        });
        assert!(
            matches!(written.resolve().await, CallSettlement::AnswerLost(actual) if actual == cause)
        );
        assert!(
            matches!(queued.resolve().await, CallSettlement::NotReached(CallNotReached::Ended(actual)) if actual == cause)
        );
        fixture.stop().await;
    }
}
