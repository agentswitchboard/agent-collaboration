use central_link_client::{
    LinkLaneDisposition, SubmitReceiptsCommand, WriteBatchDisposition, WriteBatchMemberDisposition,
};
use central_link_protocol::{
    FrameDirection, MacWorkCall, MacWorkRpc, MachineLinkCloseCode, Receipt, ReceiptBatch,
    ReceiptKind, ReceiptResult, ReplyPayload, encode_text,
    session_lifecycle_rpc::EndFleetSessionRpc,
};
use router_clock::{ClockInstant, ManualClock};
use serde_json::json;
use tokio::{io::DuplexStream, task::JoinHandle};

use crate::{
    central_call_lane::{
        AcceptedCentralCall, CallEnding, CallNotReached, CallSettlement, CentralCallSubmission,
    },
    lease_source::{ApprovalServiceTarget, LeaseFailure, LeaseFuture, LeaseSource, LinkLease},
    link_driver::DriverSettings,
    link_lane::LinkLaneHandle,
    link_policy::{LeaseVersion, LeaseWindow},
};

use super::super::control_write::fresh_driver_with_lease;
use super::{
    Arc, BufferedFixture, Control, DataFrame, Duration, GatedSocket, HeartbeatFixture, LinkFrame,
    Ordering, ReadyLinkLane, Role, SinkGate, SocketAttemptTiming, WebSocketStream,
    WriteBatchCommand, WriteCheckpoint, batch, elapsed_tick, expect_advertisement,
    link_lane_channel, mpsc, oneshot, poll_driver, read_frame, send_welcome, start_attempt,
    test_settings, watch,
};
use futures_util::FutureExt;
use futures_util::{SinkExt, StreamExt};
use std::{
    pin::pin,
    sync::atomic::{AtomicBool, AtomicUsize},
};
use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame};
use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult, NoticeCadence};

#[path = "work_call_review_test.rs"]
mod review;

struct WorkSocket {
    clock: Arc<ManualClock>,
    lane: LinkLaneHandle,
    peer: WebSocketStream<DuplexStream>,
    write_budget: Arc<AtomicUsize>,
    crossed: Arc<AtomicUsize>,
    write_polls: Arc<AtomicUsize>,
    transport_failed: Arc<AtomicBool>,
    gate: Arc<SinkGate>,
    turns: mpsc::UnboundedSender<oneshot::Sender<bool>>,
    shutdown: watch::Sender<bool>,
    settings: watch::Sender<DriverSettings<u8>>,
    renewed_from: mpsc::UnboundedReceiver<LeaseVersion>,
    task: JoinHandle<()>,
}

impl WorkSocket {
    async fn registered() -> Self {
        Self::registered_with_lease_lifetime(200).await
    }

    async fn registered_with_lease_lifetime(lifetime_seconds: u64) -> Self {
        let BufferedFixture {
            clock,
            boundary,
            socket,
            peer,
            remaining,
            crossed,
            write_polls,
            fail,
            ..
        } = BufferedFixture::new(1024).await;
        let fixture = HeartbeatFixture {
            clock: clock.clone(),
            ..HeartbeatFixture::new()
        };
        let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
        send_welcome(&mut peer, &fixture, lifetime_seconds).await;

        let (lane, mut owner) = link_lane_channel();
        let calls = lane.central_calls();
        calls.attach_clock(clock.clone());
        let (turns, mut driver_turns) = mpsc::unbounded_channel::<oneshot::Sender<bool>>();
        let (shutdown, mut stopping) = watch::channel(false);
        let (settings, mut updates) = watch::channel(test_settings());
        let (renewals, renewed_from) = mpsc::unbounded_channel();
        let gate = Arc::new(SinkGate::default());
        let transport_gate = gate.clone();
        let task = tokio::spawn(async move {
            let mut socket = GatedSocket {
                socket,
                gate: transport_gate,
            };
            let renewal_source = WorkRenewalSource(renewals);
            let mut driver = fresh_driver_with_lease(&fixture, 1, &renewal_source);
            let attempt = start_attempt(&mut driver, 1);
            let mut control = Control {
                settings: &mut updates,
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
            write_polls,
            transport_failed: fail,
            gate,
            turns,
            shutdown,
            settings,
            renewed_from,
            task,
        }
    }

    async fn stop(self) {
        drop(self.turns);
        self.task.await.unwrap();
    }
}

struct WorkRenewalSource(mpsc::UnboundedSender<LeaseVersion>);

impl LeaseSource<u8> for WorkRenewalSource {
    fn registration_target(&self, _binding: &u8) -> Result<ApprovalServiceTarget, LeaseFailure> {
        Ok(ApprovalServiceTarget {
            organization_id: "019f0000-0000-7000-8000-000000000001"
                .to_owned()
                .try_into()
                .map_err(|_| LeaseFailure::Fault)?,
        })
    }

    fn acquire<'source>(
        &'source self,
        binding: &'source u8,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, u8> {
        Box::pin(async move {
            let prior = newer_than.unwrap();
            self.0.send(prior).unwrap();
            Ok(LinkLease {
                window: LeaseWindow {
                    binding: *binding,
                    version: LeaseVersion(2),
                    usable_until: elapsed_tick(600),
                },
                token: "renewed.synthetic.credential"
                    .to_owned()
                    .try_into()
                    .unwrap(),
            })
        })
    }

    fn end(&self, _binding: &u8, _cause: central_link_client::EnrollmentEnd) {}
}

fn end_call() -> MacWorkCall {
    end_call_for_session("019f0000-0000-7000-8000-000000000002")
}

fn end_call_for_session(session_id: &str) -> MacWorkCall {
    EndFleetSessionRpc::work_request(
        serde_json::from_value(json!("019f0000-0000-7000-8000-000000000001")).unwrap(),
        serde_json::from_value(json!({"target":{"kind":"centralId","sessionId":session_id},"cause":{"kind":"givenUp"}})).unwrap(),
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
    SettingsChanged,
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
            EndInFlight::SettingsChanged,
        ] {
            if matches!(
                end,
                EndInFlight::TransportFailure | EndInFlight::SettingsChanged
            ) && !matches!(
                phase,
                WriteCheckpoint::FirstByte | WriteCheckpoint::PartialFrame
            ) {
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
            let polls_before_work = fixture.write_polls.load(Ordering::Relaxed);
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
            if matches!(
                phase,
                WriteCheckpoint::FirstByte | WriteCheckpoint::PartialFrame
            ) {
                assert!(fixture.write_polls.load(Ordering::Relaxed) > polls_before_work);
                assert_eq!(
                    fixture.crossed.load(Ordering::Relaxed),
                    usize::from(phase == WriteCheckpoint::PartialFrame)
                );
            }
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
                EndInFlight::SettingsChanged => {
                    fixture
                        .settings
                        .send_modify(|settings| settings.binding = Some(2));
                    assert!(!poll_driver(&fixture.turns).await);
                    CallEnding::SettingsChanged
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
    let mut fixture = WorkSocket::registered().await;
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
    let CentralCallSubmission::Accepted(replacement) = fixture.lane.central_calls().submit(
        EndFleetSessionRpc::work_request(
            serde_json::from_value(json!("019f0000-0000-7000-8000-000000000001")).unwrap(),
            serde_json::from_value(json!({
                "target":{"kind":"centralId","sessionId":"019f0000-0000-7000-8000-000000000003"},
                "cause":{"kind":"givenUp"}
            }))
            .unwrap(),
        ),
        elapsed_tick(15),
    ) else {
        panic!("a slot must be available before the writer is released");
    };
    assert_eq!(fixture.crossed.load(Ordering::Relaxed), 0);
    fixture.write_budget.store(4096, Ordering::Relaxed);
    fixture.gate.released.store(true, Ordering::Relaxed);
    assert!(poll_driver(&fixture.turns).await);
    assert!(matches!(
        read_frame(&mut fixture.peer).await,
        LinkFrame::Data(DataFrame::Batch { .. })
    ));
    let LinkFrame::Data(DataFrame::CallCentral { request, .. }) =
        read_frame(&mut fixture.peer).await
    else {
        panic!("the replacement must cross the wire after custody");
    };
    let central_link_protocol::CentralCall::EndFleetSession(request) = *request else {
        panic!("the replacement is the only End operation that can be sent");
    };
    assert_eq!(
        serde_json::to_value(request.body).unwrap()["target"]["sessionId"],
        "019f0000-0000-7000-8000-000000000003"
    );
    assert!(fixture.peer.next().now_or_never().is_none());
    drop(custody);
    fixture.stop().await;
    assert!(matches!(
        replacement.resolve().await,
        CallSettlement::AnswerLost(CallEnding::DriverDropped)
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

#[tokio::test]
async fn eight_written_work_calls_and_custody_batch_and_receipts_make_independent_progress() {
    let mut fixture = WorkSocket::registered().await;
    fixture.write_budget.store(4096, Ordering::Relaxed);
    let calls: Vec<_> = (0..8)
        .map(|_| submit(&fixture.lane, elapsed_tick(15)))
        .collect();
    let mut work_correlations = Vec::new();
    for _ in &calls {
        assert!(poll_driver(&fixture.turns).await);
        let Some(LinkFrame::Data(DataFrame::CallCentral { correlation_id, .. })) =
            read_frame(&mut fixture.peer).now_or_never()
        else {
            panic!("all eight work calls must be written and awaiting replies");
        };
        work_correlations.push(correlation_id);
    }
    let connection = calls.first().unwrap().connection().clone();
    let requests = batch();
    let mut custody = fixture.lane.submit_write_batch(WriteBatchCommand {
        connection: connection.clone(),
        send_before: elapsed_tick(15),
        requests: requests.clone(),
    });
    assert!(custody.as_mut().now_or_never().is_none());
    assert!(poll_driver(&fixture.turns).await);
    assert!(matches!(read_frame(&mut fixture.peer).await,
        LinkFrame::Data(DataFrame::Batch { requests: actual }) if actual == requests));

    let mut calls = calls.into_iter();
    let first = calls.next().unwrap();
    let answer = LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id: work_correlations.first().unwrap().clone(),
        result: Box::new(central_link_protocol::TypedCallResult::Answered {
            reply: serde_json::from_value(json!({
                "call":"endFleetSession",
                "envelope":{"target":"019f0000-0000-7000-8000-000000000001","body":{"result":"noSuchStart"}}
            })).unwrap(),
        }),
    });
    fixture
        .peer
        .send(Message::Text(
            encode_text(&answer, FrameDirection::ToRouter)
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    assert!(poll_driver(&fixture.turns).await);
    assert!(matches!(
        first.resolve().now_or_never(),
        Some(CallSettlement::Answered(_))
    ));
    assert!(custody.as_mut().now_or_never().is_none());
    let replacement = submit(&fixture.lane, elapsed_tick(15));
    assert!(poll_driver(&fixture.turns).await);
    assert!(matches!(
        read_frame(&mut fixture.peer).await,
        LinkFrame::Data(DataFrame::CallCentral { .. })
    ));

    for request in requests.requests() {
        let answer = LinkFrame::Data(DataFrame::Reply {
            correlation_id: request.correlation_id().clone(),
            result: Box::new(ReplyPayload::Mail(CentralMailResult::Done(
                CentralMailAnswer::NoticeCadenceSet {
                    cadence: NoticeCadence::M25,
                },
            ))),
        });
        fixture
            .peer
            .send(Message::Text(
                encode_text(&answer, FrameDirection::ToRouter)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
    }
    assert!(poll_driver(&fixture.turns).await);
    assert!(
        matches!(custody.now_or_never(), Some(WriteBatchDisposition::MemberResults(results))
        if results.members().iter().all(|member| matches!(member.disposition, WriteBatchMemberDisposition::Answered { .. })))
    );

    let correlation: central_link_protocol::CorrelationId =
        "019f0000-0000-7000-8000-000000000015".parse().unwrap();
    let mut custody_receipt = fixture.lane.submit_receipts(SubmitReceiptsCommand {
        connection,
        send_before: elapsed_tick(15),
        correlation_id: correlation.clone(),
        receipts: ReceiptBatch::new(vec![Receipt {
            message_id: "019f0000-0000-7000-8000-000000000016".parse().unwrap(),
            kind: ReceiptKind::Seen {
                completed_at: "2030-01-01T00:00:00Z".parse().unwrap(),
            },
        }])
        .unwrap(),
    });
    assert!(custody_receipt.as_mut().now_or_never().is_none());
    assert!(poll_driver(&fixture.turns).await);
    assert!(matches!(read_frame(&mut fixture.peer).await,
        LinkFrame::Data(DataFrame::Receipts { correlation_id, .. }) if correlation_id == correlation));
    let reply = LinkFrame::Data(DataFrame::ReceiptResults {
        correlation_id: correlation,
        results: vec![ReceiptResult::Recorded],
    });
    fixture
        .peer
        .send(Message::Text(
            encode_text(&reply, FrameDirection::ToRouter)
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    assert!(poll_driver(&fixture.turns).await);
    assert!(
        matches!(custody_receipt.now_or_never(), Some(LinkLaneDisposition::Answered { reply, .. }) if reply.results == [ReceiptResult::Recorded])
    );
    fixture.stop().await;
    for awaiting in calls.chain([replacement]) {
        assert!(matches!(
            awaiting.resolve().await,
            CallSettlement::AnswerLost(CallEnding::DriverDropped)
        ));
    }
}
