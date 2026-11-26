use std::{
    pin::{Pin, pin},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use central_link_client::{
    ReadyLinkLane, UntouchedCommand, WriteBatchCommand, WriteBatchDisposition,
    WriteBatchMemberDisposition, link_frame_write_deadline,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, LinkRequest, LinkRequestBatch, ReplyPayload,
    RequestPayload, encode_text,
};
use futures_util::{FutureExt, Sink, SinkExt, Stream};
use hegel::{TestCase, generators as gs};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as SocketError, Message, protocol::Role},
};
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult, NoticeCadence,
};

use super::{
    HeartbeatFixture,
    control_write::{expect_advertisement, fresh_driver, read_frame, send_welcome, start_attempt},
    elapsed_tick, test_settings,
};
use crate::{
    lane_writer::tests::BufferedFixture,
    link_driver::{Control, SocketAttemptTiming},
    link_lane::link_lane_channel,
};

#[path = "work_call_cancellation_test.rs"]
mod work_calls;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteCheckpoint {
    Queued,
    Readiness,
    PriorOutputFlush,
    FirstByte,
    PartialFrame,
    Reply,
}

impl WriteCheckpoint {
    fn has_started(self) -> bool {
        matches!(self, Self::PartialFrame | Self::Reply)
    }
}

#[derive(Clone, Copy, Debug)]
enum Termination {
    Abort,
    Shutdown,
    WindowElapsed,
    FrameWriteTimeout,
    CheckedReplies,
}

#[derive(Default)]
struct SinkGate {
    hold: OnceLock<WriteCheckpoint>,
    observed: AtomicBool,
}

impl SinkGate {
    fn held_at(&self, checkpoint: WriteCheckpoint) -> bool {
        if self.hold.get() == Some(&checkpoint) {
            self.observed.store(true, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
}

// Readiness and the pre-frame flush can both suspend before Tungstenite
// accepts the frame. The controlled transport below handles frame bytes.
struct GatedSocket<Socket> {
    socket: Socket,
    gate: Arc<SinkGate>,
}

impl<Socket: Stream + Unpin> Stream for GatedSocket<Socket> {
    type Item = Socket::Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.socket).poll_next(context)
    }
}

impl<Socket: Sink<Message, Error = SocketError> + Unpin> Sink<Message> for GatedSocket<Socket> {
    type Error = SocketError;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.gate.held_at(WriteCheckpoint::Readiness) {
            return Poll::Pending;
        }
        Pin::new(&mut self.socket).poll_ready(context)
    }

    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.socket).start_send(message)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.gate.held_at(WriteCheckpoint::PriorOutputFlush) {
            return Poll::Pending;
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

#[hegel::test(test_cases = 16)]
fn no_lane_termination_reports_not_reached_after_a_first_byte(case: TestCase) {
    let prefix = case.draw(gs::integers::<usize>().min_value(1).max_value(64));
    let window_seconds = case.draw(gs::integers::<u64>().min_value(1).max_value(4));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use Termination::{Abort, CheckedReplies, FrameWriteTimeout, Shutdown, WindowElapsed};
        use WriteCheckpoint::{
            FirstByte, PartialFrame, PriorOutputFlush, Queued, Readiness, Reply,
        };
        for checkpoint in [
            PartialFrame,
            Queued,
            Readiness,
            PriorOutputFlush,
            FirstByte,
            Reply,
        ] {
            for termination in [Abort, Shutdown] {
                check_termination(checkpoint, termination, prefix, window_seconds).await;
            }
        }
        for checkpoint in [Queued, Readiness, PriorOutputFlush, FirstByte] {
            check_termination(checkpoint, WindowElapsed, prefix, window_seconds).await;
        }
        check_termination(PartialFrame, FrameWriteTimeout, prefix, window_seconds).await;
        check_termination(Reply, CheckedReplies, prefix, window_seconds).await;
    });
}

async fn check_termination(
    checkpoint: WriteCheckpoint,
    termination: Termination,
    prefix: usize,
    window_seconds: u64,
) {
    let BufferedFixture {
        clock,
        boundary,
        socket,
        peer,
        remaining,
        crossed,
        write_polls,
        ..
    } = BufferedFixture::new(1024).await;
    let fixture = HeartbeatFixture {
        clock: clock.clone(),
        ..HeartbeatFixture::new()
    };
    let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
    send_welcome(&mut peer, &fixture, 200).await;

    let (lane, mut owner) = link_lane_channel();
    let (turns, mut driver_turns) = mpsc::unbounded_channel::<oneshot::Sender<bool>>();
    let (shutdown, mut stopping) = watch::channel(false);
    let gate = Arc::new(SinkGate::default());
    let worker_gate = gate.clone();
    let worker_fence = boundary.clone();
    let mut task = tokio::spawn(async move {
        let mut socket = GatedSocket {
            socket,
            gate: worker_gate,
        };
        let mut driver = fresh_driver(&fixture, 1);
        let attempt = start_attempt(&mut driver, 1);
        let (_setting_updates, mut settings) = watch::channel(test_settings());
        let mut control = Control {
            settings: &mut settings,
            shutdown: &mut stopping,
            observed: test_settings(),
        };
        let mut open = pin!(driver.open_socket(
            &mut socket,
            &attempt,
            &1,
            SocketAttemptTiming {
                started: elapsed_tick(0),
                writes: &worker_fence
            },
            &mut control,
            &mut owner,
        ));
        while let Some(turn) = driver_turns.recv().await {
            // The test can abort the actual task while the driver is at this
            // exact await, independently of executor wake timing.
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
    let admitted = lane.watch_admitted().borrow().clone().unwrap();
    crossed.store(0, Ordering::Relaxed);
    let prev_polls = write_polls.load(Ordering::Relaxed);
    remaining.store(
        match checkpoint {
            WriteCheckpoint::Reply => 1024,
            WriteCheckpoint::PartialFrame => prefix,
            _ => 0,
        },
        Ordering::Relaxed,
    );
    if matches!(
        checkpoint,
        WriteCheckpoint::Readiness | WriteCheckpoint::PriorOutputFlush
    ) {
        gate.hold.set(checkpoint).unwrap();
    }
    let requests = batch();
    let mut reply = lane.submit_write_batch(WriteBatchCommand {
        connection: admitted.connection,
        send_before: elapsed_tick(window_seconds),
        requests: requests.clone(),
    });
    assert!(reply.as_mut().now_or_never().is_none());
    if checkpoint != WriteCheckpoint::Queued {
        assert!(poll_driver(&turns).await);
    }
    let accepted_bytes = crossed.load(Ordering::Relaxed);
    assert_eq!(
        accepted_bytes > 0,
        checkpoint.has_started(),
        "{checkpoint:?}"
    );
    match checkpoint {
        WriteCheckpoint::Readiness | WriteCheckpoint::PriorOutputFlush => {
            assert!(gate.observed.load(Ordering::Relaxed));
            assert_eq!(write_polls.load(Ordering::Relaxed), prev_polls);
        }
        WriteCheckpoint::FirstByte | WriteCheckpoint::PartialFrame => {
            assert!(write_polls.load(Ordering::Relaxed) > prev_polls);
            assert_eq!(boundary.crossed(), checkpoint.has_started());
            assert!(reply.as_mut().now_or_never().is_none());
        }
        WriteCheckpoint::Reply => {
            assert!(
                matches!(read_frame(&mut peer).await, LinkFrame::Data(DataFrame::Batch { requests: actual }) if actual == requests)
            );
            assert!(reply.as_mut().now_or_never().is_none());
        }
        WriteCheckpoint::Queued => assert_eq!(write_polls.load(Ordering::Relaxed), prev_polls),
    }

    match termination {
        Termination::Abort => {
            task.abort();
            assert!((&mut task).await.unwrap_err().is_cancelled());
        }
        Termination::Shutdown => {
            shutdown.send_replace(true);
            assert!(!poll_driver(&turns).await);
            (&mut task).await.unwrap();
        }
        Termination::WindowElapsed => {
            clock.advance(Duration::from_secs(window_seconds)).unwrap();
            // A staged WebSocket buffer may require connection retirement.
            poll_driver(&turns).await;
        }
        Termination::FrameWriteTimeout => {
            let deadline = link_frame_write_deadline(elapsed_tick(0));
            clock
                .advance(deadline.saturating_duration_since(elapsed_tick(0)))
                .unwrap();
            assert!(!poll_driver(&turns).await);
        }
        Termination::CheckedReplies => {
            for request in requests.requests() {
                let frame = LinkFrame::Data(DataFrame::Reply {
                    correlation_id: request.correlation_id().clone(),
                    result: Box::new(ReplyPayload::Mail(CentralMailResult::Done(
                        CentralMailAnswer::NoticeCadenceSet {
                            cadence: NoticeCadence::M25,
                        },
                    ))),
                });
                let text = encode_text(&frame, FrameDirection::ToRouter).unwrap();
                peer.send(Message::Text(text.into())).await.unwrap();
                assert!(poll_driver(&turns).await);
            }
        }
    }

    let disposition = reply
        .as_mut()
        .now_or_never()
        .expect("termination settles the queued batch");
    if !matches!(termination, Termination::Abort | Termination::Shutdown) {
        drop(turns);
        task.await.unwrap();
    }
    if accepted_bytes == 0 {
        let cause = if matches!(termination, Termination::WindowElapsed) {
            UntouchedCommand::SendWindowElapsed
        } else {
            UntouchedCommand::ConnectionUnavailable
        };
        assert_eq!(
            disposition,
            WriteBatchDisposition::NotReached { cause },
            "{checkpoint:?} / {termination:?}"
        );
        assert_eq!(crossed.load(Ordering::Relaxed), 0);
        return;
    }

    let WriteBatchDisposition::MemberResults(results) = disposition else {
        panic!(
            "{checkpoint:?} / {termination:?}: {accepted_bytes} accepted transport bytes followed by {disposition:?}"
        );
    };
    assert_eq!(results.members().len(), requests.requests().len());
    for (member, request) in results.members().iter().zip(requests.requests()) {
        assert_eq!(
            Some(&member.request_id),
            request.payload().write_request_id()
        );
        if matches!(termination, Termination::CheckedReplies) {
            assert!(
                matches!(&member.disposition, WriteBatchMemberDisposition::Answered { reply, .. } if &reply.correlation_id == request.correlation_id())
            );
        } else {
            assert!(
                matches!(&member.disposition, WriteBatchMemberDisposition::AnswerLost { correlation_id } if correlation_id == request.correlation_id())
            );
        }
    }
}

async fn poll_driver(turns: &mpsc::UnboundedSender<oneshot::Sender<bool>>) -> bool {
    let (turn, complete) = oneshot::channel();
    turns.send(turn).unwrap();
    complete.await.unwrap()
}

fn batch() -> LinkRequestBatch {
    let caller: workspace_model::Caller = serde_json::from_str(r#"{"kind":"session","value":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}}"#).unwrap();
    LinkRequestBatch::new(
        (1..=2)
            .map(|member| {
                LinkRequest::new(
                    format!("01890f2e-7b4c-7cc0-98c4-{member:012x}")
                        .parse()
                        .unwrap(),
                    caller.clone(),
                    RequestPayload::Mail(CentralMailRequest {
                        request_id: format!("01890f2e-7b4c-7cc0-98c4-00000000001{member}")
                            .parse()
                            .unwrap(),
                        operation: CentralMailOperation::SetNoticeCadence {
                            cadence: NoticeCadence::M25,
                        },
                    }),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}
