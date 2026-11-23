use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures_util::FutureExt;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_util::rt::TokioIo;
use router_clock::ManualClock;
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

use super::*;
use crate::{link_driver::finish_socket, wire_write_fence::FencedTransport};

#[derive(Clone, Copy)]
enum SinkCase {
    Pending,
    ReadyAt(Duration),
    RefuseReadiness,
    RejectBeforeBuffer,
}

struct TestSink<'clock> {
    clock: &'clock ManualClock,
    case: SinkCase,
    entered_send: bool,
}

impl Sink<Message> for TestSink<'_> {
    type Error = SocketError;
    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        match self.case {
            SinkCase::Pending => Poll::Pending,
            SinkCase::ReadyAt(elapsed) => {
                self.clock
                    .advance(elapsed)
                    .map_err(|_| io::Error::other("test clock"))?;
                Poll::Ready(Ok(()))
            }
            SinkCase::RefuseReadiness => Poll::Ready(Err(io::Error::other("sink declined").into())),
            SinkCase::RejectBeforeBuffer => Poll::Ready(Ok(())),
        }
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.entered_send = true;
        if matches!(self.case, SinkCase::RejectBeforeBuffer) {
            return Err(SocketError::WriteBufferFull(Box::new(item)));
        }
        Ok(())
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

fn elapsed(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap()))
}

#[tokio::test]
async fn sink_ready_at_exact_send_or_authority_fence_cannot_emit_a_byte() {
    for (send_before, authority_until, reason) in [
        (elapsed(3), elapsed(5), UntouchedCommand::SendWindowElapsed),
        (
            elapsed(5),
            elapsed(3),
            UntouchedCommand::ConnectionUnavailable,
        ),
    ] {
        let clock = clock();
        let boundary = WireWriteFence::new(clock.clone());
        let mut sink = TestSink {
            clock: &clock,
            case: SinkCase::ReadyAt(Duration::from_secs(3)),
            entered_send: false,
        };
        let result = write_user_frame(
            &mut sink,
            "test".to_owned(),
            SendFence {
                send_before,
                authority_until,
                write_until: elapsed(10),
            },
            clock.as_ref(),
            &boundary,
        )
        .await;
        assert!(matches!(result, FrameWrite::Untouched(actual) if actual == reason));
        assert!(!sink.entered_send && !boundary.crossed());
    }
}

#[tokio::test]
async fn writer_discriminates_untouched_refusals_from_a_failed_flush() {
    for (case, entered_send) in [
        (SinkCase::RefuseReadiness, false),
        (SinkCase::RejectBeforeBuffer, true),
    ] {
        let clock = clock();
        let boundary = WireWriteFence::new(clock.clone());
        let mut sink = TestSink {
            clock: &clock,
            case,
            entered_send: false,
        };
        let result = write_user_frame(
            &mut sink,
            "test".into(),
            SendFence {
                send_before: elapsed(3),
                authority_until: elapsed(5),
                write_until: elapsed(8),
            },
            clock.as_ref(),
            &boundary,
        )
        .await;
        assert!(matches!(
            result,
            FrameWrite::Untouched(UntouchedCommand::SinkRejected)
        ));
        assert!(!boundary.crossed() && !boundary.must_discard());
        assert_eq!(sink.entered_send, entered_send);
    }
    let fixture = BufferedFixture::new(1).await;
    fixture.check_started_flush_loss().await;
}

#[tokio::test]
async fn writer_readiness_timeout_keeps_the_frame_untouched() {
    let clock = clock();
    let boundary = WireWriteFence::new(clock.clone());
    let mut sink = TestSink {
        clock: &clock,
        case: SinkCase::Pending,
        entered_send: false,
    };
    let mut write = Box::pin(write_user_frame(
        &mut sink,
        "test".into(),
        SendFence {
            send_before: elapsed(3),
            authority_until: elapsed(5),
            write_until: elapsed(8),
        },
        clock.as_ref(),
        &boundary,
    ));
    assert!(write.as_mut().now_or_never().is_none());
    clock.advance(Duration::from_secs(3)).unwrap();
    assert!(matches!(
        write.await,
        FrameWrite::Untouched(UntouchedCommand::SendWindowElapsed)
    ));
    assert!(!sink.entered_send && !boundary.crossed());
}

pub(crate) struct ControlledWire {
    stream: TokioIo<DuplexStream>,
    remaining: Arc<AtomicUsize>,
    crossed: Arc<AtomicUsize>,
    write_polls: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

impl Read for ControlledWire {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl Write for ControlledWire {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write_polls.fetch_add(1, Ordering::Relaxed);
        if self.fail.load(Ordering::Relaxed) {
            return Poll::Ready(Err(io::Error::other("controlled write failure")));
        }
        let count = bytes.len().min(self.remaining.load(Ordering::Relaxed));
        if count == 0 {
            return Poll::Pending;
        }
        let Some(prefix) = bytes.get(..count) else {
            return Poll::Ready(Err(io::Error::other("prefix")));
        };
        let result = Pin::new(&mut self.stream).poll_write(context, prefix);
        if let Poll::Ready(Ok(amount)) = &result {
            self.remaining.fetch_sub(*amount, Ordering::Relaxed);
            self.crossed.fetch_add(*amount, Ordering::Relaxed);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

pub(crate) struct BufferedFixture {
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) boundary: Arc<WireWriteFence>,
    pub(crate) socket: WebSocketStream<TokioIo<FencedTransport<ControlledWire>>>,
    pub(crate) peer: DuplexStream,
    pub(crate) remaining: Arc<AtomicUsize>,
    pub(crate) crossed: Arc<AtomicUsize>,
    pub(crate) write_polls: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

impl BufferedFixture {
    pub(crate) async fn new(allowed_bytes: usize) -> Self {
        let clock = clock();
        let boundary = Arc::new(WireWriteFence::new(clock.clone()));
        let (wire, peer) = tokio::io::duplex(1024);
        let remaining = Arc::new(AtomicUsize::new(allowed_bytes));
        let crossed = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let write_polls = Arc::new(AtomicUsize::new(0));
        let socket = WebSocketStream::from_raw_socket(
            TokioIo::new(FencedTransport {
                inner: ControlledWire {
                    stream: TokioIo::new(wire),
                    remaining: remaining.clone(),
                    crossed: crossed.clone(),
                    write_polls: write_polls.clone(),
                    fail: fail.clone(),
                },
                fence: boundary.clone(),
            }),
            Role::Client,
            None,
        )
        .await;
        Self {
            clock,
            boundary,
            socket,
            peer,
            remaining,
            crossed,
            fail,
            write_polls,
        }
    }

    async fn check_started_flush_loss(mut self) {
        let mut write = Box::pin(write_user_frame(
            &mut self.socket,
            "test".into(),
            SendFence {
                send_before: elapsed(3),
                authority_until: elapsed(5),
                write_until: elapsed(8),
            },
            self.clock.as_ref(),
            &self.boundary,
        ));
        assert!(write.as_mut().now_or_never().is_none());
        assert_eq!(self.crossed.load(Ordering::Relaxed), 1);
        self.clock.advance(Duration::from_secs(3)).unwrap();
        assert!(
            write.as_mut().now_or_never().is_none(),
            "the send fence does not withdraw a frame that has crossed"
        );
        self.fail.store(true, Ordering::Relaxed);
        assert!(matches!(write.await, FrameWrite::PossiblyWritten));
        assert!(self.boundary.crossed() && self.boundary.must_discard());
    }
}

#[tokio::test]
async fn buffered_zero_byte_frame_cannot_start_at_either_fence_or_flush_on_close() {
    for (send_before, authority_until, reason) in [
        (elapsed(3), elapsed(5), UntouchedCommand::SendWindowElapsed),
        (
            elapsed(5),
            elapsed(3),
            UntouchedCommand::ConnectionUnavailable,
        ),
    ] {
        let mut fixture = BufferedFixture::new(0).await;
        let mut write = Box::pin(write_user_frame(
            &mut fixture.socket,
            "staged user frame".into(),
            SendFence {
                send_before,
                authority_until,
                write_until: elapsed(8),
            },
            fixture.clock.as_ref(),
            &fixture.boundary,
        ));
        assert!(write.as_mut().now_or_never().is_none());
        assert_eq!(fixture.crossed.load(Ordering::Relaxed), 0);
        fixture.clock.advance(Duration::from_secs(3)).unwrap();
        fixture.remaining.store(1024, Ordering::Relaxed);
        assert!(matches!(write.await, FrameWrite::DiscardUntouched(actual) if actual == reason));
        assert!(fixture.boundary.must_discard() && !fixture.boundary.crossed());
        finish_socket(&mut fixture.socket, &fixture.boundary);
        drop(fixture.socket);
        let mut observed = Vec::new();
        fixture.peer.read_to_end(&mut observed).await.unwrap();
        assert!(
            observed.is_empty(),
            "normal flush and teardown must write zero bytes for the expired frame"
        );
    }
}
