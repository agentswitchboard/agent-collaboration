use futures_util::FutureExt;
use router_clock::ManualClock;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use super::*;

#[derive(Clone, Copy)]
enum SinkCase {
    Pending,
    ReadyAt(Duration),
    RefuseReadiness,
    RejectBeforeBuffer,
    FailFlush,
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
            SinkCase::RejectBeforeBuffer | SinkCase::FailFlush => Poll::Ready(Ok(())),
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
        if matches!(self.case, SinkCase::FailFlush) {
            return Poll::Ready(Err(
                io::Error::other("part of the bytes may have left").into()
            ));
        }
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

fn clock() -> ManualClock {
    ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap())
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
        let mut sink = TestSink {
            clock: &clock,
            case: SinkCase::ReadyAt(Duration::from_secs(3)),
            entered_send: false,
        };
        let mut writing = false;
        let result = write_user_frame(
            &mut sink,
            "test".to_owned(),
            SendFence {
                send_before,
                authority_until,
                write_until: elapsed(10),
            },
            &clock,
            || writing = true,
        )
        .await;
        assert!(matches!(result, FrameWrite::Untouched(actual) if actual == reason));
        assert!(!sink.entered_send && !writing);
    }
}

#[tokio::test]
async fn writer_discriminates_untouched_refusals_from_a_failed_flush() {
    for (case, starts, entered_send) in [
        (SinkCase::RefuseReadiness, false, false),
        (SinkCase::RejectBeforeBuffer, false, true),
        (SinkCase::FailFlush, true, true),
    ] {
        let clock = clock();
        let mut sink = TestSink {
            clock: &clock,
            case,
            entered_send: false,
        };
        let mut writing = false;
        let result = write_user_frame(
            &mut sink,
            "test".to_owned(),
            SendFence {
                send_before: elapsed(3),
                authority_until: elapsed(5),
                write_until: elapsed(8),
            },
            &clock,
            || writing = true,
        )
        .await;
        if starts {
            assert!(matches!(result, FrameWrite::PossiblyWritten));
        } else {
            assert!(matches!(
                result,
                FrameWrite::Untouched(UntouchedCommand::SinkRejected)
            ));
        }
        assert_eq!(
            writing, starts,
            "only a potentially transmitted frame enters writing custody"
        );
        assert_eq!(sink.entered_send, entered_send);
    }
}

#[tokio::test]
async fn writer_readiness_timeout_keeps_the_frame_untouched() {
    let clock = clock();
    let mut sink = TestSink {
        clock: &clock,
        case: SinkCase::Pending,
        entered_send: false,
    };
    let mut writing = false;
    let mut write = Box::pin(write_user_frame(
        &mut sink,
        "test".to_owned(),
        SendFence {
            send_before: elapsed(3),
            authority_until: elapsed(5),
            write_until: elapsed(8),
        },
        &clock,
        || writing = true,
    ));
    assert!(write.as_mut().now_or_never().is_none());
    clock.advance(Duration::from_secs(3)).unwrap();
    assert!(matches!(
        write.await,
        FrameWrite::Untouched(UntouchedCommand::SendWindowElapsed)
    ));
    assert!(!sink.entered_send && !writing);
}
