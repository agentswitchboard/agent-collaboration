//! First-byte fence and transport custody for a prepared user frame.

use std::future::poll_fn;

use central_link_client::UntouchedCommand;
use futures_util::{Sink, SinkExt};
use router_clock::{ClockInstant, RuntimeClock};
use tokio_tungstenite::tungstenite::{Error as SocketError, Message};

pub(super) struct SendFence {
    pub(super) send_before: ClockInstant,
    pub(super) authority_until: ClockInstant,
    pub(super) write_until: ClockInstant,
}

pub(super) enum FrameWrite {
    Flushed,
    Untouched(UntouchedCommand),
    PossiblyWritten,
}

impl SendFence {
    fn first_byte_deadline(&self) -> ClockInstant {
        self.send_before
            .min(self.authority_until)
            .min(self.write_until)
    }

    fn untouched_at(&self, now: ClockInstant) -> UntouchedCommand {
        if now >= self.send_before {
            UntouchedCommand::SendWindowElapsed
        } else {
            UntouchedCommand::ConnectionUnavailable
        }
    }
}

pub(super) async fn write_user_frame(
    socket: &mut (impl Sink<Message, Error = SocketError> + Unpin),
    wire: String,
    fence: SendFence,
    clock: &dyn RuntimeClock,
    mark_writing: impl FnOnce(),
) -> FrameWrite {
    let deadline = fence.first_byte_deadline();
    let ready = tokio::select! {
        biased;
        () = clock.sleep_until(deadline) => return FrameWrite::Untouched(fence.untouched_at(clock.monotonic_now())),
        ready = poll_fn(|context| socket.poll_ready_unpin(context)) => ready,
    };
    if ready.is_err() {
        return FrameWrite::Untouched(UntouchedCommand::SinkRejected);
    }
    // Sink readiness can advance elapsed time during the timer-first poll.
    let now = clock.monotonic_now();
    if now >= deadline {
        return FrameWrite::Untouched(fence.untouched_at(now));
    }
    let started = match socket.start_send_unpin(Message::Text(wire.into())) {
        // This error proves this frame was never appended. Discard its payload.
        Err(SocketError::WriteBufferFull(_)) => {
            return FrameWrite::Untouched(UntouchedCommand::SinkRejected);
        }
        result => result,
    };
    mark_writing();
    if started.is_err() {
        return FrameWrite::PossiblyWritten;
    }
    let flush_deadline = fence.authority_until.min(fence.write_until);
    tokio::select! {
        biased;
        () = clock.sleep_until(flush_deadline) => FrameWrite::PossiblyWritten,
        result = socket.flush() => match result {
            Ok(()) => FrameWrite::Flushed,
            Err(_) => FrameWrite::PossiblyWritten,
        }
    }
}

#[cfg(test)]
#[path = "lane_writer_test.rs"]
mod tests;
