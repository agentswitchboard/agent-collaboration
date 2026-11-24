//! Send custody and the exclusive first-transport-write fence for socket frames.

use std::{
    future::{pending, poll_fn},
    pin::pin,
};

use central_link_client::UntouchedCommand;
use futures_util::{Sink, SinkExt};
use router_clock::{ClockInstant, RuntimeClock};
use tokio_tungstenite::tungstenite::{Error as SocketError, Message};

use crate::wire_write_fence::WireWriteFence;

pub(super) struct SendFence {
    pub(super) send_before: ClockInstant,
    pub(super) authority_until: ClockInstant,
    pub(super) write_until: ClockInstant,
}

pub(super) enum FrameWrite {
    Flushed,
    Untouched(UntouchedCommand),
    DiscardUntouched(UntouchedCommand),
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

pub(super) async fn write_fenced_frame(
    socket: &mut (impl Sink<Message, Error = SocketError> + Unpin),
    wire: String,
    fence: SendFence,
    clock: &dyn RuntimeClock,
    boundary: &WireWriteFence,
) -> FrameWrite {
    let deadline = fence.first_byte_deadline();
    let ready = tokio::select! {
        biased;
        () = clock.sleep_until(deadline) => return FrameWrite::Untouched(fence.untouched_at(clock.monotonic_now())),
        ready = async {
            poll_fn(|context| socket.poll_ready_unpin(context)).await?;
            // Earlier control output must be fully flushed before its bytes
            // could be mistaken for those of this application frame.
            socket.flush().await
        } => ready,
    };
    if ready.is_err() {
        return FrameWrite::Untouched(UntouchedCommand::SinkRejected);
    }
    let now = clock.monotonic_now();
    if now >= deadline {
        return FrameWrite::Untouched(fence.untouched_at(now));
    }

    let guard = boundary.watch_first_byte(deadline);
    match socket.start_send_unpin(Message::Text(wire.into())) {
        Ok(()) => {}
        Err(SocketError::WriteBufferFull(_)) => {
            guard.complete();
            return FrameWrite::Untouched(UntouchedCommand::SinkRejected);
        }
        Err(_) => {
            if guard.crossed() {
                return FrameWrite::PossiblyWritten;
            }
            return FrameWrite::DiscardUntouched(fence.untouched_at(clock.monotonic_now()));
        }
    }

    let flush_deadline = fence.authority_until.min(fence.write_until);
    let mut flush = pin!(socket.flush());
    let flush_result = tokio::select! {
        biased;
        () = clock.sleep_until(flush_deadline) => None,
        () = async {
            clock.sleep_until(deadline).await;
            if guard.crossed() { pending::<()>().await; }
        } => None,
        result = &mut flush => Some(result),
    };
    if guard.crossed() {
        if matches!(flush_result, Some(Ok(()))) {
            guard.complete();
            FrameWrite::Flushed
        } else {
            FrameWrite::PossiblyWritten
        }
    } else {
        // Dropping the guard leaves the transport quarantined. A later Close
        // cannot force the staged WebSocket or TLS data across the boundary.
        FrameWrite::DiscardUntouched(fence.untouched_at(clock.monotonic_now()))
    }
}

#[cfg(test)]
#[path = "lane_writer_test.rs"]
pub(crate) mod tests;
