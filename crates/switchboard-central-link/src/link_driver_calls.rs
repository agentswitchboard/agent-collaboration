//! Writes one work call through the same bounded writer as custody and control traffic.

use central_link_client::{
    ConnectionAttempt, LinkLocalFault, UntouchedCommand, link_frame_write_deadline,
};
use central_link_protocol::{DataFrame, LinkFrame};
use futures_util::Sink;
use router_clock::ClockInstant;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};

use super::{BindingDriver, Control, Interrupted, bounded_wire_text};
use crate::{
    admitted_lane_socket::AdmittedSocketLane,
    central_call_lane::CallEnding,
    lane_writer::{FrameWrite, SendFence, write_fenced_frame},
    lease_source::LeaseSource,
    wire_write_fence::{LinkConnector, WireWriteFence},
};

impl<Binding, Source, MakeConnector, Connector> BindingDriver<'_, Binding, Source, MakeConnector>
where
    Binding: Clone + Eq + Send + Sync,
    Source: LeaseSource<Binding>,
    MakeConnector: Fn() -> Connector + Sync,
    Connector: LinkConnector,
{
    pub(super) fn work_write_can_start(&self, now: ClockInstant) -> bool {
        // A staged frame cannot yield the carrier at renewal without discarding it.
        self.state
            .next_deadline()
            .is_some_and(|next| link_frame_write_deadline(now) < next)
    }

    pub(super) async fn write_work_call(
        &mut self,
        socket: &mut (impl Sink<Message, Error = WebSocketError> + Unpin),
        attempt: &ConnectionAttempt,
        control: &mut Control<'_, Binding>,
        lane: &mut AdmittedSocketLane<'_>,
        writes: &WireWriteFence,
        delivery_stall_at: Option<ClockInstant>,
    ) -> Result<bool, Interrupted> {
        let clock = self.clock.clone();
        let now = clock.monotonic_now();
        let Some(authority_until) = self.state.renewal_deadline() else {
            return Ok(false);
        };
        let renewal_fence = self.state.next_deadline().unwrap_or(authority_until);
        if !self.work_write_can_start(now) {
            return Ok(true);
        }
        let Some(mut call) = lane.calls.begin_write(attempt, now, writes) else {
            return Ok(true);
        };
        let Some(request) = call.request.take() else {
            call.ended(CallEnding::InvalidFrame);
            return Ok(true);
        };
        let frame = LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: call.correlation().clone(),
            request,
        });
        let Ok(wire) = bounded_wire_text(&frame) else {
            call.ended(CallEnding::InvalidFrame);
            return Ok(true);
        };
        let deadline = call.deadline;
        let fence = SendFence {
            send_before: deadline,
            authority_until: authority_until.min(renewal_fence),
            write_until: link_frame_write_deadline(now).min(deadline),
        };
        let stalled = lane.wait_for_delivery_stall(clock.as_ref(), delivery_stall_at);
        let writing = lane.keeping_reply_deadline(
            clock.as_ref(),
            control.during(write_fenced_frame(
                socket,
                wire,
                fence,
                clock.as_ref(),
                writes,
            )),
        );
        let result = tokio::select! {
            biased;
            () = stalled => None,
            result = writing => Some(result),
        };
        match result {
            None => {
                call.ended(CallEnding::DeliveryConsumerStalled);
                lane.mark_local_fault(LinkLocalFault::DeliveryConsumerStalled);
                Ok(false)
            }
            Some(Err(interrupted)) => {
                call.ended(match interrupted {
                    Interrupted::SettingsChanged => CallEnding::SettingsChanged,
                    Interrupted::Stopped => CallEnding::Shutdown,
                });
                Err(interrupted)
            }
            Some(Ok(FrameWrite::Flushed)) => {
                call.flushed();
                Ok(true)
            }
            Some(Ok(FrameWrite::Untouched(UntouchedCommand::SinkRejected))) => {
                call.ended(CallEnding::TransportLost);
                Ok(false)
            }
            Some(Ok(outcome)) => {
                let untouched = matches!(outcome, FrameWrite::Untouched(_));
                let cause = if clock.monotonic_now() >= deadline {
                    CallEnding::Deadline
                } else if untouched || clock.monotonic_now() >= renewal_fence {
                    CallEnding::WriterWindowElapsed
                } else {
                    CallEnding::TransportLost
                };
                call.ended(cause);
                Ok(untouched)
            }
        }
    }
}
