//! Writes the driver's registration frame and settles its exact socket reply.

use central_link_client::{
    ConnectionAttempt, LinkLocalFault, UntouchedCommand, link_frame_answer_deadline,
    link_frame_write_deadline,
};
use central_link_protocol::{CentralCallReply, CorrelationId, TypedCallResult};
use futures_util::Sink;
use router_clock::ClockInstant;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};

use super::{BindingDriver, Control, Interrupted, bounded_wire_text};
use crate::{
    admitted_lane_socket::AdmittedSocketLane,
    lane_writer::{FrameWrite, SendFence, write_fenced_frame},
    lease_source::LeaseSource,
    link_policy::{LinkInput, RegistrationFailure},
    run_registration::ReplyAction,
    wire_write_fence::{LinkConnector, WireWriteFence},
};

impl<Binding, Source, MakeConnector, Connector> BindingDriver<'_, Binding, Source, MakeConnector>
where
    Binding: Clone + Eq + Send + Sync,
    Source: LeaseSource<Binding>,
    MakeConnector: Fn() -> Connector + Sync,
    Connector: LinkConnector,
{
    pub(super) fn settle_registration_reply(
        &mut self,
        attempt: &ConnectionAttempt,
        correlation_id: &CorrelationId,
        result: &TypedCallResult<CentralCallReply>,
        admitted_lane: &mut AdmittedSocketLane<'_>,
    ) -> bool {
        let now = self.clock.monotonic_now();
        if self
            .state
            .renewal_deadline()
            .is_none_or(|deadline| now >= deadline)
        {
            return true;
        }
        match self
            .registration
            .observe_reply(attempt, correlation_id, result, now)
        {
            ReplyAction::KeepSocket => {}
            ReplyAction::Publish(registered) => {
                if self.client.ready(attempt).is_err() {
                    return false;
                }
                admitted_lane.publish_registered(*registered, correlation_id);
            }
            ReplyAction::CloseSocket => return false,
        }
        self.publish_status();
        true
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Use the exact socket, binding, deadline and lane owned by this attempt."
    )]
    pub(super) async fn write_registration(
        &mut self,
        socket: &mut (impl Sink<Message, Error = WebSocketError> + Unpin),
        attempt: &ConnectionAttempt,
        binding: &Binding,
        control: &mut Control<'_, Binding>,
        admitted_lane: &mut AdmittedSocketLane<'_>,
        writes: &WireWriteFence,
        delivery_stall_at: Option<ClockInstant>,
    ) -> Result<bool, Interrupted> {
        let clock = self.clock.clone();
        let now = clock.monotonic_now();
        let Some(authority_until) = self.state.renewal_deadline() else {
            self.close(attempt, None);
            return Ok(false);
        };
        let Ok(target) = self.lease_source.registration_target(binding) else {
            let _ = self.apply(LinkInput::LeaseFault {
                binding: binding.clone(),
            });
            self.close(attempt, None);
            return Ok(false);
        };
        let Some(frame) =
            self.registration
                .begin_write(target, now, self.clock.utc_now().timestamp_millis())
        else {
            self.close(attempt, None);
            return Ok(false);
        };
        self.publish_status();
        let outcome = if let Ok(wire) = bounded_wire_text(&frame) {
            let write_until = link_frame_write_deadline(now);
            let fence = SendFence {
                send_before: write_until.min(authority_until),
                authority_until,
                write_until,
            };
            let stalled = admitted_lane.wait_for_delivery_stall(clock.as_ref(), delivery_stall_at);
            let writing = admitted_lane.keeping_reply_deadline(
                clock.as_ref(),
                control.during(write_fenced_frame(
                    socket,
                    wire,
                    fence,
                    clock.as_ref(),
                    writes,
                )),
            );
            tokio::select! {
                biased;
                () = stalled => {
                    admitted_lane.mark_local_fault(LinkLocalFault::DeliveryConsumerStalled);
                    FrameWrite::PossiblyWritten
                }
                result = writing => result?,
            }
        } else {
            FrameWrite::Untouched(UntouchedCommand::InvalidFrame)
        };
        let completed_at = self.clock.monotonic_now();
        match outcome {
            FrameWrite::Flushed
                if self
                    .registration
                    .written(&frame, link_frame_answer_deadline(now, completed_at)) => {}
            FrameWrite::Untouched(cause) => {
                self.registration
                    .fail(completed_at, RegistrationFailure::NotWritten(cause));
                if cause == UntouchedCommand::SinkRejected {
                    self.close(attempt, None);
                    return Ok(false);
                }
            }
            FrameWrite::DiscardUntouched(cause) => {
                self.registration
                    .fail(completed_at, RegistrationFailure::NotWritten(cause));
                self.close(attempt, None);
                return Ok(false);
            }
            FrameWrite::Flushed | FrameWrite::PossiblyWritten => {
                self.registration.failed_on_lost_socket(completed_at);
                self.close(attempt, None);
                return Ok(false);
            }
        }
        Ok(true)
    }
}
