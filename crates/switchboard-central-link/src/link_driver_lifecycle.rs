//! Write one lifecycle frame through the socket's existing first-byte fence.

use central_link_client::{
    ConnectionAttempt, LifecycleCallNotSent, LinkLocalFault, UntouchedCommand,
    link_frame_answer_deadline,
};
use central_link_protocol::DataFrame;
use futures_util::Sink;
use router_clock::ClockInstant;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};

use super::{BindingDriver, Control, Interrupted};
use crate::{
    admitted_lane_socket::AdmittedSocketLane,
    lane_writer::{FrameWrite, SendFence, write_fenced_frame},
    lease_source::LeaseSource,
    lifecycle_lane::{LifecycleCompletion, LifecycleRequest, not_sent},
    lifecycle_pending::WrittenLifecycleCall,
    link_lane::encode_user_frame,
    wire_write_fence::{LinkConnector, WireWriteFence},
};

pub(super) struct LifecycleWriteContext<'socket> {
    pub(super) writes: &'socket WireWriteFence,
    pub(super) delivery_stall_at: Option<ClockInstant>,
    pub(super) write_until: ClockInstant,
}

struct InFlightLifecycleWrite<'socket> {
    completion: &'socket mut LifecycleCompletion,
    fence: &'socket WireWriteFence,
}

impl Drop for InFlightLifecycleWrite<'_> {
    fn drop(&mut self) {
        // Also covers cancellation of the whole driver mid-write. A completed
        // flush clears the fence, and is recorded explicitly by the caller.
        if self.fence.crossed() {
            self.completion.may_have_written();
        }
    }
}

impl<Binding, Source, MakeConnector, Connector> BindingDriver<'_, Binding, Source, MakeConnector>
where
    Binding: Clone + Eq + Send + Sync,
    Source: LeaseSource<Binding>,
    MakeConnector: Fn() -> Connector + Sync,
    Connector: LinkConnector,
{
    pub(super) async fn write_lifecycle_call(
        &mut self,
        request: LifecycleRequest,
        attempt: &ConnectionAttempt,
        socket: &mut (impl Sink<Message, Error = WebSocketError> + Unpin),
        control: &mut Control<'_, Binding>,
        admitted: &mut AdmittedSocketLane<'_>,
        context: LifecycleWriteContext<'_>,
    ) -> Result<bool, Interrupted> {
        let LifecycleRequest {
            scope,
            connection,
            send_before,
            call,
            expected,
            mut completion,
        } = request;
        if !completion.caller_is_waiting()
            || &connection != attempt
            || admitted.lifecycle.scope() != Some(&scope)
        {
            return Ok(true);
        }
        let now = self.clock.monotonic_now();
        if now >= send_before {
            completion.finish(Err(not_sent(LifecycleCallNotSent::Deadline)));
            return Ok(true);
        }
        let Some(authority_until) = self
            .state
            .renewal_deadline()
            .filter(|deadline| now < *deadline)
        else {
            return Ok(false);
        };
        let Ok(correlation) = self.client.central_call_correlation(attempt) else {
            return Ok(false);
        };
        let frame = DataFrame::CallCentral {
            correlation_id: correlation.clone(),
            request: call,
        };
        let Ok(wire) = encode_user_frame(&frame) else {
            completion.finish(Err(not_sent(LifecycleCallNotSent::InvalidFrame)));
            return Ok(true);
        };
        drop(frame);

        let fence = SendFence {
            send_before,
            authority_until,
            write_until: context.write_until,
        };
        let outcome = {
            let _in_flight = InFlightLifecycleWrite {
                completion: &mut completion,
                fence: context.writes,
            };
            let stalled =
                admitted.wait_for_delivery_stall(self.clock.as_ref(), context.delivery_stall_at);
            let writing = control.during(write_fenced_frame(
                socket,
                wire,
                fence,
                self.clock.as_ref(),
                context.writes,
            ));
            admitted
                .keeping_reply_deadlines(self.clock.as_ref(), async {
                    tokio::select! {
                        biased;
                        () = stalled => Ok(None),
                        result = writing => result.map(Some),
                    }
                })
                .await?
        };
        match outcome {
            Some(FrameWrite::Flushed) => {
                completion.may_have_written();
                admitted.lifecycle.record(
                    correlation,
                    WrittenLifecycleCall {
                        connection,
                        deadline: link_frame_answer_deadline(now, self.clock.monotonic_now()),
                        expected,
                        completion,
                    },
                );
                Ok(true)
            }
            Some(FrameWrite::Untouched(reason)) => {
                completion.finish(Err(not_sent(untouched_reason(reason))));
                Ok(true)
            }
            Some(FrameWrite::DiscardUntouched(reason)) => {
                completion.finish(Err(not_sent(untouched_reason(reason))));
                Ok(false)
            }
            Some(FrameWrite::PossiblyWritten) => {
                completion.may_have_written();
                Ok(false)
            }
            None => {
                admitted.mark_local_fault(LinkLocalFault::DeliveryConsumerStalled);
                Ok(false)
            }
        }
    }
}

fn untouched_reason(reason: UntouchedCommand) -> LifecycleCallNotSent {
    match reason {
        UntouchedCommand::ConnectionUnavailable | UntouchedCommand::SinkRejected => {
            LifecycleCallNotSent::LinkNotReady
        }
        UntouchedCommand::WriterBusy => LifecycleCallNotSent::Busy,
        UntouchedCommand::SendWindowElapsed => LifecycleCallNotSent::Deadline,
        UntouchedCommand::FrameTooLarge | UntouchedCommand::InvalidFrame => {
            LifecycleCallNotSent::InvalidFrame
        }
    }
}
