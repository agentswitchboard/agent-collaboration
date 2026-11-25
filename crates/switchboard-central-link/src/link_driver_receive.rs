//! Incoming link frames and the socket control writes they require.

use central_link_client::{
    AdmittedConnection, ClientEvent, ConnectionAttempt, WelcomeClockAnchor,
    link_frame_write_deadline,
};
use central_link_protocol::{
    CorrelationId, DataFrame, FrameDirection, HandledMacCalls, LinkCallRefusal, LinkFrame,
    MacCapabilities, ProtocolError, TokenRenewalResult, TypedCallResult, decode_text,
};
use futures_util::Sink;
use router_clock::ClockInstant;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};

use super::{BindingDriver, Control, Interrupted, bounded_wire_text};
use crate::{
    admitted_lane_policy::SocketReply,
    admitted_lane_socket::{AdmittedSocketLane, EventPermit},
    lane_writer::{FrameWrite, SendFence, write_fenced_frame},
    lease_source::LeaseSource,
    link_policy::LinkInput,
    wire_write_fence::{LinkConnector, WireWriteFence},
};

pub(super) enum SocketReceive {
    Continue,
    Close,
    AnnounceCapabilities(AdmittedConnection),
    RefuseCall(CorrelationId),
}

impl<Binding, Source, MakeConnector, Connector> BindingDriver<'_, Binding, Source, MakeConnector>
where
    Binding: Clone + Eq + Send + Sync,
    Source: LeaseSource<Binding>,
    MakeConnector: Fn() -> Connector + Sync,
    Connector: LinkConnector,
{
    pub(super) fn receive_text(
        &mut self,
        attempt: &ConnectionAttempt,
        text: &str,
        started: ClockInstant,
        renewal_sent_at: Option<ClockInstant>,
        admitted_lane: &mut AdmittedSocketLane<'_>,
        permit: Option<EventPermit>,
    ) -> SocketReceive {
        match decode_text(text, FrameDirection::ToRouter) {
            Ok(frame) => self.receive_frame(
                attempt,
                frame,
                started,
                renewal_sent_at,
                admitted_lane,
                permit,
            ),
            Err(ProtocolError::UnknownTypedCall { correlation_id })
                if admitted_lane.is_admitted() =>
            {
                SocketReceive::RefuseCall(correlation_id)
            }
            Err(_) => SocketReceive::Close,
        }
    }

    pub(super) async fn complete_receive(
        &mut self,
        received: SocketReceive,
        socket: &mut (impl Sink<Message, Error = WebSocketError> + Unpin),
        control: &mut Control<'_, Binding>,
        admitted_lane: &mut AdmittedSocketLane<'_>,
        writes: &WireWriteFence,
    ) -> Result<bool, Interrupted> {
        match received {
            SocketReceive::Continue => Ok(true),
            SocketReceive::Close => Ok(false),
            SocketReceive::AnnounceCapabilities(admission) => {
                let frame = DataFrame::MacCapabilities(MacCapabilities {
                    handled_calls: HandledMacCalls::default(),
                });
                let flushed = self
                    .write_socket_control(frame, socket, control, admitted_lane, writes)
                    .await?;
                Ok(flushed && admitted_lane.admit(admission))
            }
            SocketReceive::RefuseCall(correlation_id) => {
                let frame = DataFrame::MacCallReply {
                    correlation_id,
                    result: Box::new(TypedCallResult::Refused {
                        reason: LinkCallRefusal::UnknownCall,
                    }),
                };
                self.write_socket_control(frame, socket, control, admitted_lane, writes)
                    .await
            }
        }
    }

    async fn write_socket_control(
        &mut self,
        frame: DataFrame,
        socket: &mut (impl Sink<Message, Error = WebSocketError> + Unpin),
        control: &mut Control<'_, Binding>,
        admitted_lane: &mut AdmittedSocketLane<'_>,
        writes: &WireWriteFence,
    ) -> Result<bool, Interrupted> {
        let Some(authority_until) = self.state.renewal_deadline() else {
            return Ok(false);
        };
        let Ok(wire) = bounded_wire_text(&LinkFrame::Data(frame)) else {
            return Ok(false);
        };
        let fence = SendFence {
            send_before: authority_until,
            authority_until,
            write_until: link_frame_write_deadline(self.clock.monotonic_now()),
        };
        let write = control.during(write_fenced_frame(
            socket,
            wire,
            fence,
            self.clock.as_ref(),
            writes,
        ));
        let outcome = admitted_lane
            .keeping_reply_deadline(self.clock.as_ref(), write)
            .await?;
        Ok(matches!(outcome, FrameWrite::Flushed) && self.clock.monotonic_now() < authority_until)
    }

    pub(super) fn receive_frame(
        &mut self,
        attempt: &ConnectionAttempt,
        frame: LinkFrame,
        started: ClockInstant,
        renewal_sent_at: Option<ClockInstant>,
        admitted_lane: &mut AdmittedSocketLane<'_>,
        permit: Option<EventPermit>,
    ) -> SocketReceive {
        let reply = match frame {
            LinkFrame::Data(DataFrame::Reply {
                correlation_id,
                result,
            }) => SocketReply::Request {
                correlation_id,
                result,
            },
            LinkFrame::Data(DataFrame::RequestStatusReply {
                correlation_id,
                result,
            }) => SocketReply::RequestStatus {
                correlation_id,
                result,
            },
            LinkFrame::Data(DataFrame::ReceiptResults {
                correlation_id,
                results,
            }) => SocketReply::ReceiptResults {
                correlation_id,
                results,
            },
            other => {
                let Ok(event) = self.client.receive(attempt, other) else {
                    return SocketReceive::Close;
                };
                return self.receive_event(
                    attempt,
                    event,
                    started,
                    renewal_sent_at,
                    admitted_lane,
                    permit,
                );
            }
        };
        if !admitted_lane.is_admitted() {
            return SocketReceive::Close;
        }
        let now = self.clock.monotonic_now();
        if self
            .state
            .renewal_deadline()
            .is_some_and(|deadline| now < deadline)
        {
            admitted_lane.reply(attempt, reply, now, self.clock.as_ref());
        }
        SocketReceive::Continue
    }

    fn receive_event(
        &mut self,
        attempt: &ConnectionAttempt,
        event: ClientEvent,
        started: ClockInstant,
        renewal_sent_at: Option<ClockInstant>,
        admitted_lane: &mut AdmittedSocketLane<'_>,
        permit: Option<EventPermit>,
    ) -> SocketReceive {
        match event {
            ClientEvent::Welcomed {
                central_time,
                effective_expires_at,
            } => {
                let lifetime = (effective_expires_at - central_time)
                    .to_std()
                    .unwrap_or_default();
                let now = self.clock.monotonic_now();
                let accepted = self
                    .apply(LinkInput::Welcome {
                        attempt: attempt.clone(),
                        machine_id: self.machine_id.clone(),
                        deadline: started.after(lifetime),
                        now,
                    })
                    .is_some();
                if !accepted {
                    return SocketReceive::Close;
                }
                if self.state.renewal_deadline().is_none() {
                    return SocketReceive::Continue;
                }
                SocketReceive::AnnounceCapabilities(AdmittedConnection {
                    connection: attempt.clone(),
                    clock_anchor: WelcomeClockAnchor {
                        central_time,
                        observed_at: now,
                    },
                })
            }
            ClientEvent::TokenRenewal {
                correlation_id,
                result,
            } => {
                let event = match result {
                    TokenRenewalResult::Renewed {
                        central_time,
                        effective_expires_at,
                    } => {
                        let Some(sent_at) = renewal_sent_at else {
                            return SocketReceive::Continue;
                        };
                        let lifetime = (effective_expires_at - central_time)
                            .to_std()
                            .unwrap_or_default();
                        LinkInput::RenewalAccepted {
                            attempt: attempt.clone(),
                            correlation_id,
                            deadline: sent_at.after(lifetime),
                            now: self.clock.monotonic_now(),
                        }
                    }
                    TokenRenewalResult::Refused { reason } => LinkInput::RenewalRefused {
                        attempt: attempt.clone(),
                        correlation_id,
                        reason,
                        now: self.clock.monotonic_now(),
                    },
                };
                let _ = self.apply(event);
                SocketReceive::Continue
            }
            event @ ClientEvent::Deliver { .. } => {
                if admitted_lane.deliver(event, permit, self.clock.as_ref()) {
                    SocketReceive::Continue
                } else {
                    SocketReceive::Close
                }
            }
            ClientEvent::Pong => SocketReceive::Continue,
            ClientEvent::CallMac { correlation_id, .. } if admitted_lane.is_admitted() => {
                SocketReceive::RefuseCall(correlation_id)
            }
            ClientEvent::CentralCallReply {
                correlation_id,
                result,
            } if admitted_lane.is_admitted() => {
                let now = self.clock.monotonic_now();
                if self
                    .state
                    .renewal_deadline()
                    .is_some_and(|deadline| now < deadline)
                {
                    admitted_lane
                        .lifecycle
                        .answer(attempt, &correlation_id, *result, now);
                }
                SocketReceive::Continue
            }
            ClientEvent::CallMac { .. }
            | ClientEvent::CentralCallReply { .. }
            | ClientEvent::ReceiptResults { .. }
            | ClientEvent::LateReply { .. }
            | ClientEvent::Reply { .. } => SocketReceive::Close,
        }
    }
}
