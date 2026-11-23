//! Interpret the admitted-lane policy and own its event permits for one socket.

use std::{
    collections::BTreeSet,
    future::{Future, pending},
    pin::pin,
    sync::Arc,
};

use central_link_client::{
    AdmittedConnection, ClientEvent, ConnectionAttempt, LinkLaneEvent, LinkLocalFault,
    UntouchedCommand, ValidatedDeliveryOffer, link_frame_answer_deadline,
};
use central_link_protocol::CorrelationId;
use router_clock::{ClockInstant, RuntimeClock};
use state_machine_core::{StateMachine, StateMachineResult};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    admitted_lane_policy::{
        AdmittedLanePolicy, LaneEffect, LaneInput, LaneResult, LaneState, SocketReply,
    },
    link_lane::{CommandIntake, LaneRequest, LinkLaneOwner},
};

const CONNECTION_CORRELATION_LIMIT: usize = 4096;

pub(super) type EventPermit = mpsc::OwnedPermit<LinkLaneEvent>;

pub(super) enum ReadAdmission {
    AwaitingWelcome,
    AwaitingEventSpace {
        events: mpsc::Sender<LinkLaneEvent>,
        slots: usize,
    },
    Admitted(mpsc::Sender<LinkLaneEvent>),
}

pub(super) struct AdmittedSocketLane {
    state: LaneState,
    admitted: watch::Sender<Option<AdmittedConnection>>,
    intake: Arc<CommandIntake>,
    events: mpsc::Sender<LinkLaneEvent>,
    pending_admission: Option<AdmittedConnection>,
    withdrawal: Option<EventPermit>,
    local_fault: Option<LinkLocalFault>,
    operation: Option<PendingOperation>,
    used_correlations: BTreeSet<CorrelationId>,
}

struct PendingOperation {
    connection: ConnectionAttempt,
    correlations: Vec<CorrelationId>,
    settled: oneshot::Sender<LaneResult>,
}

pub(super) enum FrameOffer {
    Queued {
        wire: String,
        send_before: ClockInstant,
    },
    Declined,
    Reconnect,
}

impl AdmittedSocketLane {
    pub(super) fn new(owner: &LinkLaneOwner) -> Self {
        Self {
            state: LaneState::WaitingForAdmission,
            admitted: owner.admitted.clone(),
            intake: owner.intake.clone(),
            events: owner.events.clone(),
            pending_admission: None,
            withdrawal: None,
            local_fault: None,
            operation: None,
            used_correlations: BTreeSet::new(),
        }
    }

    fn apply(&mut self, input: LaneInput) -> bool {
        self.apply_transition(AdmittedLanePolicy::transition(&self.state, input))
    }

    fn apply_transition(&mut self, transition: StateMachineResult<AdmittedLanePolicy>) -> bool {
        let Ok((state, effects)) = transition else {
            return false;
        };
        self.state = state;
        for LaneEffect::Settle(result) in effects {
            if let Some(operation) = self.operation.take() {
                self.intake.settle(&operation.connection);
                let _ = operation.settled.send(result);
            }
        }
        true
    }

    pub(super) fn admit(&mut self, admitted: AdmittedConnection) -> bool {
        self.pending_admission = Some(admitted);
        self.publish_admission()
    }

    pub(super) fn publish_admission(&mut self) -> bool {
        if self.events.is_closed() {
            return false;
        }
        if self.pending_admission.is_none() || self.events.capacity() < 2 {
            return true;
        }
        let Ok(withdrawal) = self.events.clone().try_reserve_owned() else {
            return true;
        };
        let Ok(admission) = self.events.clone().try_reserve_owned() else {
            return true;
        };
        let Some(admitted) = self.pending_admission.take() else {
            return false;
        };
        if !self.apply(LaneInput::Admit(admitted.clone())) {
            return false;
        }

        // Only this socket publishes. Holding the withdrawal permit makes
        // termination lossless even with a full delivery queue.
        self.withdrawal = Some(withdrawal);
        self.intake.open(admitted.connection.clone());
        self.admitted.send_replace(Some(admitted.clone()));
        admission.send(LinkLaneEvent::ConnectionAdmitted(admitted));
        true
    }

    pub(super) fn read_admission(&self) -> ReadAdmission {
        if self.pending_admission.is_some() {
            ReadAdmission::AwaitingEventSpace {
                events: self.events.clone(),
                slots: 2,
            }
        } else if self.is_admitted() && self.events.capacity() == 0 {
            ReadAdmission::AwaitingEventSpace {
                events: self.events.clone(),
                slots: 1,
            }
        } else if self.is_admitted() {
            ReadAdmission::Admitted(self.events.clone())
        } else {
            ReadAdmission::AwaitingWelcome
        }
    }

    pub(super) fn can_read_without_delay(&self) -> bool {
        self.pending_admission.is_none() && (!self.is_admitted() || self.events.capacity() != 0)
    }

    pub(super) fn mark_local_fault(&mut self, cause: LinkLocalFault) {
        self.local_fault = Some(cause);
    }

    pub(super) fn wait_for_delivery_stall<'clock>(
        &self,
        clock: &'clock dyn RuntimeClock,
        deadline: Option<ClockInstant>,
    ) -> impl Future<Output = ()> + use<'clock> {
        let pressure = deadline.map(|deadline| (self.events.clone(), deadline));
        async move {
            if let Some((events, deadline)) = pressure {
                clock.sleep_until(deadline).await;
                if events.capacity() == 0 {
                    return;
                }
            }
            pending::<()>().await;
        }
    }

    pub(super) fn deliver(
        &self,
        event: ClientEvent,
        permit: Option<EventPermit>,
        clock: &dyn RuntimeClock,
    ) -> bool {
        let (
            Some(admitted),
            Some(permit),
            ClientEvent::Deliver {
                message,
                sender,
                time_left,
            },
        ) = (self.state.admitted(), permit, event)
        else {
            return false;
        };
        let Ok(offer) = ValidatedDeliveryOffer::new(message, time_left) else {
            return false;
        };
        permit.send(LinkLaneEvent::Deliver {
            connection: admitted.connection.clone(),
            arrived_at: clock.utc_now(),
            offer,
            sender,
        });
        true
    }

    pub(super) fn can_offer(&self) -> bool {
        self.state.can_offer()
    }

    pub(super) fn is_admitted(&self) -> bool {
        self.state.admitted().is_some()
    }

    pub(super) fn reply_deadline(&self) -> Option<ClockInstant> {
        self.state.reply_deadline()
    }

    pub(super) fn expire(&mut self, now: ClockInstant) {
        if self
            .reply_deadline()
            .is_some_and(|deadline| now >= deadline)
        {
            self.apply(LaneInput::Elapsed { now });
        }
    }

    pub(super) async fn keeping_reply_deadline<Output>(
        &mut self,
        clock: &dyn RuntimeClock,
        write: impl Future<Output = Output>,
    ) -> Output {
        let Some(deadline) = self.reply_deadline() else {
            return write.await;
        };
        let mut write = pin!(write);
        tokio::select! {
            biased;
            () = clock.sleep_until(deadline) => self.expire(clock.monotonic_now()),
            output = &mut write => return output,
        }
        write.await
    }

    pub(super) fn reply(
        &mut self,
        connection: &ConnectionAttempt,
        reply: SocketReply,
        now: ClockInstant,
        clock: &dyn RuntimeClock,
    ) {
        self.apply_transition(AdmittedLanePolicy::transition_with_reply_clock(
            &self.state,
            LaneInput::Reply {
                connection: connection.clone(),
                reply,
                now,
            },
            |_| clock.monotonic_now(),
        ));
    }

    pub(super) fn offer(&mut self, request: LaneRequest, now: ClockInstant) -> FrameOffer {
        let LaneRequest {
            connection,
            send_before,
            wire,
            expected,
            settled,
        } = request;
        if !self.state.can_offer()
            || !self
                .state
                .admitted()
                .is_some_and(|admitted| admitted.connection == connection)
        {
            self.reject_offer(
                &connection,
                settled,
                UntouchedCommand::ConnectionUnavailable,
            );
            return FrameOffer::Declined;
        }
        if now >= send_before {
            self.reject_offer(&connection, settled, UntouchedCommand::SendWindowElapsed);
            return FrameOffer::Declined;
        }
        let correlations = expected.correlations();
        if self.used_correlations.len() + correlations.len() > CONNECTION_CORRELATION_LIMIT {
            self.reject_offer(
                &connection,
                settled,
                UntouchedCommand::ConnectionUnavailable,
            );
            return FrameOffer::Reconnect;
        }
        if correlations
            .iter()
            .any(|id| self.used_correlations.contains(id))
            || !self.apply(LaneInput::Offer {
                connection: connection.clone(),
                expected,
            })
        {
            self.reject_offer(&connection, settled, UntouchedCommand::InvalidFrame);
            return FrameOffer::Declined;
        }
        self.operation = Some(PendingOperation {
            connection,
            correlations,
            settled,
        });
        FrameOffer::Queued { wire, send_before }
    }

    fn reject_offer(
        &self,
        connection: &ConnectionAttempt,
        settled: oneshot::Sender<LaneResult>,
        cause: UntouchedCommand,
    ) {
        self.intake.settle(connection);
        let _ = settled.send(LaneResult::Untouched(cause));
    }

    pub(super) fn start_write(&mut self) -> bool {
        if !self.apply(LaneInput::WriteStarted) {
            return false;
        }
        if let Some(operation) = self.operation.as_mut() {
            self.used_correlations
                .extend(operation.correlations.drain(..));
        }
        true
    }

    pub(super) fn reject_write(&mut self, cause: UntouchedCommand) {
        self.apply(LaneInput::WriteRejected { cause });
    }

    pub(super) fn finish_write(&mut self, write_began_at: ClockInstant, flushed_at: ClockInstant) {
        self.apply(LaneInput::WriteFlushed {
            deadline: link_frame_answer_deadline(write_began_at, flushed_at),
        });
    }
}

impl Drop for AdmittedSocketLane {
    fn drop(&mut self) {
        // This is the fence that linearizes retirement against enqueue. The
        // owner drains the receiver after the socket scope returns.
        self.intake.close();
        if let Some(admitted) = self.state.admitted() {
            let connection = admitted.connection.clone();
            self.admitted.send_replace(None);
            self.apply(LaneInput::Close {
                connection: connection.clone(),
            });
            if let Some(withdrawal) = self.withdrawal.take() {
                let event = match self.local_fault {
                    Some(cause) => LinkLaneEvent::ConnectionFaulted { connection, cause },
                    None => LinkLaneEvent::ConnectionNotAdmitted { connection },
                };
                withdrawal.send(event);
            }
        }
    }
}
