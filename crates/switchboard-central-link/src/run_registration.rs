//! One binding's run authority and the registration exchange on its current socket.

use std::time::Duration;

use central_link_client::{
    AdmittedConnection, ConnectionAttempt, PendingRunRegistration, RegisteredConnection,
    RunRegistrationDecision,
};
use central_link_protocol::{
    CentralCall, CentralCallReply, CorrelationId, DataFrame, FreshCorrelationId, LinkCallRefusal,
    LinkFrame, TypedCallResult, approval_rpc::RegisterRunReply,
};
use router_clock::ClockInstant;
use state_machine_core::StateMachine;
use switchboard_run_gate::{
    AuthorityEnd, CallAttempt, CallKey, DialAttempt, LinkAttempt, RunAuthority, RunGate,
    RunGateEffect, RunGateEvent, RunGateState,
};
use workspace_ids::{MachineId, RouterRunId};

use crate::{
    lease_source::ApprovalServiceTarget,
    link_policy::{LinkStatus, RegistrationFailure},
};

const FIRST_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(30);
const REFUSED_RETRY: Duration = Duration::from_secs(60);

type RegistrationLink = LinkAttempt<ConnectionAttempt>;
type RegistrationCall = CallKey<ConnectionAttempt>;

enum Exchange {
    AwaitingWelcome,
    Due {
        admitted: AdmittedConnection,
        retry: Option<(ClockInstant, RegistrationFailure)>,
    },
    Writing {
        admitted: AdmittedConnection,
        call: RegistrationCall,
    },
    AwaitingReply {
        admitted: AdmittedConnection,
        call: RegistrationCall,
        pending: PendingRunRegistration,
    },
    Ready,
}

pub(super) enum ReplyAction {
    KeepSocket,
    Publish(Box<RegisteredConnection>),
    CloseSocket,
}

pub(super) struct RunRegistration<Binding> {
    authority: RunAuthority<Binding>,
    gate: RunGateState<Binding, ConnectionAttempt>,
    link: Option<RegistrationLink>,
    exchange: Exchange,
    attempts: u64,
    retry_delay: Duration,
    retry_not_before: Option<ClockInstant>,
    parked: Option<AuthorityEnd>,
}

impl<Binding: Clone + Eq> RunRegistration<Binding> {
    pub(super) fn new(binding: Binding, router_run: RouterRunId) -> Self {
        let authority = RunAuthority {
            enrollment: binding,
            router_run,
        };
        Self {
            gate: RunGateState::new(authority.clone()),
            authority,
            link: None,
            exchange: Exchange::AwaitingWelcome,
            attempts: 0,
            retry_delay: FIRST_RETRY,
            retry_not_before: None,
            parked: None,
        }
    }

    fn step(
        &mut self,
        event: RunGateEvent<Binding, ConnectionAttempt>,
    ) -> Option<Vec<RunGateEffect<Binding, ConnectionAttempt>>> {
        let (gate, effects) = RunGate::transition(&self.gate, event).ok()?;
        self.gate = gate;
        Some(effects)
    }

    pub(super) fn dial_started(&mut self, connection: ConnectionAttempt) -> bool {
        self.attempts += 1;
        let link = RegistrationLink {
            connection,
            dial: DialAttempt::new(self.attempts),
        };
        if self
            .step(RunGateEvent::DialStarted { link: link.clone() })
            .is_none()
        {
            return false;
        }
        self.link = Some(link);
        true
    }

    pub(super) fn connection_lost(&mut self, attempt: &ConnectionAttempt) {
        if let Some(link) = self.link.take_if(|link| &link.connection == attempt) {
            let _ = self.step(RunGateEvent::ConnectionLost { link });
            self.exchange = Exchange::AwaitingWelcome;
        }
    }

    pub(super) fn enrollment_ended(&mut self, enrollment: Binding) {
        let _ = self.step(RunGateEvent::AuthenticatedEnrollmentEnded { enrollment });
    }

    pub(super) fn welcomed(&mut self, admitted: AdmittedConnection) -> bool {
        let Some(link) = self
            .link
            .clone()
            .filter(|link| link.connection == admitted.connection)
        else {
            return false;
        };
        if self
            .step(RunGateEvent::Welcome {
                authority: self.authority.clone(),
                link,
            })
            .is_none()
        {
            return false;
        }
        self.exchange = Exchange::Due {
            admitted,
            retry: None,
        };
        true
    }

    pub(super) fn parked(&self) -> bool {
        self.parked.is_some()
    }

    pub(super) fn has_welcome(&self) -> bool {
        !matches!(self.exchange, Exchange::AwaitingWelcome)
    }

    pub(super) fn status(&self, transport: LinkStatus, machine_id: &MachineId) -> LinkStatus {
        match self.parked {
            Some(AuthorityEnd::RunRetired) => return LinkStatus::RunRetired,
            Some(AuthorityEnd::EnrollmentEnded) => {
                return LinkStatus::RegistrationFailed {
                    reason: RegistrationFailure::CentralEnrollmentEnded,
                };
            }
            None => {}
        }
        match transport {
            LinkStatus::Welcomed { .. } => {}
            LinkStatus::Connected { .. } => return LinkStatus::Connecting,
            other => return other,
        }
        match self.exchange {
            Exchange::AwaitingWelcome => LinkStatus::Connecting,
            Exchange::Due {
                retry: Some((_, RegistrationFailure::Refused(reason))),
                ..
            } if requires_long_wait(reason) => LinkStatus::RegistrationFault { reason },
            Exchange::Due {
                retry: Some((_, reason)),
                ..
            } => LinkStatus::RegistrationFailed { reason },
            Exchange::Due { retry: None, .. }
            | Exchange::Writing { .. }
            | Exchange::AwaitingReply { .. } => LinkStatus::Registering,
            Exchange::Ready => LinkStatus::Connected {
                machine_id: machine_id.clone(),
            },
        }
    }

    pub(super) fn next_deadline(&self) -> Option<ClockInstant> {
        match &self.exchange {
            Exchange::Due { retry, .. } => Some(retry.map_or(
                self.retry_not_before.unwrap_or(ClockInstant::ZERO),
                |(at, _)| at,
            )),
            Exchange::AwaitingReply { pending, .. } => Some(pending.answer_deadline()),
            Exchange::AwaitingWelcome | Exchange::Writing { .. } | Exchange::Ready => None,
        }
    }

    pub(super) fn write_due(&self, now: ClockInstant) -> bool {
        matches!(self.exchange, Exchange::Due { .. })
            && self.next_deadline().is_some_and(|deadline| now >= deadline)
    }

    pub(super) fn begin_write(
        &mut self,
        target: ApprovalServiceTarget,
        now: ClockInstant,
        utc_millis: i64,
    ) -> Option<LinkFrame> {
        if !self.write_due(now) {
            return None;
        }
        let Exchange::Due { admitted, .. } = &self.exchange else {
            return None;
        };
        let admitted = admitted.clone();
        self.attempts += 1;
        let call = RegistrationCall {
            link: self.link.clone()?,
            attempt: CallAttempt::new(self.attempts),
        };
        let mut effects = self
            .step(RunGateEvent::PublishRegistration { call })?
            .into_iter();
        let Some(RunGateEffect::Register { call, request }) = effects.next() else {
            return None;
        };
        self.exchange = Exchange::Writing { admitted, call };
        Some(LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: FreshCorrelationId::mint_at(utc_millis).wire_id().clone(),
            request: Box::new(CentralCall::register_run(target, request)),
        }))
    }

    pub(super) fn written(&mut self, frame: &LinkFrame, answer_before: ClockInstant) -> bool {
        let Exchange::Writing { admitted, call } = &self.exchange else {
            return false;
        };
        let Some(pending) =
            PendingRunRegistration::from_sent_frame(admitted.clone(), frame, answer_before)
        else {
            return false;
        };
        self.exchange = Exchange::AwaitingReply {
            admitted: admitted.clone(),
            call: call.clone(),
            pending,
        };
        true
    }

    pub(super) fn fail(&mut self, now: ClockInstant, reason: RegistrationFailure) {
        let (admitted, call) = match &self.exchange {
            Exchange::Writing { admitted, call }
            | Exchange::AwaitingReply { admitted, call, .. } => (admitted.clone(), call.clone()),
            _ => return,
        };
        let Some(_) = self.step(RunGateEvent::RegistrationFailed { call }) else {
            return;
        };
        let delay = match reason {
            RegistrationFailure::Refused(reason) if requires_long_wait(reason) => {
                REFUSED_RETRY + self.retry_delay - FIRST_RETRY
            }
            _ => self.retry_delay,
        };
        let retry_at = now.after(delay);
        self.retry_not_before = Some(retry_at);
        self.retry_delay = (self.retry_delay * 2).min(MAX_RETRY);
        self.exchange = Exchange::Due {
            admitted,
            retry: Some((retry_at, reason)),
        };
    }

    pub(super) fn expire(&mut self, now: ClockInstant) {
        if matches!(&self.exchange, Exchange::AwaitingReply { pending, .. } if now >= pending.answer_deadline())
        {
            self.fail(now, RegistrationFailure::TimedOut);
        }
    }

    pub(super) fn failed_on_lost_socket(&mut self, now: ClockInstant) {
        self.retry_not_before = Some(now.after(self.retry_delay));
        self.retry_delay = (self.retry_delay * 2).min(MAX_RETRY);
    }

    pub(super) fn observe_reply(
        &mut self,
        attempt: &ConnectionAttempt,
        correlation_id: &CorrelationId,
        result: &TypedCallResult<CentralCallReply>,
        now: ClockInstant,
    ) -> ReplyAction {
        use RunRegistrationDecision as Decision;
        let (call, decision) = match &mut self.exchange {
            Exchange::AwaitingReply { call, pending, .. } => (
                call.clone(),
                pending.observe_reply(attempt, correlation_id, result, now),
            ),
            _ => {
                log::debug!("Central run registration reply has no pending attempt");
                return ReplyAction::KeepSocket;
            }
        };
        let (reply, registered) = match decision {
            Decision::Registered(evidence) => (
                RegisterRunReply::Registered {},
                Some(RegisteredConnection::from_registration(evidence)),
            ),
            Decision::Answered(reply) => (reply, None),
            Decision::Refused(reason) => {
                self.fail(now, RegistrationFailure::Refused(reason));
                return ReplyAction::KeepSocket;
            }
            Decision::TimedOut => {
                self.fail(now, RegistrationFailure::TimedOut);
                return ReplyAction::KeepSocket;
            }
            Decision::Mismatched | Decision::NotPending => {
                log::debug!("Central run registration reply did not match the pending attempt");
                return ReplyAction::KeepSocket;
            }
        };
        let Some(effects) = self.step(RunGateEvent::RegistrationReply { call, reply }) else {
            return ReplyAction::KeepSocket;
        };
        if let Some(registered) = registered
            && self.gate.ready_connection() == Some(attempt)
        {
            self.retry_delay = FIRST_RETRY;
            self.retry_not_before = None;
            self.exchange = Exchange::Ready;
            return ReplyAction::Publish(Box::new(registered));
        }

        for effect in effects {
            match effect {
                RunGateEffect::RetireConnection { link } if &link.connection == attempt => {
                    self.failed_on_lost_socket(now);
                    return ReplyAction::CloseSocket;
                }
                RunGateEffect::FenceWaits { authority, reason } if authority == self.authority => {
                    self.parked = Some(reason);
                    return ReplyAction::CloseSocket;
                }
                _ => {}
            }
        }
        ReplyAction::KeepSocket
    }
}

#[cfg(test)]
#[path = "run_registration_test.rs"]
mod tests;

const fn requires_long_wait(reason: LinkCallRefusal) -> bool {
    matches!(
        reason,
        LinkCallRefusal::UnknownCall
            | LinkCallRefusal::WrongTarget
            | LinkCallRefusal::NotPermitted
            | LinkCallRefusal::SessionProofNotAccepted
    )
}
