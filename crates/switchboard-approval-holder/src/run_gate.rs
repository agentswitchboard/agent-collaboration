//! The Mac's approval run gate for its one active Central connection.

use std::marker::PhantomData;

use approval_request_protocol::{
    DeliverEndingReply, RegisterRunReply, RegisterRunRequest, RunRefusal,
};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::RouterRunId;

/// The exact local enrollment and run admitted into the approval service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunAuthority<Enrollment> {
    pub enrollment: Enrollment,
    pub router_run: RouterRunId,
}

/// A final refusal of the current run or enrollment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityEnd {
    RunRetired,
    EnrollmentEnded,
}

/// How the authenticated link may handle an ending offer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndingIngress {
    Authoritative,
    Reply(DeliverEndingReply),
    Discard,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Phase<Connection> {
    AwaitingWelcome,
    RegistrationDue(Connection),
    Registering(Connection),
    Ready(Connection),
    Ended(AuthorityEnd),
}

/// The registration state for one live Router run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunGateState<Enrollment, Connection> {
    authority: RunAuthority<Enrollment>,
    phase: Phase<Connection>,
}

impl<Enrollment, Connection> RunGateState<Enrollment, Connection> {
    pub const fn new(authority: RunAuthority<Enrollment>) -> Self {
        Self {
            authority,
            phase: Phase::AwaitingWelcome,
        }
    }

    pub const fn ready_connection(&self) -> Option<&Connection> {
        match &self.phase {
            Phase::Ready(connection) => Some(connection),
            Phase::AwaitingWelcome
            | Phase::RegistrationDue(_)
            | Phase::Registering(_)
            | Phase::Ended(_) => None,
        }
    }
}

impl<Enrollment: Eq, Connection: Eq> RunGateState<Enrollment, Connection> {
    pub fn ending_ingress(
        &self,
        authority: &RunAuthority<Enrollment>,
        connection: &Connection,
    ) -> EndingIngress {
        if self.authority == *authority {
            if matches!(self.phase, Phase::Ended(_)) {
                return EndingIngress::Discard;
            }
            if self.ready_connection() == Some(connection) {
                return EndingIngress::Authoritative;
            }
        }
        EndingIngress::Reply(DeliverEndingReply::HolderNotReady {})
    }

    fn is_current(&self, connection: &Connection) -> bool {
        match &self.phase {
            Phase::RegistrationDue(current)
            | Phase::Registering(current)
            | Phase::Ready(current) => current == connection,
            Phase::AwaitingWelcome | Phase::Ended(_) => false,
        }
    }
}

/// Observations from the one authenticated link and its typed registration call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunGateEvent<Enrollment, Connection> {
    Welcome {
        authority: RunAuthority<Enrollment>,
        connection: Connection,
    },
    PublishRegistration {
        connection: Connection,
    },
    /// The RPC owner is returning the completion of the pending typed call on this connection.
    RegistrationReply {
        connection: Connection,
        reply: RegisterRunReply,
    },
    /// The RPC owner ended its pending call without a reply.
    RegistrationFailed {
        connection: Connection,
    },
    ConnectionLost {
        connection: Connection,
    },
    /// A matched holder call returned a current run refusal.
    HolderAuthorityRefused {
        connection: Connection,
        refusal: RunRefusal,
    },
    AuthenticatedEnrollmentEnded {
        enrollment: Enrollment,
    },
}

/// A typed registration call or a fence for this run's waits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunGateEffect<Enrollment, Connection> {
    Register {
        connection: Connection,
        request: RegisterRunRequest,
    },
    FenceWaits {
        authority: RunAuthority<Enrollment>,
        reason: AuthorityEnd,
    },
}

/// Registration transitions have structural stale refusals only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunGateRefusal {}

/// Pure reducer for Welcome, RegisterRun and terminal authority.
pub struct RunGate<Enrollment, Connection>(PhantomData<(Enrollment, Connection)>);

impl<Enrollment, Connection> StateTag for RunGateState<Enrollment, Connection> {
    fn state_tag(&self) -> &'static str {
        match self.phase {
            Phase::AwaitingWelcome => "awaiting_welcome",
            Phase::RegistrationDue(_) => "registration_due",
            Phase::Registering(_) => "registering",
            Phase::Ready(_) => "ready",
            Phase::Ended(_) => "ended",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self.phase, Phase::Ended(_))
    }
}

impl<Enrollment, Connection> EventTag for RunGateEvent<Enrollment, Connection> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Welcome { .. } => "welcome",
            Self::PublishRegistration { .. } => "publish_registration",
            Self::RegistrationReply { .. } => "registration_reply",
            Self::RegistrationFailed { .. } => "registration_failed",
            Self::ConnectionLost { .. } => "connection_lost",
            Self::HolderAuthorityRefused { .. } => "holder_authority_refused",
            Self::AuthenticatedEnrollmentEnded { .. } => "authenticated_enrollment_ended",
        }
    }
}

impl RefusalTag for RunGateRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

impl<Enrollment: Clone + Eq, Connection: Clone + Eq> StateMachine
    for RunGate<Enrollment, Connection>
{
    type State = RunGateState<Enrollment, Connection>;
    type Event = RunGateEvent<Enrollment, Connection>;
    type Effect = RunGateEffect<Enrollment, Connection>;
    type Error = RunGateRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        if state.is_terminal() {
            return Err(TransitionError::Stale);
        }
        let mut effects = Vec::new();
        let phase = match event {
            RunGateEvent::Welcome {
                authority,
                connection,
            } => {
                if state.authority != authority || !matches!(state.phase, Phase::AwaitingWelcome) {
                    return Err(TransitionError::Stale);
                }
                Phase::RegistrationDue(connection)
            }
            RunGateEvent::PublishRegistration { connection } => {
                if !matches!(&state.phase, Phase::RegistrationDue(current) if current == &connection)
                {
                    return Err(TransitionError::Stale);
                }
                effects.push(RunGateEffect::Register {
                    connection: connection.clone(),
                    request: RegisterRunRequest {
                        router_run: state.authority.router_run.clone(),
                    },
                });
                Phase::Registering(connection)
            }
            RunGateEvent::RegistrationReply { connection, reply } => {
                if !matches!(&state.phase, Phase::Registering(pending) if pending == &connection) {
                    return Err(TransitionError::Stale);
                }
                match reply {
                    RegisterRunReply::Registered {} => Phase::Ready(connection),
                    RegisterRunReply::RunNotRegistered {} => Phase::RegistrationDue(connection),
                    RegisterRunReply::RunRetired {} => Phase::Ended(AuthorityEnd::RunRetired),
                    RegisterRunReply::EnrollmentEnded {} => {
                        Phase::Ended(AuthorityEnd::EnrollmentEnded)
                    }
                }
            }
            RunGateEvent::RegistrationFailed { connection } => {
                if !matches!(&state.phase, Phase::Registering(pending) if pending == &connection) {
                    return Err(TransitionError::Stale);
                }
                Phase::RegistrationDue(connection)
            }
            RunGateEvent::ConnectionLost { connection } => {
                if !state.is_current(&connection) {
                    return Err(TransitionError::Stale);
                }
                Phase::AwaitingWelcome
            }
            RunGateEvent::HolderAuthorityRefused {
                connection,
                refusal,
            } => {
                if !state.is_current(&connection) {
                    return Err(TransitionError::Stale);
                }
                match refusal {
                    RunRefusal::RunNotRegistered {} => Phase::RegistrationDue(connection),
                    RunRefusal::RunRetired {} => Phase::Ended(AuthorityEnd::RunRetired),
                    RunRefusal::EnrollmentEnded {} => Phase::Ended(AuthorityEnd::EnrollmentEnded),
                }
            }
            RunGateEvent::AuthenticatedEnrollmentEnded { enrollment } => {
                if state.authority.enrollment != enrollment {
                    return Err(TransitionError::Stale);
                }
                Phase::Ended(AuthorityEnd::EnrollmentEnded)
            }
        };
        if let Phase::Ended(reason) = phase {
            effects.push(RunGateEffect::FenceWaits {
                authority: state.authority.clone(),
                reason,
            });
        }
        Ok((
            RunGateState {
                authority: state.authority.clone(),
                phase,
            },
            effects,
        ))
    }

    fn transitions() -> &'static [Transition] {
        &GATE_TRANSITIONS
    }
}

const fn move_to(from: &'static str, event: &'static str, to: &'static str) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: false,
    }
}

const fn end_from(from: &'static str, event: &'static str) -> Transition {
    Transition {
        from,
        event,
        to: "ended",
        terminal: true,
    }
}

const GATE_TRANSITIONS: [Transition; 19] = [
    move_to("awaiting_welcome", "welcome", "registration_due"),
    move_to("registration_due", "publish_registration", "registering"),
    move_to("registering", "registration_reply", "ready"),
    move_to("registering", "registration_reply", "registration_due"),
    end_from("registering", "registration_reply"),
    move_to("registering", "registration_failed", "registration_due"),
    move_to("registration_due", "connection_lost", "awaiting_welcome"),
    move_to("registering", "connection_lost", "awaiting_welcome"),
    move_to("ready", "connection_lost", "awaiting_welcome"),
    move_to(
        "registration_due",
        "holder_authority_refused",
        "registration_due",
    ),
    move_to(
        "registering",
        "holder_authority_refused",
        "registration_due",
    ),
    move_to("ready", "holder_authority_refused", "registration_due"),
    end_from("registration_due", "holder_authority_refused"),
    end_from("registering", "holder_authority_refused"),
    end_from("ready", "holder_authority_refused"),
    end_from("awaiting_welcome", "authenticated_enrollment_ended"),
    end_from("registration_due", "authenticated_enrollment_ended"),
    end_from("registering", "authenticated_enrollment_ended"),
    end_from("ready", "authenticated_enrollment_ended"),
];

#[cfg(test)]
#[path = "run_gate_test.rs"]
mod tests;
