//! Registration and authority for one Router run.

use std::marker::PhantomData;

use approval_request_protocol::{RegisterRunReply, RegisterRunRequest, RunRefusal};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::RouterRunId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DialAttempt(u64);

impl DialAttempt {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAttempt(u64);

impl CallAttempt {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkAttempt<Connection> {
    pub connection: Connection,
    pub dial: DialAttempt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallKey<Connection> {
    pub link: LinkAttempt<Connection>,
    pub attempt: CallAttempt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunAuthority<Enrollment> {
    pub enrollment: Enrollment,
    pub router_run: RouterRunId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityEnd {
    RunRetired,
    EnrollmentEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkAdmission {
    Ready,
    NotReady,
    Discard,
}

/// Authority refusals come from a completed call matched by its owning wait.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchedRunRefusal<Connection> {
    call: CallKey<Connection>,
    router_run: RouterRunId,
    refusal: RunRefusal,
}

impl<Connection> MatchedRunRefusal<Connection> {
    /// Carry a refusal after the owning wait has matched its reply to the exact pending call.
    pub const fn from_matched_wait(
        call: CallKey<Connection>,
        router_run: RouterRunId,
        refusal: RunRefusal,
    ) -> Self {
        Self {
            call,
            router_run,
            refusal,
        }
    }

    pub const fn call(&self) -> &CallKey<Connection> {
        &self.call
    }

    pub const fn reason(&self) -> RunRefusal {
        self.refusal
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Phase<Connection> {
    AwaitingDial,
    Dialing(LinkAttempt<Connection>),
    RegistrationDue(LinkAttempt<Connection>),
    Registering(CallKey<Connection>),
    Ready(LinkAttempt<Connection>),
    Ended(AuthorityEnd),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunGateState<Enrollment, Connection> {
    authority: RunAuthority<Enrollment>,
    phase: Phase<Connection>,
}

impl<Enrollment, Connection> RunGateState<Enrollment, Connection> {
    pub const fn new(authority: RunAuthority<Enrollment>) -> Self {
        Self {
            authority,
            phase: Phase::AwaitingDial,
        }
    }

    /// The ready link's identity, for addressing a candidate.
    pub const fn ready_link(&self) -> Option<&LinkAttempt<Connection>> {
        match &self.phase {
            Phase::Ready(link) => Some(link),
            _ => None,
        }
    }

    pub fn ready_connection(&self) -> Option<&Connection> {
        self.ready_link().map(|link| &link.connection)
    }

    fn current_link(&self) -> Option<&LinkAttempt<Connection>> {
        match &self.phase {
            Phase::Dialing(link) | Phase::RegistrationDue(link) | Phase::Ready(link) => Some(link),
            Phase::Registering(call) => Some(&call.link),
            Phase::AwaitingDial | Phase::Ended(_) => None,
        }
    }
}

impl<Enrollment: Eq, Connection: Eq> RunGateState<Enrollment, Connection> {
    pub fn admit_link(
        &self,
        authority: &RunAuthority<Enrollment>,
        link: &LinkAttempt<Connection>,
    ) -> LinkAdmission {
        if &self.authority != authority || self.current_link() != Some(link) {
            return LinkAdmission::Discard;
        }
        if self.ready_link().is_some() {
            LinkAdmission::Ready
        } else {
            LinkAdmission::NotReady
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunGateEvent<Enrollment, Connection> {
    DialStarted {
        link: LinkAttempt<Connection>,
    },
    Welcome {
        authority: RunAuthority<Enrollment>,
        link: LinkAttempt<Connection>,
    },
    PublishRegistration {
        call: CallKey<Connection>,
    },
    RegistrationReply {
        call: CallKey<Connection>,
        reply: RegisterRunReply,
    },
    RegistrationFailed {
        call: CallKey<Connection>,
    },
    ConnectionLost {
        link: LinkAttempt<Connection>,
    },
    HolderAuthorityRefused(MatchedRunRefusal<Connection>),
    AuthenticatedEnrollmentEnded {
        enrollment: Enrollment,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunGateEffect<Enrollment, Connection> {
    Register {
        call: CallKey<Connection>,
        request: RegisterRunRequest,
    },
    RetireConnection {
        link: LinkAttempt<Connection>,
    },
    FenceWaits {
        authority: RunAuthority<Enrollment>,
        reason: AuthorityEnd,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunGateRefusal {}

pub struct RunGate<Enrollment, Connection>(PhantomData<(Enrollment, Connection)>);

impl<Enrollment, Connection> StateTag for RunGateState<Enrollment, Connection> {
    fn state_tag(&self) -> &'static str {
        match self.phase {
            Phase::AwaitingDial => "awaiting_dial",
            Phase::Dialing(_) => "dialing",
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
            Self::DialStarted { .. } => "dial_started",
            Self::Welcome { .. } => "welcome",
            Self::PublishRegistration { .. } => "publish_registration",
            Self::RegistrationReply { .. } => "registration_reply",
            Self::RegistrationFailed { .. } => "registration_failed",
            Self::ConnectionLost { .. } => "connection_lost",
            Self::HolderAuthorityRefused(_) => "holder_authority_refused",
            Self::AuthenticatedEnrollmentEnded { .. } => "authenticated_enrollment_ended",
        }
    }
}

impl RefusalTag for RunGateRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

fn refuse_run<Enrollment, Connection>(
    link: LinkAttempt<Connection>,
    refusal: RunRefusal,
    effects: &mut Vec<RunGateEffect<Enrollment, Connection>>,
) -> Phase<Connection> {
    match refusal {
        RunRefusal::RunNotRegistered {} => {
            effects.push(RunGateEffect::RetireConnection { link });
            Phase::AwaitingDial
        }
        RunRefusal::RunRetired {} => Phase::Ended(AuthorityEnd::RunRetired),
        RunRefusal::EnrollmentEnded {} => Phase::Ended(AuthorityEnd::EnrollmentEnded),
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
            RunGateEvent::DialStarted { link } if matches!(state.phase, Phase::AwaitingDial) => {
                Phase::Dialing(link)
            }
            RunGateEvent::Welcome { authority, link }
                if state.authority == authority
                    && matches!(&state.phase, Phase::Dialing(current) if current == &link) =>
            {
                Phase::RegistrationDue(link)
            }
            RunGateEvent::PublishRegistration { call } if matches!(&state.phase, Phase::RegistrationDue(link) if link == &call.link) =>
            {
                effects.push(RunGateEffect::Register {
                    call: call.clone(),
                    request: RegisterRunRequest {
                        router_run: state.authority.router_run.clone(),
                    },
                });
                Phase::Registering(call)
            }
            RunGateEvent::RegistrationReply { call, reply } if matches!(&state.phase, Phase::Registering(current) if current == &call) => {
                match reply {
                    RegisterRunReply::Registered {} => Phase::Ready(call.link),
                    RegisterRunReply::RunNotRegistered {} => {
                        refuse_run(call.link, RunRefusal::RunNotRegistered {}, &mut effects)
                    }
                    RegisterRunReply::RunRetired {} => Phase::Ended(AuthorityEnd::RunRetired),
                    RegisterRunReply::EnrollmentEnded {} => {
                        Phase::Ended(AuthorityEnd::EnrollmentEnded)
                    }
                }
            }
            RunGateEvent::RegistrationFailed { call } if matches!(&state.phase, Phase::Registering(current) if current == &call) => {
                Phase::RegistrationDue(call.link)
            }
            RunGateEvent::ConnectionLost { link } if state.current_link() == Some(&link) => {
                Phase::AwaitingDial
            }
            RunGateEvent::HolderAuthorityRefused(matched)
                if matched.router_run == state.authority.router_run
                    && matches!(&state.phase, Phase::Ready(link) if link == &matched.call.link) =>
            {
                refuse_run(matched.call.link, matched.refusal, &mut effects)
            }
            RunGateEvent::AuthenticatedEnrollmentEnded { enrollment }
                if state.authority.enrollment == enrollment =>
            {
                Phase::Ended(AuthorityEnd::EnrollmentEnded)
            }
            RunGateEvent::DialStarted { .. }
            | RunGateEvent::Welcome { .. }
            | RunGateEvent::PublishRegistration { .. }
            | RunGateEvent::RegistrationReply { .. }
            | RunGateEvent::RegistrationFailed { .. }
            | RunGateEvent::ConnectionLost { .. }
            | RunGateEvent::HolderAuthorityRefused(_)
            | RunGateEvent::AuthenticatedEnrollmentEnded { .. } => {
                return Err(TransitionError::Stale);
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

const fn edge(from: &'static str, event: &'static str, to: &'static str) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: matches!(to.as_bytes(), b"ended"),
    }
}

const GATE_TRANSITIONS: [Transition; 18] = [
    edge("awaiting_dial", "dial_started", "dialing"),
    edge("dialing", "welcome", "registration_due"),
    edge("dialing", "connection_lost", "awaiting_dial"),
    edge("registration_due", "publish_registration", "registering"),
    edge("registration_due", "connection_lost", "awaiting_dial"),
    edge("registering", "registration_reply", "ready"),
    edge("registering", "registration_reply", "awaiting_dial"),
    edge("registering", "registration_reply", "ended"),
    edge("registering", "registration_failed", "registration_due"),
    edge("registering", "connection_lost", "awaiting_dial"),
    edge("ready", "connection_lost", "awaiting_dial"),
    edge("ready", "holder_authority_refused", "awaiting_dial"),
    edge("ready", "holder_authority_refused", "ended"),
    edge("awaiting_dial", "authenticated_enrollment_ended", "ended"),
    edge("dialing", "authenticated_enrollment_ended", "ended"),
    edge(
        "registration_due",
        "authenticated_enrollment_ended",
        "ended",
    ),
    edge("registering", "authenticated_enrollment_ended", "ended"),
    edge("ready", "authenticated_enrollment_ended", "ended"),
];

#[cfg(test)]
#[path = "run_gate_test.rs"]
mod tests;
