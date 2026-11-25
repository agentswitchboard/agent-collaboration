#![deny(clippy::wildcard_enum_match_arm)]

use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, CorrelationId, TypedCallResult,
    person_link_rpc::{
        ListOrganizationWorkspaces, ListOrganizationWorkspacesRpc, OrganizationWorkspacePage,
    },
};
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::Envelope;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::PersonLinkError;

pub(crate) type WorkspaceRead = Envelope<OrganizationId, ListOrganizationWorkspaces>;

#[derive(Clone)]
pub(crate) struct ReadAttempt {
    pub read: WorkspaceRead,
    pub correlation: CorrelationId,
}

#[derive(Clone)]
pub(crate) enum ConnectionState {
    Connecting(WorkspaceRead),
    Connected(Option<ReadAttempt>),
    ClosedByLifetime(WorkspaceRead),
    Reconnecting(WorkspaceRead),
    Refused(PersonLinkError),
}

#[derive(Clone)]
pub(crate) enum ConnectionEvent {
    Read(ReadAttempt),
    Upgraded(CorrelationId),
    LifetimeClosed,
    SocketRetired,
    Reply {
        correlation: CorrelationId,
        result: Box<TypedCallResult<CentralCallReply>>,
    },
    Failed(PersonLinkError),
}

pub(crate) enum ConnectionEffect {
    Upgrade,
    Send(ReadAttempt),
    Receive,
    RetireSocket,
    Finish(Result<OrganizationWorkspacePage, PersonLinkError>),
}

pub(crate) struct ConnectionMachine;

impl ConnectionMachine {
    pub(crate) fn begin(read: WorkspaceRead) -> (ConnectionState, ConnectionEffect) {
        (ConnectionState::Connecting(read), ConnectionEffect::Upgrade)
    }
}

impl StateTag for ConnectionState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Connecting(_) => "connecting",
            Self::Connected(None) => "connectedIdle",
            Self::Connected(Some(_)) => "connectedAwaitingReply",
            Self::ClosedByLifetime(_) => "closedByLifetime",
            Self::Reconnecting(_) => "reconnecting",
            Self::Refused(_) => "refused",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Refused(_))
    }
}

impl EventTag for ConnectionEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Read(_) => "read",
            Self::Upgraded(_) => "upgraded",
            Self::LifetimeClosed => "lifetimeClosed",
            Self::SocketRetired => "socketRetired",
            Self::Reply { .. } => "reply",
            Self::Failed(_) => "failed",
        }
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionRefusal {
    AlreadyRefused(PersonLinkError),
}

impl RefusalTag for ConnectionRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::AlreadyRefused(_) => "alreadyRefused",
        }
    }
}

impl StateMachine for ConnectionMachine {
    type State = ConnectionState;
    type Event = ConnectionEvent;
    type Effect = ConnectionEffect;
    type Error = ConnectionRefusal;

    fn transition(state: &ConnectionState, event: ConnectionEvent) -> StateMachineResult<Self> {
        use ConnectionEffect as Effect;
        use ConnectionEvent as Event;
        use ConnectionState as State;

        let (next, effect) = match (state, event) {
            (State::Connecting(read) | State::Reconnecting(read), Event::Upgraded(correlation)) => {
                let attempt = ReadAttempt {
                    read: read.clone(),
                    correlation,
                };
                (
                    State::Connected(Some(attempt.clone())),
                    Effect::Send(attempt),
                )
            }
            (State::Connected(None), Event::Read(attempt)) => (
                State::Connected(Some(attempt.clone())),
                Effect::Send(attempt),
            ),
            (State::Connected(Some(attempt)), Event::LifetimeClosed) => (
                State::ClosedByLifetime(attempt.read.clone()),
                Effect::RetireSocket,
            ),
            (State::ClosedByLifetime(read), Event::SocketRetired) => {
                (State::Reconnecting(read.clone()), Effect::Upgrade)
            }
            (
                State::Connected(Some(attempt)),
                Event::Reply {
                    correlation,
                    result,
                },
            ) => {
                if correlation != attempt.correlation {
                    return Err(TransitionError::Stale);
                }
                let answer = match *result {
                    TypedCallResult::Answered { reply } => {
                        ListOrganizationWorkspacesRpc::take_reply(reply)
                            .filter(|reply| reply.target == attempt.read.target)
                            .map(|reply| reply.body)
                            .ok_or(PersonLinkError::Protocol)
                    }
                    TypedCallResult::Refused { reason } => {
                        Err(PersonLinkError::WorkspaceRefused(reason))
                    }
                };
                if matches!(answer, Err(PersonLinkError::Protocol)) {
                    (
                        State::Refused(PersonLinkError::Protocol),
                        Effect::Finish(answer),
                    )
                } else {
                    (State::Connected(None), Effect::Finish(answer))
                }
            }
            (
                State::Connecting(_) | State::Connected(Some(_)) | State::Reconnecting(_),
                Event::Failed(error),
            ) => (State::Refused(error), Effect::Finish(Err(error))),
            (State::Refused(error), Event::Read(_)) => {
                return Err(TransitionError::Domain(ConnectionRefusal::AlreadyRefused(
                    *error,
                )));
            }
            (
                State::Connecting(_)
                | State::Connected(Some(_))
                | State::ClosedByLifetime(_)
                | State::Reconnecting(_),
                event @ Event::Read(_),
            )
            | (
                State::Connected(_) | State::ClosedByLifetime(_) | State::Refused(_),
                event @ Event::Upgraded(_),
            )
            | (
                State::Connecting(_)
                | State::Connected(None)
                | State::ClosedByLifetime(_)
                | State::Reconnecting(_)
                | State::Refused(_),
                event @ Event::LifetimeClosed,
            )
            | (
                State::Connecting(_)
                | State::Connected(_)
                | State::Reconnecting(_)
                | State::Refused(_),
                event @ Event::SocketRetired,
            )
            | (
                State::Connecting(_)
                | State::Connected(None)
                | State::ClosedByLifetime(_)
                | State::Reconnecting(_)
                | State::Refused(_),
                event @ Event::Reply { .. },
            )
            | (
                State::Connected(None) | State::ClosedByLifetime(_) | State::Refused(_),
                event @ Event::Failed(_),
            ) => {
                return Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: event.event_tag(),
                });
            }
        };
        Ok((next, vec![effect]))
    }

    fn transitions() -> &'static [Transition] {
        const fn edge(from: &'static str, event: &'static str, to: &'static str) -> Transition {
            Transition {
                from,
                event,
                to,
                terminal: false,
            }
        }
        const fn refusal(from: &'static str, event: &'static str) -> Transition {
            Transition {
                from,
                event,
                to: "refused",
                terminal: true,
            }
        }
        const TRANSITIONS: &[Transition] = &[
            edge("connecting", "upgraded", "connectedAwaitingReply"),
            refusal("connecting", "failed"),
            edge("connectedIdle", "read", "connectedAwaitingReply"),
            edge("connectedAwaitingReply", "reply", "connectedIdle"),
            refusal("connectedAwaitingReply", "reply"),
            edge(
                "connectedAwaitingReply",
                "lifetimeClosed",
                "closedByLifetime",
            ),
            refusal("connectedAwaitingReply", "failed"),
            edge("closedByLifetime", "socketRetired", "reconnecting"),
            edge("reconnecting", "upgraded", "connectedAwaitingReply"),
            refusal("reconnecting", "failed"),
        ];
        TRANSITIONS
    }
}

#[cfg(test)]
#[path = "connection_machine_test.rs"]
mod tests;
