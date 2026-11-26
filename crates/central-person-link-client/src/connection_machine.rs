#![deny(clippy::wildcard_enum_match_arm)]

use approval_request_protocol::{ApprovalDecideReply, ApprovalListReply, ApprovalReadReply};
use central_link_protocol::{
    CentralCall, CentralCallReply, CentralLinkRpc, CorrelationId, TypedCallResult,
    person_link_rpc::{
        DecidePersonApproval, DecidePersonApprovalRpc, ListOrganizationWorkspaces,
        ListOrganizationWorkspacesRpc, ListPersonApprovals, ListPersonApprovalsRpc,
        OrganizationWorkspacePage, ReadPersonApproval, ReadPersonApprovalRpc,
    },
};
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::Envelope;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::PersonLinkError;

pub(crate) type AddressedPersonCall = Envelope<OrganizationId, PersonCall>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PersonCall {
    ListWorkspaces(ListOrganizationWorkspaces),
    ListApprovals(ListPersonApprovals),
    ReadApproval(ReadPersonApproval),
    DecideApproval(DecidePersonApproval),
}

#[derive(Debug)]
pub(crate) enum PersonReply {
    Workspaces(OrganizationWorkspacePage),
    Approvals(ApprovalListReply),
    Approval(Box<ApprovalReadReply>),
    Decision(ApprovalDecideReply),
}

impl PersonCall {
    pub(crate) fn into_request(self, target: OrganizationId) -> CentralCall {
        match self {
            Self::ListWorkspaces(body) => {
                ListOrganizationWorkspacesRpc::request(Envelope { target, body })
            }
            Self::ListApprovals(body) => ListPersonApprovalsRpc::request(Envelope { target, body }),
            Self::ReadApproval(body) => ReadPersonApprovalRpc::request(Envelope { target, body }),
            Self::DecideApproval(body) => {
                DecidePersonApprovalRpc::request(Envelope { target, body })
            }
        }
    }

    fn answer(
        &self,
        target: &OrganizationId,
        reply: CentralCallReply,
    ) -> Result<PersonReply, PersonLinkError> {
        match self {
            Self::ListWorkspaces(_) => {
                take_organization_reply::<ListOrganizationWorkspacesRpc>(target, reply)
                    .map(PersonReply::Workspaces)
            }
            Self::ListApprovals(_) => {
                take_organization_reply::<ListPersonApprovalsRpc>(target, reply)
                    .map(PersonReply::Approvals)
            }
            Self::ReadApproval(_) => {
                take_organization_reply::<ReadPersonApprovalRpc>(target, reply)
                    .map(PersonReply::Approval)
            }
            Self::DecideApproval(_) => {
                take_organization_reply::<DecidePersonApprovalRpc>(target, reply)
                    .map(PersonReply::Decision)
            }
        }
    }
}

fn take_organization_reply<Call: CentralLinkRpc<Target = OrganizationId>>(
    target: &OrganizationId,
    reply: CentralCallReply,
) -> Result<Call::Reply, PersonLinkError> {
    Call::take_reply(reply)
        .filter(|reply| &reply.target == target)
        .map(|reply| reply.body)
        .ok_or(PersonLinkError::Protocol)
}

#[derive(Clone)]
pub(crate) struct CallAttempt {
    pub call: AddressedPersonCall,
    pub correlation: CorrelationId,
}

#[derive(Clone)]
pub(crate) enum ConnectionState {
    Connecting(AddressedPersonCall),
    Connected(Option<CallAttempt>),
    ClosedByLifetime(AddressedPersonCall),
    Reconnecting(AddressedPersonCall),
    Refused(PersonLinkError),
}

#[derive(Clone)]
pub(crate) enum ConnectionEvent {
    Call(CallAttempt),
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
    Send(CallAttempt),
    Receive,
    RetireSocket,
    Finish(Result<PersonReply, PersonLinkError>),
}

pub(crate) struct ConnectionMachine;

impl ConnectionMachine {
    pub(crate) fn begin(call: AddressedPersonCall) -> (ConnectionState, ConnectionEffect) {
        (ConnectionState::Connecting(call), ConnectionEffect::Upgrade)
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
            Self::Call(_) => "call",
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
            (State::Connecting(call) | State::Reconnecting(call), Event::Upgraded(correlation)) => {
                let attempt = CallAttempt {
                    call: call.clone(),
                    correlation,
                };
                (
                    State::Connected(Some(attempt.clone())),
                    Effect::Send(attempt),
                )
            }
            (State::Connected(None), Event::Call(attempt)) => (
                State::Connected(Some(attempt.clone())),
                Effect::Send(attempt),
            ),
            (State::Connected(Some(attempt)), Event::LifetimeClosed) => (
                State::ClosedByLifetime(attempt.call.clone()),
                Effect::RetireSocket,
            ),
            (State::ClosedByLifetime(call), Event::SocketRetired) => {
                (State::Reconnecting(call.clone()), Effect::Upgrade)
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
                        attempt.call.body.answer(&attempt.call.target, reply)
                    }
                    TypedCallResult::Refused { reason } => {
                        Err(PersonLinkError::CallRefused(reason))
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
            (State::Refused(error), Event::Call(_)) => {
                return Err(TransitionError::Domain(ConnectionRefusal::AlreadyRefused(
                    *error,
                )));
            }
            (
                State::Connecting(_)
                | State::Connected(Some(_))
                | State::ClosedByLifetime(_)
                | State::Reconnecting(_),
                event @ Event::Call(_),
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
            edge("connectedIdle", "call", "connectedAwaitingReply"),
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
