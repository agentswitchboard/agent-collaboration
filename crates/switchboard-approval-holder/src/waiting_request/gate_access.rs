use approval_request_protocol::{DeliverEndingReply, DeliverEndingRequest};
use state_machine_core::{EventTag, StateMachineResult, Transition, TransitionError};

use super::{Authority, WaitEffect, WaitState, WaitingRequest, step};
use crate::run_gate::{CallKey, LinkAdmission, LinkAttempt, RunAuthority, RunGateState};

/// Inputs whose effects and custody decisions depend on the current run gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateWaitEvent<Enrollment, Connection> {
    Publish {
        authority: RunAuthority<Enrollment>,
        call: CallKey<Connection>,
    },
    DeliverEnding {
        authority: RunAuthority<Enrollment>,
        link: LinkAttempt<Connection>,
        request: DeliverEndingRequest,
    },
}

impl<Enrollment, Connection> GateWaitEvent<Enrollment, Connection> {
    /// Declared transitions for the gate-checked inputs.
    pub const fn transitions() -> &'static [Transition] {
        GATE_WAIT_TRANSITIONS
    }
}

impl<Enrollment, Connection> EventTag for GateWaitEvent<Enrollment, Connection> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Publish { .. } => "publish",
            Self::DeliverEnding { .. } => "deliver_ending",
        }
    }
}

/// Applies a publication or ending to one wait under the owner's current gate.
pub fn step_under_gate<Responder: Clone + Eq, Connection: Clone + Eq, Enrollment: Eq>(
    wait: &WaitState<Responder, Connection>,
    gate: &RunGateState<Enrollment, Connection>,
    event: GateWaitEvent<Enrollment, Connection>,
) -> StateMachineResult<WaitingRequest<Responder, Connection, Enrollment>> {
    let mut next = wait.clone();
    let mut effects = Vec::new();
    match event {
        GateWaitEvent::Publish { authority, call } => {
            if wait.authority == Authority::Ended
                || !wait.holds_place()
                || wait.router_run.as_ref() != Some(&authority.router_run)
                || gate.admit_link(&authority, &call.link) != LinkAdmission::Ready
            {
                return Err(TransitionError::Stale);
            }
            effects.push(next.publish(authority, call)?);
        }
        GateWaitEvent::DeliverEnding {
            authority,
            link,
            request,
        } => {
            if !wait.holds_place() && !wait.may_be_forgotten() {
                return Err(TransitionError::Stale);
            }
            if wait.authority == Authority::Live {
                let reply = match gate.admit_link(&authority, &link) {
                    LinkAdmission::NotReady => DeliverEndingReply::HolderNotReady {},
                    LinkAdmission::Ready
                        if wait.router_run.as_ref() == Some(&authority.router_run) =>
                    {
                        next.accept_ending(request, &mut effects)?
                    }
                    LinkAdmission::Ready | LinkAdmission::Discard => return Ok((next, effects)),
                };
                effects.push(WaitEffect::EndingReply {
                    authority,
                    link,
                    reply,
                });
            }
        }
    }
    Ok((next, effects))
}

/// At transport enqueue, check the exact authority and link on a wire candidate.
/// On refusal, settle the exact Raise or Stop attempt as
/// [not reached](super::LinkAttemptDisposition::NotReached) and drop any ending reply.
/// Local effects are never transport candidates.
pub fn may_enqueue<Responder, Connection: Eq, Enrollment: Eq>(
    gate: &RunGateState<Enrollment, Connection>,
    candidate: &WaitEffect<Responder, Connection, Enrollment>,
) -> bool {
    match candidate {
        WaitEffect::Raise {
            authority, call, ..
        }
        | WaitEffect::Stop {
            authority, call, ..
        } => gate.admit_link(authority, &call.link) == LinkAdmission::Ready,
        WaitEffect::EndingReply {
            authority,
            link,
            reply,
        } => match gate.admit_link(authority, link) {
            LinkAdmission::Ready => true,
            LinkAdmission::NotReady => matches!(reply, DeliverEndingReply::HolderNotReady {}),
            LinkAdmission::Discard => false,
        },
        WaitEffect::EndingConflict { .. }
        | WaitEffect::StopReconciled(_)
        | WaitEffect::StopNotVisible { .. }
        | WaitEffect::RunRefused(_)
        | WaitEffect::Submit { .. }
        | WaitEffect::ReleaseBinding { .. }
        | WaitEffect::WriterResolved { .. } => false,
    }
}

const GATE_WAIT_TRANSITIONS: &[Transition] = &[
    step("live", "publish", "live", false),
    step("live", "deliver_ending", "live", false),
    step("fenced_write", "deliver_ending", "fenced_write", false),
    step("closed", "deliver_ending", "closed", true),
];
